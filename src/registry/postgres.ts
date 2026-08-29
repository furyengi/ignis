/**
 * Postgres-backed function store.
 *
 * What this buys: a control plane restart no longer forgets every deployment.
 * What it does not buy: clustering. Two nodes sharing this database will each
 * allocate versions correctly, but neither is told when the other deploys, so
 * their read caches drift until they restart. Fixing that needs LISTEN/NOTIFY
 * on top of this, which is not here.
 *
 * `pg` is imported dynamically so a memory-store deployment never pays to load
 * the driver, and so a missing module produces an actionable error rather than
 * a crash at import time.
 */
import type { Pool, PoolClient } from 'pg';
import { log } from '../log.js';
import type { FunctionSpec } from '../types.js';
import type { FunctionStore, PendingSpec } from './store.js';

/**
 * Advisory-lock key guarding migrations.
 *
 * Concurrent `CREATE TABLE IF NOT EXISTS` from several nodes can deadlock in
 * Postgres rather than politely no-op, so migration is serialised. The constant
 * is arbitrary but must never change.
 */
const MIGRATION_LOCK_KEY = 0x1917_1502;

interface Migration {
  id: string;
  sql: string;
}

const MIGRATIONS: Migration[] = [
  {
    id: '001-functions',
    sql: `
      CREATE TABLE IF NOT EXISTS ignis_functions (
        name             text PRIMARY KEY,
        entrypoint       text        NOT NULL,
        timeout_ms       integer     NOT NULL,
        memory_mib       integer     NOT NULL,
        min_warm         integer     NOT NULL,
        max_concurrency  integer     NOT NULL,
        env              jsonb       NOT NULL DEFAULT '{}'::jsonb,
        version          integer     NOT NULL DEFAULT 1,
        created_at       timestamptz NOT NULL DEFAULT now(),
        updated_at       timestamptz NOT NULL DEFAULT now()
      );
    `,
  },
];

/** Shape of a row in `ignis_functions`. */
interface FunctionRow {
  name: string;
  entrypoint: string;
  timeout_ms: number;
  memory_mib: number;
  min_warm: number;
  max_concurrency: number;
  env: Record<string, string>;
  version: number;
}

function toSpec(row: FunctionRow): FunctionSpec {
  return {
    name: row.name,
    entrypoint: row.entrypoint,
    // integer columns arrive as JS numbers; bigint would not, which is part of
    // why these are integer.
    timeoutMs: row.timeout_ms,
    memoryMib: row.memory_mib,
    minWarm: row.min_warm,
    maxConcurrency: row.max_concurrency,
    env: row.env ?? {},
    version: row.version,
  };
}

export interface PostgresStoreOptions {
  connectionString: string;
  /** Upper bound on pooled connections. The control plane is not chatty. */
  max?: number;
  connectionTimeoutMillis?: number;
}

export class PostgresStore implements FunctionStore {
  readonly name = 'postgres';
  private pool: Pool | null = null;

  constructor(
    private readonly opts: PostgresStoreOptions,
    private readonly logger = log.child({ component: 'store', store: 'postgres' }),
  ) {}

  private async getPool(): Promise<Pool> {
    if (this.pool) return this.pool;
    let pg: typeof import('pg');
    try {
      pg = await import('pg');
    } catch {
      throw new Error(
        'the postgres store requires the "pg" package -- run `npm install pg`, ' +
          'or unset IGNIS_DATABASE_URL to use the in-memory store',
      );
    }
    // pg ships as CJS; the default export is the namespace under ESM.
    const { Pool } = (pg as unknown as { default?: typeof import('pg') }).default ?? pg;
    this.pool = new Pool({
      connectionString: this.opts.connectionString,
      max: this.opts.max ?? 8,
      connectionTimeoutMillis: this.opts.connectionTimeoutMillis ?? 5_000,
    });
    // An idle-client error must not take the process down.
    this.pool.on('error', (err: Error) => {
      this.logger.warn('idle postgres client error', { err: err.message });
    });
    return this.pool;
  }

  async init(): Promise<void> {
    const pool = await this.getPool();
    const client = await pool.connect();
    try {
      await client.query('SELECT pg_advisory_lock($1)', [MIGRATION_LOCK_KEY]);
      try {
        await this.migrate(client);
      } finally {
        await client.query('SELECT pg_advisory_unlock($1)', [MIGRATION_LOCK_KEY]);
      }
    } finally {
      client.release();
    }
  }

  private async migrate(client: PoolClient): Promise<void> {
    await client.query(`
      CREATE TABLE IF NOT EXISTS ignis_migrations (
        id         text PRIMARY KEY,
        applied_at timestamptz NOT NULL DEFAULT now()
      );
    `);

    const { rows } = await client.query<{ id: string }>('SELECT id FROM ignis_migrations');
    const applied = new Set(rows.map((r) => r.id));

    for (const m of MIGRATIONS) {
      if (applied.has(m.id)) continue;
      // Each migration and its bookkeeping commit together, so a crash halfway
      // through cannot leave a migration applied but unrecorded.
      await client.query('BEGIN');
      try {
        await client.query(m.sql);
        await client.query('INSERT INTO ignis_migrations (id) VALUES ($1)', [m.id]);
        await client.query('COMMIT');
        this.logger.info('applied migration', { id: m.id });
      } catch (err) {
        await client.query('ROLLBACK');
        throw err;
      }
    }
  }

  async load(): Promise<FunctionSpec[]> {
    const pool = await this.getPool();
    const { rows } = await pool.query<FunctionRow>(
      `SELECT name, entrypoint, timeout_ms, memory_mib, min_warm, max_concurrency, env, version
         FROM ignis_functions
        ORDER BY name`,
    );
    return rows.map(toSpec);
  }

  /**
   * Upsert, bumping the version in the same statement.
   *
   * Read-modify-write from the application would let two concurrent deploys
   * both read v3 and both write v4, leaving two different code versions
   * claiming the same number -- and the pool would then treat stale sandboxes
   * as current. `version = ignis_functions.version + 1` inside the UPDATE makes
   * the increment atomic under the row lock the upsert already takes.
   */
  async put(spec: PendingSpec): Promise<FunctionSpec> {
    const pool = await this.getPool();
    const { rows } = await pool.query<FunctionRow>(
      `INSERT INTO ignis_functions
         (name, entrypoint, timeout_ms, memory_mib, min_warm, max_concurrency, env, version)
       VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, 1)
       ON CONFLICT (name) DO UPDATE SET
         entrypoint      = EXCLUDED.entrypoint,
         timeout_ms      = EXCLUDED.timeout_ms,
         memory_mib      = EXCLUDED.memory_mib,
         min_warm        = EXCLUDED.min_warm,
         max_concurrency = EXCLUDED.max_concurrency,
         env             = EXCLUDED.env,
         version         = ignis_functions.version + 1,
         updated_at      = now()
       RETURNING name, entrypoint, timeout_ms, memory_mib, min_warm, max_concurrency, env, version`,
      [
        spec.name,
        spec.entrypoint,
        spec.timeoutMs,
        spec.memoryMib,
        spec.minWarm,
        spec.maxConcurrency,
        JSON.stringify(spec.env ?? {}),
      ],
    );
    return toSpec(rows[0]!);
  }

  async delete(name: string): Promise<boolean> {
    const pool = await this.getPool();
    const res = await pool.query('DELETE FROM ignis_functions WHERE name = $1', [name]);
    return (res.rowCount ?? 0) > 0;
  }

  async close(): Promise<void> {
    await this.pool?.end();
    this.pool = null;
  }
}
