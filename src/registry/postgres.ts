/**
 * Postgres-backed function store.
 *
 * A control plane restart no longer forgets every deployment, and nodes sharing
 * a database stay in step: writes are published on a LISTEN/NOTIFY channel by a
 * trigger, so every node learns about every other node's deploys.
 *
 * Two things make that safe rather than merely plausible. The listener owns a
 * dedicated connection, because LISTEN binds to a backend session and a pooled
 * client would stop delivering the moment it was returned. And a reconnect
 * triggers a full resync, because notifications are fire-and-forget -- anything
 * published while the connection was down is gone for good.
 *
 * `pg` is imported dynamically so a memory-store deployment never pays to load
 * the driver, and so a missing module produces an actionable error rather than
 * a crash at import time.
 */
import type { Client, Pool, PoolClient } from 'pg';
import { log } from '../log.js';
import type { FunctionSpec } from '../types.js';
import type {
  WatchableStore,
  PendingSpec,
  StoreChange,
  StoreWatcher,
  Unwatch,
} from './store.js';

/**
 * Advisory-lock key guarding migrations.
 *
 * Concurrent `CREATE TABLE IF NOT EXISTS` from several nodes can deadlock in
 * Postgres rather than politely no-op, so migration is serialised. The constant
 * is arbitrary but must never change.
 */
const MIGRATION_LOCK_KEY = 0x1917_1502;

/** Channel the trigger publishes on, and every node listens to. */
const NOTIFY_CHANNEL = 'ignis_functions';

/** Reconnect backoff bounds for the listener connection. */
const RECONNECT_MIN_MS = 250;
const RECONNECT_MAX_MS = 5_000;

/**
 * Load `pg` lazily.
 *
 * Dynamic so a memory-store deployment never pays to load the driver, and so a
 * missing module produces an actionable message rather than an import-time
 * crash. `pg` ships as CJS, whose namespace lands under `.default` in ESM.
 */
async function loadPg(): Promise<typeof import('pg')> {
  let mod: typeof import('pg');
  try {
    mod = await import('pg');
  } catch {
    throw new Error(
      'the postgres store requires the "pg" package -- run `npm install pg`, ' +
        'or unset IGNIS_DATABASE_URL to use the in-memory store',
    );
  }
  return (mod as unknown as { default?: typeof import('pg') }).default ?? mod;
}

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
  {
    // Notify from a trigger rather than from the application, so *any* writer
    // publishes -- including a DBA doing surgery in psql. An application-level
    // NOTIFY silently misses those, and a cache that is wrong only in the rare
    // case is worse than one that is never wrong.
    //
    // pg_notify fires on commit, so listeners never see a change that later
    // rolls back.
    id: '002-notify',
    sql: `
      CREATE OR REPLACE FUNCTION ignis_notify_functions() RETURNS trigger AS $fn$
      BEGIN
        PERFORM pg_notify(
          '${NOTIFY_CHANNEL}',
          json_build_object(
            'op',      CASE WHEN TG_OP = 'DELETE' THEN 'delete' ELSE 'upsert' END,
            'name',    COALESCE(NEW.name, OLD.name),
            'version', COALESCE(NEW.version, 0)
          )::text
        );
        RETURN NULL;
      END;
      $fn$ LANGUAGE plpgsql;

      DROP TRIGGER IF EXISTS ignis_functions_notify ON ignis_functions;

      CREATE TRIGGER ignis_functions_notify
        AFTER INSERT OR UPDATE OR DELETE ON ignis_functions
        FOR EACH ROW EXECUTE FUNCTION ignis_notify_functions();
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

export class PostgresStore implements WatchableStore {
  readonly name = 'postgres';
  private pool: Pool | null = null;

  constructor(
    private readonly opts: PostgresStoreOptions,
    private readonly logger = log.child({ component: 'store', store: 'postgres' }),
  ) {}

  private async getPool(): Promise<Pool> {
    if (this.pool) return this.pool;
    const { Pool } = await loadPg();
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

  async get(name: string): Promise<FunctionSpec | null> {
    const pool = await this.getPool();
    const { rows } = await pool.query<FunctionRow>(
      `SELECT name, entrypoint, timeout_ms, memory_mib, min_warm, max_concurrency, env, version
         FROM ignis_functions
        WHERE name = $1`,
      [name],
    );
    return rows[0] ? toSpec(rows[0]) : null;
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

  /**
   * Subscribe to cluster-wide writes.
   *
   * Uses its own connection, never one from the pool: LISTEN registers against
   * a specific backend session, so a pooled client would stop delivering the
   * moment it was returned -- and would do so silently, which is the worst
   * possible failure mode for a cache-invalidation channel.
   */
  async watch(watcher: StoreWatcher): Promise<Unwatch> {
    let stopped = false;
    let client: Client | null = null;
    let retry = RECONNECT_MIN_MS;
    let timer: NodeJS.Timeout | null = null;
    /** First connect is a subscribe; every later one is a gap to be closed. */
    let reconnecting = false;

    const scheduleReconnect = (): void => {
      if (stopped || timer) return;
      // Jitter so a Postgres restart does not bring every node back at once.
      const delay = Math.min(retry, RECONNECT_MAX_MS) * (0.5 + Math.random() / 2);
      timer = setTimeout(() => {
        timer = null;
        retry = Math.min(retry * 2, RECONNECT_MAX_MS);
        void connect();
      }, delay);
      // A reconnect timer must never be the reason the process stays alive.
      timer.unref?.();
    };

    const connect = async (): Promise<void> => {
      if (stopped) return;
      try {
        const pg = await loadPg();
        client = new pg.Client({ connectionString: this.opts.connectionString });

        // Registered before connecting so a failure during startup still
        // routes into the retry path instead of becoming an unhandled error.
        client.on('error', (err: Error) => {
          this.logger.warn('listener connection error', { err: err.message });
          void drop();
        });
        client.on('end', () => void drop());
        client.on('notification', (msg: { channel: string; payload?: string }) => {
          if (msg.channel !== NOTIFY_CHANNEL || !msg.payload) return;
          try {
            const change = JSON.parse(msg.payload) as StoreChange;
            watcher.onChange(change);
          } catch (err) {
            this.logger.warn('unparseable notification payload', {
              payload: msg.payload.slice(0, 200),
              err: (err as Error).message,
            });
          }
        });

        await client.connect();
        await client.query(`LISTEN ${NOTIFY_CHANNEL}`);
        retry = RECONNECT_MIN_MS;

        if (reconnecting) {
          // Everything published while we were away is unrecoverable, so the
          // cache has to be rebuilt rather than patched.
          this.logger.info('listener reconnected; resyncing');
          watcher.onResync();
        } else {
          this.logger.info('listening for function changes', { channel: NOTIFY_CHANNEL });
        }
        reconnecting = true;
      } catch (err) {
        if (stopped) return;
        this.logger.warn('listener connect failed; retrying', { err: (err as Error).message });
        client = null;
        scheduleReconnect();
      }
    };

    /** Tear down a dead connection and queue another attempt. */
    const drop = async (): Promise<void> => {
      if (stopped || !client) return;
      const dead = client;
      client = null;
      // Already broken; a failed close is not interesting.
      await dead.end().catch(() => {});
      scheduleReconnect();
    };

    await connect();

    return async () => {
      stopped = true;
      if (timer) clearTimeout(timer);
      timer = null;
      const open = client;
      client = null;
      await open?.end().catch(() => {});
    };
  }

  async close(): Promise<void> {
    await this.pool?.end();
    this.pool = null;
  }
}
