/**
 * The function registry: the control plane's source of truth for what is
 * deployed.
 *
 * Validation and default-merging live here; durability lives in a FunctionStore
 * behind it. The registry keeps every spec in memory as a write-through cache,
 * because `get` runs on the invocation path and a database round trip per
 * request would cost more than the sandbox it is looking up. Writes go to the
 * store first and update the cache only once they are durable -- the reverse
 * would let a failed write leave the cache claiming a version that does not
 * exist.
 */
import { existsSync } from 'node:fs';
import path from 'node:path';
import { log } from '../log.js';
import { errors, FUNCTION_DEFAULTS, type FunctionInput, type FunctionSpec } from '../types.js';
import {
  isWatchable,
  MemoryStore,
  type FunctionStore,
  type PendingSpec,
  type StoreChange,
  type Unwatch,
} from './store.js';

const NAME_RE = /^[a-z0-9][a-z0-9-]{0,62}$/;

/** What changed in the cache, so the scheduler can react to another node. */
export type RegistryChange =
  | { type: 'upsert'; spec: FunctionSpec }
  | { type: 'delete'; name: string };

export interface RegistryOptions {
  /**
   * Called when a *remote* write changes this node's cache. Local deploys do
   * not fire it -- the caller already knows what it just did.
   */
  onRemoteChange?: (change: RegistryChange) => void;
}

export class FunctionRegistry {
  private readonly specs = new Map<string, FunctionSpec>();
  private unwatch: Unwatch | null = null;

  constructor(
    private readonly store: FunctionStore = new MemoryStore(),
    private readonly opts: RegistryOptions = {},
    private readonly logger = log.child({ component: 'registry' }),
  ) {}

  get backend(): string {
    return this.store.name;
  }

  /** True once this node is receiving other nodes' writes. */
  get watching(): boolean {
    return this.unwatch !== null;
  }

  /**
   * Load persisted specs into the cache. Call once at startup, before serving.
   *
   * Entrypoints are checked but a missing one is not fatal: refusing to start
   * because one function's file moved would take down every other function too.
   * The deployment stays listed and fails loudly when invoked.
   */
  async hydrate(): Promise<FunctionSpec[]> {
    await this.store.init();
    const specs = await this.store.load();
    for (const spec of specs) {
      this.specs.set(spec.name, spec);
      if (!existsSync(spec.entrypoint)) {
        this.logger.warn('restored function has a missing entrypoint', {
          fn: spec.name,
          version: spec.version,
          entrypoint: spec.entrypoint,
        });
      }
    }
    if (specs.length > 0) {
      this.logger.info('restored functions', { count: specs.length, store: this.store.name });
    }
    return specs;
  }

  /**
   * Register or update a function. Every deploy bumps the version, which is
   * what signals the pool to drain sandboxes running the old code.
   */
  async deploy(input: FunctionInput): Promise<FunctionSpec> {
    if (!NAME_RE.test(input.name)) {
      throw errors.badRequest(
        `invalid function name "${input.name}": must match ${NAME_RE.source}`,
      );
    }

    const entrypoint = path.resolve(input.entrypoint);
    if (!existsSync(entrypoint)) {
      throw errors.badRequest(`entrypoint does not exist: ${entrypoint}`);
    }

    // Omitted fields fall back to the current deployment before the built-in
    // defaults, so a redeploy that only changes the code keeps its tuning.
    const previous = this.specs.get(input.name);
    const pending: PendingSpec = {
      name: input.name,
      entrypoint,
      timeoutMs: input.timeoutMs ?? previous?.timeoutMs ?? FUNCTION_DEFAULTS.timeoutMs,
      memoryMib: input.memoryMib ?? previous?.memoryMib ?? FUNCTION_DEFAULTS.memoryMib,
      minWarm: input.minWarm ?? previous?.minWarm ?? FUNCTION_DEFAULTS.minWarm,
      maxConcurrency:
        input.maxConcurrency ?? previous?.maxConcurrency ?? FUNCTION_DEFAULTS.maxConcurrency,
      env: input.env ?? previous?.env ?? { ...FUNCTION_DEFAULTS.env },
    };

    if (pending.timeoutMs <= 0) throw errors.badRequest('timeoutMs must be positive');
    if (pending.memoryMib < 32) throw errors.badRequest('memoryMib must be at least 32');
    if (pending.maxConcurrency < 1) throw errors.badRequest('maxConcurrency must be at least 1');
    if (pending.minWarm > pending.maxConcurrency) {
      throw errors.badRequest('minWarm cannot exceed maxConcurrency');
    }

    // The store owns version allocation; only cache what it confirms.
    const spec = await this.store.put(pending);
    this.specs.set(spec.name, spec);
    return spec;
  }

  /**
   * Start applying other nodes' writes to this node's cache.
   *
   * No-op on a store that cannot publish changes -- a single-node deployment
   * is still correct, it just has nobody to hear from.
   */
  async startWatching(): Promise<boolean> {
    if (!isWatchable(this.store) || this.unwatch) return false;
    this.unwatch = await this.store.watch({
      onChange: (change) => void this.applyRemote(change),
      onResync: () => void this.resync(),
    });
    return true;
  }

  /**
   * Apply one remote change.
   *
   * The event carries a version but not the spec, so the spec is re-read. That
   * makes two rapid deploys safe: the reads can complete out of order, and the
   * version guard below drops the older one rather than letting v4 land on top
   * of v5. Sending the whole spec in the payload would avoid the read, but
   * NOTIFY payloads are capped at 8000 bytes and would still need the guard.
   */
  private async applyRemote(change: StoreChange): Promise<void> {
    try {
      if (change.op === 'delete') {
        if (!this.specs.delete(change.name)) return;
        this.logger.info('function removed on another node', { fn: change.name });
        this.opts.onRemoteChange?.({ type: 'delete', name: change.name });
        return;
      }

      const current = this.specs.get(change.name);
      // Our own write, echoed back by the trigger. Nothing to do.
      if (current && change.version !== undefined && change.version <= current.version) return;

      const spec = await this.store.get(change.name);
      // Deployed and deleted again before we could read it.
      if (!spec) return;
      const latest = this.specs.get(change.name);
      if (latest && spec.version <= latest.version) return;

      this.specs.set(spec.name, spec);
      this.logger.info('function updated on another node', {
        fn: spec.name,
        version: spec.version,
      });
      this.opts.onRemoteChange?.({ type: 'upsert', spec });
    } catch (err) {
      // A dropped event leaves the cache stale until the next write or
      // reconnect; louder failure would not make it fresher.
      this.logger.warn('could not apply remote change', {
        fn: change.name,
        err: (err as Error).message,
      });
    }
  }

  /**
   * Rebuild the cache from scratch after a gap in the change feed.
   *
   * Reloading rather than patching is the only correct response: we cannot know
   * what was missed, so anything short of a full reload leaves the cache
   * plausibly wrong in a way nothing later would correct.
   */
  private async resync(): Promise<void> {
    try {
      const specs = await this.store.load();
      const seen = new Set<string>();

      for (const spec of specs) {
        seen.add(spec.name);
        const current = this.specs.get(spec.name);
        if (current && current.version === spec.version) continue;
        this.specs.set(spec.name, spec);
        this.opts.onRemoteChange?.({ type: 'upsert', spec });
      }

      // Deletions that happened while we were disconnected.
      for (const name of [...this.specs.keys()]) {
        if (seen.has(name)) continue;
        this.specs.delete(name);
        this.opts.onRemoteChange?.({ type: 'delete', name });
      }

      this.logger.info('resynced after listener gap', { functions: specs.length });
    } catch (err) {
      this.logger.warn('resync failed; cache may be stale', { err: (err as Error).message });
    }
  }

  /** Hot path: served from cache, never from the store. */
  get(name: string): FunctionSpec {
    const spec = this.specs.get(name);
    if (!spec) throw errors.notFound(`function "${name}"`);
    return spec;
  }

  has(name: string): boolean {
    return this.specs.has(name);
  }

  list(): FunctionSpec[] {
    return [...this.specs.values()].sort((a, b) => a.name.localeCompare(b.name));
  }

  async delete(name: string): Promise<boolean> {
    const removed = await this.store.delete(name);
    this.specs.delete(name);
    return removed;
  }

  async close(): Promise<void> {
    // Stop listening before closing the pool, so a reconnect cannot race the
    // shutdown and resurrect a connection.
    await this.unwatch?.();
    this.unwatch = null;
    await this.store.close();
  }
}
