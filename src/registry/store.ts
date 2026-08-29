/**
 * Durable storage for function specs.
 *
 * The registry above this is a write-through cache: reads are served from
 * memory because `get` sits on the invocation hot path, and a database round
 * trip per request would cost more than the sandbox it is looking up. The store
 * is only touched on deploy, delete and startup.
 *
 * Version allocation belongs to the store, not the caller. It is the one field
 * that must be decided atomically -- two control planes deploying the same
 * function concurrently must not both hand out v4, because the version is what
 * tells the pool which sandboxes are stale.
 */
import type { FunctionSpec } from '../types.js';

/** A spec with everything resolved except the version the store assigns. */
export type PendingSpec = Omit<FunctionSpec, 'version'>;

export interface FunctionStore {
  readonly name: string;
  /** Create schema if needed. Safe to run concurrently from several nodes. */
  init(): Promise<void>;
  /** Every spec on record, for hydrating the cache at startup. */
  load(): Promise<FunctionSpec[]>;
  /** One spec, or null. Used to refresh a single entry after a change event. */
  get(name: string): Promise<FunctionSpec | null>;
  /** Persist, allocating the next version atomically. */
  put(spec: PendingSpec): Promise<FunctionSpec>;
  /** Returns false if there was nothing to delete. */
  delete(name: string): Promise<boolean>;
  close(): Promise<void>;
}

/** A write observed somewhere in the cluster. */
export interface StoreChange {
  name: string;
  op: 'upsert' | 'delete';
  /** Version as of the write. Absent for deletes. */
  version?: number;
}

export interface StoreWatcher {
  /** A single change. May be this node's own write echoed back. */
  onChange(change: StoreChange): void;
  /**
   * The change feed was interrupted and has come back.
   *
   * Change events are fire-and-forget: anything published while the connection
   * was down is gone, with no way to ask for it later. So a reconnect is not a
   * resumption, it is a gap -- the only safe response is to reload everything.
   */
  onResync(): void;
}

/** Stop watching. Safe to call more than once. */
export type Unwatch = () => Promise<void>;

/**
 * A store that can tell this node when another node writes.
 *
 * Optional, mirroring the snapshot capability: a store that cannot do this is
 * still a perfectly good single-node store, and the registry simply keeps a
 * cache that only its own writes update.
 */
export interface WatchableStore extends FunctionStore {
  watch(watcher: StoreWatcher): Promise<Unwatch>;
}

export function isWatchable(s: FunctionStore): s is WatchableStore {
  return typeof (s as WatchableStore).watch === 'function';
}

/**
 * The default. Fast, dependency-free, and forgets everything on restart --
 * which is fine for tests and single-node development and not fine for
 * anything else.
 */
export class MemoryStore implements FunctionStore {
  readonly name: string = 'memory';
  private readonly specs = new Map<string, FunctionSpec>();

  async init(): Promise<void> {}

  async load(): Promise<FunctionSpec[]> {
    return [...this.specs.values()];
  }

  async get(name: string): Promise<FunctionSpec | null> {
    return this.specs.get(name) ?? null;
  }

  async put(spec: PendingSpec): Promise<FunctionSpec> {
    const previous = this.specs.get(spec.name);
    const next: FunctionSpec = { ...spec, version: (previous?.version ?? 0) + 1 };
    this.specs.set(next.name, next);
    return next;
  }

  async delete(name: string): Promise<boolean> {
    return this.specs.delete(name);
  }

  async close(): Promise<void> {
    this.specs.clear();
  }
}
