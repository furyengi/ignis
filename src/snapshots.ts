/**
 * Snapshot orchestration.
 *
 * The backend knows how to freeze and resume one machine. This decides *when*:
 * capture once at deploy time, restore on every cold start, and delete the
 * images for versions nobody can reach any more.
 *
 * All of it is backend-agnostic on purpose. Firecracker is the only backend
 * that can actually snapshot, but none of the policy here is Firecracker-shaped,
 * so it is testable against a fake store on any machine -- which matters,
 * because the policy is where the bugs are, not the ioctls.
 */
import type {
  Sandbox,
  SandboxBackend,
  SnapshotInfo,
  SnapshotStore,
} from './backends/backend.js';
import { log, type Logger } from './log.js';
import type { RuntimeMetrics } from './metrics.js';
import type { FunctionSpec } from './types.js';

export interface SnapshotOptions {
  /**
   * Capture during `deploy`, so the first caller already benefits. When false,
   * capture happens lazily on the first cold start instead -- cheaper deploys,
   * but somebody pays full price for the first invocation.
   */
  captureOnDeploy: boolean;
  /** Delete snapshots for superseded versions after a redeploy. */
  evictSuperseded: boolean;
  /**
   * Give up on capture after this long. A capture that hangs must never hang
   * the deploy that triggered it.
   */
  captureTimeoutMs: number;
}

export const SNAPSHOT_DEFAULTS: SnapshotOptions = {
  captureOnDeploy: true,
  evictSuperseded: true,
  captureTimeoutMs: 60_000,
};

export interface SnapshotStats {
  captures: number;
  captureFailures: number;
  restores: number;
  restoreFailures: number;
  evicted: number;
  /** Snapshots currently on disk. */
  live: SnapshotInfo[];
  bytes: number;
}

/** A version whose restore blew up; do not keep trying it every cold start. */
const POISON_TTL_MS = 30_000;

export class SnapshotManager {
  private readonly inFlight = new Map<string, Promise<SnapshotInfo | null>>();
  private readonly poisoned = new Map<string, number>();
  private readonly captured = new Map<string, SnapshotInfo>();

  private captures = 0;
  private captureFailures = 0;
  private restores = 0;
  private restoreFailures = 0;
  private evicted = 0;

  constructor(
    private readonly store: SnapshotStore,
    private readonly metrics: RuntimeMetrics,
    private readonly opts: SnapshotOptions = SNAPSHOT_DEFAULTS,
    private readonly logger: Logger = log.child({ component: 'snapshots' }),
  ) {}

  private static key(fn: string, version: number): string {
    return `${fn}@${version}`;
  }

  /**
   * Capture for the new version and drop the old one.
   *
   * Deliberately never throws: a backend that cannot snapshot today is a
   * performance regression, not a failed deploy. The function still works, it
   * just cold-boots.
   */
  async onDeploy(spec: FunctionSpec): Promise<SnapshotInfo | null> {
    if (this.opts.evictSuperseded) {
      // Before capturing, so a redeploy does not transiently hold two full
      // memory images for the same function.
      await this.evict(spec.name, spec.version);
    }
    if (!this.opts.captureOnDeploy) return null;
    return this.capture(spec);
  }

  async onRemove(fn: string): Promise<void> {
    await this.evict(fn, null);
    for (const key of [...this.captured.keys()]) {
      if (key.startsWith(`${fn}@`)) this.captured.delete(key);
    }
  }

  /**
   * Capture, at most once per version even under concurrent callers.
   *
   * Without the single-flight map, two simultaneous deploys of the same
   * version each boot a VM and race to write the same files -- doubling the
   * cost to produce a snapshot that may be a torn mix of both.
   */
  async capture(spec: FunctionSpec): Promise<SnapshotInfo | null> {
    const key = SnapshotManager.key(spec.name, spec.version);
    const existing = this.captured.get(key);
    if (existing) return existing;

    const running = this.inFlight.get(key);
    if (running) return running;

    const task = this.runCapture(spec, key).finally(() => this.inFlight.delete(key));
    this.inFlight.set(key, task);
    return task;
  }

  private async runCapture(spec: FunctionSpec, key: string): Promise<SnapshotInfo | null> {
    const started = performance.now();
    try {
      const info = await withTimeout(
        this.store.capture(spec),
        this.opts.captureTimeoutMs,
        `snapshot capture for ${key}`,
      );
      this.captured.set(key, info);
      this.poisoned.delete(key);
      this.captures++;
      this.metrics.counters.inc('snapshot_captures_total');
      this.logger.info('snapshot captured', {
        fn: spec.name,
        version: spec.version,
        ms: Math.round(info.captureMs),
        mib: +(info.bytes / 1024 / 1024).toFixed(1),
      });
      return info;
    } catch (err) {
      this.captureFailures++;
      this.metrics.counters.inc('snapshot_capture_failures_total');
      // Warn, do not throw. The deploy is still good.
      this.logger.warn('snapshot capture failed; falling back to cold boots', {
        fn: spec.name,
        version: spec.version,
        ms: Math.round(performance.now() - started),
        err: (err as Error).message,
      });
      return null;
    }
  }

  /**
   * Resume a sandbox for `spec`, or null to tell the caller to boot normally.
   *
   * A restore that fails must not fail the invocation -- the sandbox is
   * recoverable by booting. It does poison the version briefly, so one bad
   * image cannot add a failed restore to the latency of every cold start.
   */
  async tryRestore(spec: FunctionSpec): Promise<Sandbox | null> {
    const key = SnapshotManager.key(spec.name, spec.version);

    const poisonedAt = this.poisoned.get(key);
    if (poisonedAt !== undefined) {
      if (performance.now() - poisonedAt < POISON_TTL_MS) return null;
      this.poisoned.delete(key);
    }

    const started = performance.now();
    try {
      const sandbox = await this.store.restore(spec);
      if (!sandbox) return null;
      this.restores++;
      this.metrics.counters.inc('snapshot_restores_total');
      this.metrics.coldStart.record(sandbox.coldStart.totalMs);
      this.logger.debug('restored from snapshot', {
        fn: spec.name,
        version: spec.version,
        ms: +(performance.now() - started).toFixed(2),
      });
      return sandbox;
    } catch (err) {
      this.restoreFailures++;
      this.poisoned.set(key, performance.now());
      this.metrics.counters.inc('snapshot_restore_failures_total');
      this.logger.warn('snapshot restore failed; booting instead', {
        fn: spec.name,
        version: spec.version,
        err: (err as Error).message,
      });
      return null;
    }
  }

  private async evict(fn: string, keepVersion: number | null): Promise<void> {
    try {
      const n = await this.store.evict(fn, keepVersion);
      if (n > 0) {
        this.evicted += n;
        this.metrics.counters.inc('snapshot_evictions_total', n);
        this.logger.info('evicted stale snapshots', { fn, kept: keepVersion, count: n });
      }
      for (const [key, info] of [...this.captured]) {
        if (info.fn === fn && info.version !== keepVersion) this.captured.delete(key);
      }
    } catch (err) {
      // Leaking a stale image wastes disk; failing the deploy over it would be
      // worse.
      this.logger.warn('snapshot eviction failed', { fn, err: (err as Error).message });
    }
  }

  async stats(): Promise<SnapshotStats> {
    let live: SnapshotInfo[] = [];
    try {
      live = await this.store.list();
    } catch {
      live = [...this.captured.values()];
    }
    return {
      captures: this.captures,
      captureFailures: this.captureFailures,
      restores: this.restores,
      restoreFailures: this.restoreFailures,
      evicted: this.evicted,
      live,
      bytes: live.reduce((sum, s) => sum + s.bytes, 0),
    };
  }
}

/**
 * Wrap a backend so `create` prefers a snapshot restore.
 *
 * A decorator rather than a branch inside the pool: the pool's job is capacity,
 * not provenance. It asks for a sandbox and gets one, and the only visible
 * difference is that `coldStart.restored` is true and the number is small.
 */
export function withSnapshots(
  backend: SandboxBackend,
  manager: SnapshotManager,
): SandboxBackend {
  return {
    name: `${backend.name}+snapshot`,
    preflight: () => backend.preflight(),
    shutdown: () => backend.shutdown(),
    async create(spec: FunctionSpec): Promise<Sandbox> {
      const restored = await manager.tryRestore(spec);
      if (restored) return restored;
      return backend.create(spec);
    },
  };
}

async function withTimeout<T>(p: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      p,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${what} timed out after ${ms}ms`)), ms);
        timer.unref?.();
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}
