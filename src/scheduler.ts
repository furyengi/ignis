/**
 * The invocation path.
 *
 * Everything above this is plumbing and everything below is isolation; this is
 * the layer that decides where a request runs and accounts for where its
 * latency went. The timing breakdown it produces (queue / cold start / handler)
 * is the whole point of the project -- an aggregate "p99 latency" number cannot
 * tell you whether you have a capacity problem or a boot problem.
 */
import { randomUUID } from 'node:crypto';
import { log } from './log.js';
import { RuntimeMetrics } from './metrics.js';
import { PoolManager, POOL_DEFAULTS, type PoolOptions } from './pool.js';
import { FunctionRegistry } from './registry/functions.js';
import type { FunctionStore } from './registry/store.js';
import { isSnapshotCapable, type SandboxBackend } from './backends/backend.js';
import {
  SnapshotManager,
  withSnapshots,
  SNAPSHOT_DEFAULTS,
  type SnapshotOptions,
} from './snapshots.js';
import {
  IgnisError,
  type FunctionInput,
  type FunctionSpec,
  type InvokeResponse,
} from './types.js';

export class Scheduler {
  readonly registry: FunctionRegistry;
  readonly metrics = new RuntimeMetrics();
  private readonly pools: PoolManager;
  private readonly logger = log.child({ component: 'scheduler' });
  /** Null when the backend cannot snapshot; everything degrades to cold boots. */
  readonly snapshots: SnapshotManager | null;
  private readonly backend: SandboxBackend;

  constructor(
    backend: SandboxBackend,
    opts: PoolOptions = POOL_DEFAULTS,
    snapshotOpts?: Partial<SnapshotOptions>,
    /** Durable spec storage. Defaults to in-memory, which forgets on restart. */
    store?: FunctionStore,
  ) {
    this.registry = new FunctionRegistry(store);
    if (isSnapshotCapable(backend)) {
      this.snapshots = new SnapshotManager(backend.snapshots, this.metrics, {
        ...SNAPSHOT_DEFAULTS,
        ...snapshotOpts,
      });
      // The pool asks this wrapper for sandboxes and never learns whether it
      // got a restore or a boot.
      this.backend = withSnapshots(backend, this.snapshots);
    } else {
      this.snapshots = null;
      this.backend = backend;
    }
    this.pools = new PoolManager(this.backend, opts);
  }

  async start(): Promise<void> {
    await this.backend.preflight();
    this.pools.start();
    this.logger.info('scheduler started', { backend: this.backend.name });
  }

  /**
   * Restore persisted functions and warm them back up.
   *
   * Prewarming is best-effort per function: one deployment whose entrypoint has
   * gone missing must not stop the others from coming back.
   */
  async hydrate(): Promise<FunctionSpec[]> {
    const specs = await this.registry.hydrate();
    await Promise.all(
      specs.map(async (spec) => {
        const pool = this.pools.forFunction(spec);
        try {
          await pool.prewarm();
        } catch (err) {
          this.logger.warn('could not prewarm restored function', {
            fn: spec.name,
            version: spec.version,
            err: (err as Error).message,
          });
        }
      }),
    );
    return specs;
  }

  /** Register a function and bring its warm pool up to `minWarm`. */
  async deploy(input: FunctionInput): Promise<FunctionSpec> {
    const spec = await this.registry.deploy(input);
    const pool = this.pools.forFunction(spec);
    this.metrics.counters.inc('deploys_total');
    this.logger.info('deployed', {
      fn: spec.name,
      version: spec.version,
      minWarm: spec.minWarm,
    });
    // Capture before prewarming so the prewarmed sandboxes are themselves
    // restores rather than boots -- otherwise the deploy pays full price for
    // exactly the sandboxes the snapshot was meant to make cheap.
    await this.snapshots?.onDeploy(spec);

    // Prewarming is part of the deploy: returning before the pool is warm
    // would hand the first caller a cold start the operator thought they had
    // paid to avoid.
    await pool.prewarm();
    return spec;
  }

  async remove(name: string): Promise<void> {
    if (!this.registry.has(name)) throw new IgnisError(`function "${name}" not found`, 'NOT_FOUND', 404);
    await this.registry.delete(name);
    await this.pools.remove(name);
    await this.snapshots?.onRemove(name);
    this.logger.info('removed', { fn: name });
  }

  /**
   * Run one invocation end to end: acquire a sandbox, run under the function's
   * timeout, return it to the pool, and record the split timings.
   */
  async invoke(name: string, payload: unknown): Promise<InvokeResponse> {
    const spec = this.registry.get(name);
    const pool = this.pools.forFunction(spec);
    const id = randomUUID();
    const startedAt = performance.now();

    this.metrics.counters.inc('invocations_total');

    const lease = await pool.acquire();
    this.metrics.queueWait.record(lease.queueMs);
    if (lease.warm) {
      this.metrics.counters.inc('warm_hits_total');
    } else {
      this.metrics.counters.inc('cold_starts_total');
      this.metrics.coldStart.record(lease.coldStartMs);
    }

    // The handler's budget is what remains after queueing and boot, not the
    // full timeout -- otherwise a slow cold start silently extends the SLA.
    const spent = performance.now() - startedAt;
    const deadlineMs = Math.max(1, spec.timeoutMs - spent);

    let healthy = true;
    try {
      const result = await lease.sandbox.invoke({ id, payload, deadlineMs });
      const totalMs = performance.now() - startedAt;

      // A handler that threw leaves module-level state in an unknown shape.
      // Cheaper to retire the sandbox than to reason about what it kept.
      healthy = result.ok;

      this.metrics.handler.record(result.handlerMs);
      (lease.warm ? this.metrics.warmLatency : this.metrics.coldLatency).record(totalMs);
      this.metrics.counters.inc(result.ok ? 'invocations_ok' : 'invocations_error');

      const timing = {
        queueMs: lease.queueMs,
        coldStartMs: lease.coldStartMs,
        handlerMs: result.handlerMs,
        totalMs,
        warm: lease.warm,
      };

      this.logger.debug('invocation complete', {
        fn: name,
        id,
        sandbox: lease.sandbox.id,
        ok: result.ok,
        ...timing,
      });

      return { result, timing, sandboxId: lease.sandbox.id, backend: this.backend.name };
    } catch (err) {
      healthy = false;
      this.metrics.counters.inc('invocations_failed');
      const e = err as Error;
      this.logger.warn('invocation failed', { fn: name, id, err: e.message });
      throw err;
    } finally {
      pool.release(lease.sandbox, healthy);
    }
  }

  async stats() {
    return {
      backend: this.backend.name,
      store: this.registry.backend,
      snapshots: (await this.snapshots?.stats()) ?? null,
      functions: this.registry.list().map((s) => ({
        name: s.name,
        version: s.version,
        minWarm: s.minWarm,
        maxConcurrency: s.maxConcurrency,
        timeoutMs: s.timeoutMs,
        memoryMib: s.memoryMib,
      })),
      pools: this.pools.stats(),
      metrics: this.metrics.toJSON(),
    };
  }

  async shutdown(): Promise<void> {
    this.logger.info('scheduler shutting down');
    await this.pools.shutdown();
    await this.registry.close();
  }
}
