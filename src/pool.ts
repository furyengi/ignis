/**
 * Per-function warm pool.
 *
 * Three ideas do most of the work here:
 *
 *  - **LIFO, not FIFO.** Idle sandboxes are reused most-recently-used first.
 *    Under partial load that keeps a small subset genuinely hot and lets the
 *    rest age out, instead of round-robining traffic across every sandbox and
 *    keeping all of them marginally warm.
 *
 *  - **Reservations before creation.** Capacity is claimed synchronously, then
 *    the sandbox is booted. Without that, N concurrent requests all observe
 *    `total < max` and stampede past the concurrency ceiling.
 *
 *  - **Version-pinned reuse.** A sandbox booted against v3 is never handed out
 *    after v4 deploys; it is drained. This is why a deploy cannot serve stale
 *    code from the pool.
 */
import { log } from './log.js';
import type { Sandbox, SandboxBackend } from './backends/backend.js';
import { errors, type FunctionSpec } from './types.js';

export interface PoolOptions {
  /** Idle sandboxes above `minWarm` are reaped after this long unused. */
  idleTtlMs: number;
  /** How often the reaper runs. */
  reapIntervalMs: number;
  /** Ceiling on idle sandboxes retained per function. */
  maxIdle: number;
  /** How long a request will wait for capacity before being shed. */
  acquireTimeoutMs: number;
}

export const POOL_DEFAULTS: PoolOptions = {
  idleTtlMs: 60_000,
  reapIntervalMs: 5_000,
  maxIdle: 16,
  acquireTimeoutMs: 10_000,
};

/** What a caller gets back, including whether it paid for a boot. */
export interface Lease {
  sandbox: Sandbox;
  /** True when this lease came from the pool rather than a fresh boot. */
  warm: boolean;
  /** Boot cost in ms; 0 on a warm hit. */
  coldStartMs: number;
  /** Time spent waiting for capacity. */
  queueMs: number;
}

interface Waiter {
  resolve: (lease: Lease) => void;
  reject: (err: Error) => void;
  enqueuedAt: number;
  timer: NodeJS.Timeout;
}

export interface PoolStats {
  fn: string;
  idle: number;
  busy: number;
  reserved: number;
  waiting: number;
  warmHits: number;
  coldStarts: number;
  reaped: number;
}

export class FunctionPool {
  /** Idle sandboxes, most-recently-released last (pop() gives MRU). */
  private idle: Sandbox[] = [];
  private busy = new Set<Sandbox>();
  /** Capacity claimed by an in-progress boot. */
  private reserved = 0;
  private waiters: Waiter[] = [];
  private draining = false;

  private warmHits = 0;
  private coldStarts = 0;
  private reaped = 0;

  private readonly logger = log.child({ component: 'pool' });

  constructor(
    private spec: FunctionSpec,
    private readonly backend: SandboxBackend,
    private readonly opts: PoolOptions,
  ) {}

  get functionName(): string {
    return this.spec.name;
  }

  private get total(): number {
    return this.idle.length + this.busy.size + this.reserved;
  }

  /**
   * Point the pool at a new spec version. Idle sandboxes on the old version
   * are destroyed immediately; busy ones drain naturally on release.
   */
  updateSpec(spec: FunctionSpec): void {
    this.spec = spec;
    const stale = this.idle.filter((s) => s.version !== spec.version);
    this.idle = this.idle.filter((s) => s.version === spec.version);
    for (const s of stale) void s.destroy('spec version changed');
    if (stale.length) {
      this.logger.info('drained stale sandboxes', {
        fn: spec.name,
        count: stale.length,
        version: spec.version,
      });
    }
  }

  /** Boot sandboxes up to `minWarm` so the first real request is never cold. */
  async prewarm(): Promise<void> {
    const missing = Math.max(0, this.spec.minWarm - this.total);
    if (missing === 0) return;
    this.logger.info('prewarming', { fn: this.spec.name, count: missing });
    await Promise.allSettled(
      Array.from({ length: missing }, async () => {
        this.reserved++;
        try {
          const sandbox = await this.backend.create(this.spec);
          this.reserved--;
          // A request may have queued while this was booting; give it away
          // rather than parking it in idle.
          if (!this.handOff(sandbox, false, sandbox.coldStart.totalMs)) {
            this.idle.push(sandbox);
          }
        } catch (err) {
          this.reserved--;
          this.logger.error('prewarm failed', {
            fn: this.spec.name,
            err: (err as Error).message,
          });
        }
      }),
    );
  }

  async acquire(): Promise<Lease> {
    if (this.draining) throw errors.capacity(this.spec.name);
    const start = performance.now();

    // Fast path: a live, current-version sandbox is sitting idle. Only take it
    // when nobody is queued, so waiters are not starved by fresh arrivals.
    if (this.waiters.length === 0) {
      const sandbox = this.takeIdle();
      if (sandbox) {
        this.busy.add(sandbox);
        this.warmHits++;
        return { sandbox, warm: true, coldStartMs: 0, queueMs: 0 };
      }
    }

    // Room to grow: claim the slot before awaiting so concurrent callers see it.
    if (this.total < this.spec.maxConcurrency) {
      this.reserved++;
      try {
        const sandbox = await this.backend.create(this.spec);
        this.reserved--;
        this.busy.add(sandbox);
        this.coldStarts++;
        return {
          sandbox,
          warm: false,
          coldStartMs: sandbox.coldStart.totalMs,
          queueMs: performance.now() - start,
        };
      } catch (err) {
        this.reserved--;
        // Freeing the reservation may have unblocked a queued request.
        this.pump();
        throw err;
      }
    }

    // At the ceiling: wait for a release.
    return this.enqueue(start);
  }

  /** Pop the most recent usable sandbox, discarding dead or stale ones. */
  private takeIdle(): Sandbox | null {
    while (this.idle.length > 0) {
      const sandbox = this.idle.pop()!;
      if (sandbox.alive && sandbox.version === this.spec.version) return sandbox;
      void sandbox.destroy(sandbox.alive ? 'stale version' : 'dead in pool');
    }
    return null;
  }

  private enqueue(start: number): Promise<Lease> {
    return new Promise<Lease>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.waiters = this.waiters.filter((w) => w.timer !== timer);
        reject(errors.capacity(this.spec.name));
      }, this.opts.acquireTimeoutMs);
      timer.unref?.();
      this.waiters.push({ resolve, reject, enqueuedAt: start, timer });
    });
  }

  /** Give a sandbox to the oldest waiter. Returns false if nobody is waiting. */
  private handOff(sandbox: Sandbox, warm: boolean, coldStartMs: number): boolean {
    const waiter = this.waiters.shift();
    if (!waiter) return false;
    clearTimeout(waiter.timer);
    this.busy.add(sandbox);
    if (warm) this.warmHits++;
    waiter.resolve({
      sandbox,
      warm,
      coldStartMs,
      queueMs: performance.now() - waiter.enqueuedAt,
    });
    return true;
  }

  /**
   * Return a sandbox after use. `healthy: false` retires it -- used when the
   * handler threw in a way that may have corrupted module state, or the
   * sandbox died mid-invocation.
   */
  release(sandbox: Sandbox, healthy = true): void {
    this.busy.delete(sandbox);

    const reusable =
      healthy && sandbox.alive && sandbox.version === this.spec.version && !this.draining;

    if (!reusable) {
      void sandbox.destroy(healthy ? 'not reusable' : 'unhealthy after invocation');
      // Capacity freed; a queued request may now be able to boot its own.
      this.pump();
      return;
    }

    if (this.handOff(sandbox, true, 0)) return;

    if (this.idle.length >= this.opts.maxIdle) {
      void sandbox.destroy('idle pool full');
      this.reaped++;
      return;
    }

    sandbox.lastUsedAt = performance.now();
    this.idle.push(sandbox);
  }

  /** After capacity frees up, let one queued request try to boot a sandbox. */
  private pump(): void {
    if (this.waiters.length === 0) return;
    if (this.total >= this.spec.maxConcurrency) return;

    const waiter = this.waiters.shift()!;
    clearTimeout(waiter.timer);
    this.reserved++;
    this.backend
      .create(this.spec)
      .then((sandbox) => {
        this.reserved--;
        this.busy.add(sandbox);
        this.coldStarts++;
        waiter.resolve({
          sandbox,
          warm: false,
          coldStartMs: sandbox.coldStart.totalMs,
          queueMs: performance.now() - waiter.enqueuedAt,
        });
      })
      .catch((err) => {
        this.reserved--;
        waiter.reject(err as Error);
      });
  }

  /** Destroy idle sandboxes that have aged out, respecting `minWarm`. */
  reap(now = performance.now()): number {
    const keep: Sandbox[] = [];
    let killed = 0;
    // Walk oldest-first (index 0 is LRU) so the survivors are the hottest.
    for (const sandbox of this.idle) {
      const expired = now - sandbox.lastUsedAt > this.opts.idleTtlMs;
      const surplus = this.idle.length - killed > this.spec.minWarm;
      if (sandbox.alive && (!expired || !surplus)) {
        keep.push(sandbox);
      } else {
        void sandbox.destroy(sandbox.alive ? 'idle ttl expired' : 'dead');
        killed++;
      }
    }
    this.idle = keep;
    this.reaped += killed;
    return killed;
  }

  stats(): PoolStats {
    return {
      fn: this.spec.name,
      idle: this.idle.length,
      busy: this.busy.size,
      reserved: this.reserved,
      waiting: this.waiters.length,
      warmHits: this.warmHits,
      coldStarts: this.coldStarts,
      reaped: this.reaped,
    };
  }

  async drain(): Promise<void> {
    this.draining = true;
    for (const waiter of this.waiters) {
      clearTimeout(waiter.timer);
      waiter.reject(errors.capacity(this.spec.name));
    }
    this.waiters = [];
    const all = [...this.idle, ...this.busy];
    this.idle = [];
    this.busy.clear();
    await Promise.all(all.map((s) => s.destroy('pool draining')));
  }
}

/** Owns one pool per function and runs the shared reaper. */
export class PoolManager {
  private readonly pools = new Map<string, FunctionPool>();
  private reaper: NodeJS.Timeout | null = null;

  constructor(
    private readonly backend: SandboxBackend,
    private readonly opts: PoolOptions = POOL_DEFAULTS,
  ) {}

  start(): void {
    if (this.reaper) return;
    this.reaper = setInterval(() => {
      for (const pool of this.pools.values()) pool.reap();
    }, this.opts.reapIntervalMs);
    // The reaper must never be the reason the process stays alive.
    this.reaper.unref?.();
  }

  forFunction(spec: FunctionSpec): FunctionPool {
    const existing = this.pools.get(spec.name);
    if (existing) {
      existing.updateSpec(spec);
      return existing;
    }
    const pool = new FunctionPool(spec, this.backend, this.opts);
    this.pools.set(spec.name, pool);
    return pool;
  }

  get(name: string): FunctionPool | undefined {
    return this.pools.get(name);
  }

  stats(): PoolStats[] {
    return [...this.pools.values()].map((p) => p.stats());
  }

  async remove(name: string): Promise<void> {
    const pool = this.pools.get(name);
    if (!pool) return;
    this.pools.delete(name);
    await pool.drain();
  }

  async shutdown(): Promise<void> {
    if (this.reaper) clearInterval(this.reaper);
    this.reaper = null;
    await Promise.all([...this.pools.values()].map((p) => p.drain()));
    this.pools.clear();
    await this.backend.shutdown();
  }
}
