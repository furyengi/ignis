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
import { MemoryStore, type FunctionStore, type PendingSpec } from './store.js';

const NAME_RE = /^[a-z0-9][a-z0-9-]{0,62}$/;

export class FunctionRegistry {
  private readonly specs = new Map<string, FunctionSpec>();

  constructor(
    private readonly store: FunctionStore = new MemoryStore(),
    private readonly logger = log.child({ component: 'registry' }),
  ) {}

  get backend(): string {
    return this.store.name;
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
    await this.store.close();
  }
}
