/**
 * The function registry: the control plane's source of truth for what is
 * deployed. In-memory here, deliberately behind an interface so it can be
 * swapped for etcd or Postgres without touching the scheduler.
 */
import { existsSync } from 'node:fs';
import path from 'node:path';
import { errors, FUNCTION_DEFAULTS, type FunctionInput, type FunctionSpec } from '../types.js';

const NAME_RE = /^[a-z0-9][a-z0-9-]{0,62}$/;

export class FunctionRegistry {
  private readonly specs = new Map<string, FunctionSpec>();

  /**
   * Register or update a function. Every deploy bumps the version, which is
   * what signals the pool to drain sandboxes running the old code.
   */
  deploy(input: FunctionInput): FunctionSpec {
    if (!NAME_RE.test(input.name)) {
      throw errors.badRequest(
        `invalid function name "${input.name}": must match ${NAME_RE.source}`,
      );
    }

    const entrypoint = path.resolve(input.entrypoint);
    if (!existsSync(entrypoint)) {
      throw errors.badRequest(`entrypoint does not exist: ${entrypoint}`);
    }

    const previous = this.specs.get(input.name);
    const spec: FunctionSpec = {
      name: input.name,
      entrypoint,
      timeoutMs: input.timeoutMs ?? previous?.timeoutMs ?? FUNCTION_DEFAULTS.timeoutMs,
      memoryMib: input.memoryMib ?? previous?.memoryMib ?? FUNCTION_DEFAULTS.memoryMib,
      minWarm: input.minWarm ?? previous?.minWarm ?? FUNCTION_DEFAULTS.minWarm,
      maxConcurrency:
        input.maxConcurrency ?? previous?.maxConcurrency ?? FUNCTION_DEFAULTS.maxConcurrency,
      env: input.env ?? previous?.env ?? { ...FUNCTION_DEFAULTS.env },
      version: (previous?.version ?? 0) + 1,
    };

    if (spec.timeoutMs <= 0) throw errors.badRequest('timeoutMs must be positive');
    if (spec.memoryMib < 32) throw errors.badRequest('memoryMib must be at least 32');
    if (spec.maxConcurrency < 1) throw errors.badRequest('maxConcurrency must be at least 1');
    if (spec.minWarm > spec.maxConcurrency) {
      throw errors.badRequest('minWarm cannot exceed maxConcurrency');
    }

    this.specs.set(spec.name, spec);
    return spec;
  }

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

  delete(name: string): boolean {
    return this.specs.delete(name);
  }
}
