/** Backend selection. One place that knows which isolation strategies exist. */
import os from 'node:os';
import { log } from '../log.js';
import type { SandboxBackend } from './backend.js';
import { FirecrackerBackend } from './firecracker.js';
import { ProcessBackend } from './process.js';

export type BackendName = 'process' | 'firecracker' | 'auto';

export function createBackend(name: BackendName = 'auto'): SandboxBackend {
  if (name === 'auto') {
    // Prefer real isolation wherever it is actually available.
    const resolved = os.platform() === 'linux' ? 'firecracker' : 'process';
    log.info('auto-selected backend', { backend: resolved, platform: os.platform() });
    return createBackend(resolved);
  }
  switch (name) {
    case 'process':
      return new ProcessBackend();
    case 'firecracker':
      return new FirecrackerBackend();
    default:
      throw new Error(`unknown backend: ${name satisfies never}`);
  }
}

/**
 * Try the requested backend and fall back to `process` if it cannot run here.
 * Used by the server so a developer on a laptop gets a working runtime instead
 * of a stack trace, while still being told what they lost.
 */
export async function resolveBackend(name: BackendName = 'auto'): Promise<SandboxBackend> {
  const backend = createBackend(name);
  try {
    await backend.preflight();
    return backend;
  } catch (err) {
    if (backend.name === 'process') throw err;
    log.warn('backend unavailable, falling back to process isolation', {
      requested: backend.name,
      reason: (err as Error).message,
    });
    const fallback = new ProcessBackend();
    await fallback.preflight();
    return fallback;
  }
}
