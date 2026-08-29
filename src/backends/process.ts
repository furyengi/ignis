/**
 * Process isolation backend.
 *
 * Each sandbox is a forked Node process with its own heap ceiling, a scrubbed
 * environment and no inherited stdio. That is weaker isolation than a microVM
 * -- same kernel, same user, no seccomp -- so it is the development and CI
 * backend, not the multi-tenant one. It exists so the scheduler, warm pool and
 * cold-start accounting can be built and measured anywhere Node runs.
 *
 * For the real isolation story see `firecracker.ts`.
 */
import { fork, type ChildProcess } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { log } from '../log.js';
import {
  errors,
  type ColdStartBreakdown,
  type FunctionSpec,
  type InvokeRequest,
  type InvokeResult,
  TIMEOUT_GRACE_MS,
} from '../types.js';
import type { Sandbox, SandboxBackend } from './backend.js';
import type { GuestMessage, HostMessage } from './protocol.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
/** Compiled shim, sibling to this file under dist/. */
const SHIM_PATH = path.resolve(HERE, '../guest/shim.js');

/** Boot budget. A sandbox that cannot say `ready` in this long is broken. */
const BOOT_TIMEOUT_MS = 10_000;

class ProcessSandbox implements Sandbox {
  readonly id = `sb-${randomUUID().slice(0, 8)}`;
  lastUsedAt = performance.now();
  alive = true;

  /** Resolver for the single in-flight invocation, if any. */
  private pending: {
    id: string;
    resolve: (r: InvokeResult) => void;
    reject: (e: Error) => void;
    timer: NodeJS.Timeout;
  } | null = null;

  constructor(
    readonly functionName: string,
    readonly version: number,
    readonly coldStart: ColdStartBreakdown,
    private readonly child: ChildProcess,
    private readonly logger = log.child({ component: 'sandbox' }),
  ) {
    this.child.on('message', (m) => this.onGuestMessage(m as GuestMessage));
    this.child.on('exit', (code, signal) => this.onExit(code, signal));
    this.child.on('error', (err) => {
      this.logger.warn('sandbox child error', { id: this.id, err: (err as Error).message });
      this.onExit(null, null);
    });
  }

  private onGuestMessage(msg: GuestMessage): void {
    if (msg.t !== 'result') return;
    const p = this.pending;
    // A result for a request we already timed out and abandoned.
    if (!p || p.id !== msg.id) return;
    clearTimeout(p.timer);
    this.pending = null;
    this.lastUsedAt = performance.now();
    p.resolve({
      id: msg.id,
      ok: msg.ok,
      body: msg.body,
      error: msg.error,
      handlerMs: msg.handlerMs,
    });
  }

  private onExit(code: number | null, signal: NodeJS.Signals | null): void {
    if (!this.alive) return;
    this.alive = false;
    const p = this.pending;
    if (p) {
      clearTimeout(p.timer);
      this.pending = null;
      // The handler took the sandbox down with it. Surface that as a crash
      // rather than letting the caller hang until its own deadline.
      p.reject(errors.crashed(`sandbox exited (code=${code} signal=${signal}) mid-invocation`));
    }
    this.logger.debug('sandbox exited', { id: this.id, code, signal });
  }

  private send(msg: HostMessage): void {
    if (this.alive && this.child.connected) this.child.send(msg);
  }

  invoke(req: InvokeRequest): Promise<InvokeResult> {
    if (!this.alive) return Promise.reject(errors.crashed('sandbox is not alive'));
    if (this.pending) {
      // The scheduler owns single-flight; reaching here is a scheduler bug.
      return Promise.reject(new Error(`sandbox ${this.id} already has an in-flight invocation`));
    }

    return new Promise<InvokeResult>((resolve, reject) => {
      // The guest aborts at `deadlineMs`; this fires only if that failed to
      // produce a result, so a cooperative handler always wins the race.
      const timer = setTimeout(() => {
        this.pending = null;
        // A timed-out sandbox may still be spinning in the handler, so it can
        // never be returned to the pool -- kill it.
        void this.destroy('invocation timeout');
        reject(errors.timeout(req.deadlineMs));
      }, req.deadlineMs + TIMEOUT_GRACE_MS);
      timer.unref?.();

      this.pending = { id: req.id, resolve, reject, timer };
      this.send({ t: 'invoke', id: req.id, payload: req.payload, deadlineMs: req.deadlineMs });
    });
  }

  async destroy(reason: string): Promise<void> {
    if (!this.alive) return;
    this.alive = false;
    this.logger.debug('destroying sandbox', { id: this.id, reason });
    this.send({ t: 'shutdown' });

    await new Promise<void>((resolve) => {
      const kill = setTimeout(() => {
        this.child.kill('SIGKILL');
        resolve();
      }, 250);
      kill.unref?.();
      this.child.once('exit', () => {
        clearTimeout(kill);
        resolve();
      });
    });
  }
}

export class ProcessBackend implements SandboxBackend {
  readonly name = 'process';

  async preflight(): Promise<void> {
    if (!existsSync(SHIM_PATH)) {
      throw new Error(`guest shim missing at ${SHIM_PATH} -- run "npm run build" first`);
    }
  }

  async create(spec: FunctionSpec): Promise<Sandbox> {
    const bootStart = performance.now();

    const child = fork(SHIM_PATH, [], {
      // Only what the function declares, plus the channel selector. The host's
      // environment -- credentials included -- is not inherited.
      env: { ...spec.env, IGNIS_CHANNEL: 'ipc', IGNIS_FUNCTION: spec.name },
      // Heap ceiling is the memory limit V8 will actually enforce.
      execArgv: [`--max-old-space-size=${spec.memoryMib}`],
      // Guest stdout/stderr are piped so a chatty handler cannot interleave
      // with the control plane's own logs.
      stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
      serialization: 'advanced',
    });

    const fnLog = log.child({ component: 'guest', fn: spec.name });
    child.stderr?.on('data', (b: Buffer) =>
      fnLog.warn('guest stderr', { line: b.toString().trimEnd() }),
    );
    child.stdout?.on('data', (b: Buffer) =>
      fnLog.info('guest stdout', { line: b.toString().trimEnd() }),
    );

    const coldStart = await this.handshake(child, spec, bootStart);
    return new ProcessSandbox(spec.name, spec.version, coldStart, child);
  }

  /**
   * Drive the boot sequence: wait for `ready`, send `load`, wait for `loaded`.
   * Resolves with the two phases timed separately, which is what makes it
   * possible to say whether a slow cold start is process spawn or user code.
   */
  private handshake(
    child: ChildProcess,
    spec: FunctionSpec,
    bootStart: number,
  ): Promise<ColdStartBreakdown> {
    return new Promise((resolve, reject) => {
      let bootMs = 0;

      const timer = setTimeout(() => {
        cleanup();
        child.kill('SIGKILL');
        reject(new Error(`sandbox for ${spec.name} did not boot within ${BOOT_TIMEOUT_MS}ms`));
      }, BOOT_TIMEOUT_MS);
      timer.unref?.();

      const cleanup = () => {
        clearTimeout(timer);
        child.off('message', onMessage);
        child.off('exit', onExit);
      };

      const onExit = (code: number | null) => {
        cleanup();
        reject(new Error(`sandbox for ${spec.name} exited during boot (code=${code})`));
      };

      const onMessage = (raw: unknown) => {
        const msg = raw as GuestMessage;
        if (msg.t === 'ready') {
          bootMs = performance.now() - bootStart;
          child.send({ t: 'load', entrypoint: spec.entrypoint } satisfies HostMessage);
        } else if (msg.t === 'loaded') {
          cleanup();
          resolve({ bootMs, loadMs: msg.loadMs, totalMs: performance.now() - bootStart });
        } else if (msg.t === 'load_failed') {
          cleanup();
          child.kill('SIGKILL');
          reject(new Error(`handler load failed for ${spec.name}: ${msg.message}`));
        }
      };

      child.on('message', onMessage);
      child.on('exit', onExit);
    });
  }

  async shutdown(): Promise<void> {
    // Individual sandboxes are owned by the pool; nothing backend-wide to free.
  }
}
