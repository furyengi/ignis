import type { ColdStartBreakdown, FunctionSpec, InvokeRequest, InvokeResult } from '../types.js';

/**
 * One isolated execution environment holding exactly one loaded handler.
 *
 * A sandbox is single-tenant and single-flight: the scheduler guarantees at
 * most one outstanding `invoke` at a time. That is what makes a warm pool a
 * pool rather than a shared connection.
 */
export interface Sandbox {
  readonly id: string;
  /** The function this sandbox is specialised for. */
  readonly functionName: string;
  /** Spec version it was booted against; stale versions are drained. */
  readonly version: number;
  /** How long the boot cost, for cold-start accounting. */
  readonly coldStart: ColdStartBreakdown;
  /** False once the process died or was retired. */
  readonly alive: boolean;
  /** Monotonic ms timestamp of the last completed invocation. */
  lastUsedAt: number;

  invoke(req: InvokeRequest): Promise<InvokeResult>;
  /** Best-effort graceful stop, escalating to a kill. */
  destroy(reason: string): Promise<void>;
}

export interface SandboxBackend {
  readonly name: string;
  /**
   * Boot a sandbox and load the handler. Resolves only once the guest is
   * ready to serve, so the returned `coldStart` is the real number.
   */
  create(spec: FunctionSpec): Promise<Sandbox>;
  /** Verify the backend can actually run here; throws with a reason if not. */
  preflight(): Promise<void>;
  /** Release backend-wide resources (tap devices, temp dirs, sockets). */
  shutdown(): Promise<void>;
}
