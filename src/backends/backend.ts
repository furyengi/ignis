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

/** What a captured snapshot cost and occupies. */
export interface SnapshotInfo {
  fn: string;
  version: number;
  /** Total on-disk size: VM state file plus the memory image. */
  bytes: number;
  /** Wall-clock cost of capturing it, including the boot it had to do. */
  captureMs: number;
  capturedAt: number;
}

/**
 * Capture-once, restore-many storage for booted sandboxes.
 *
 * Booting a kernel, starting the runtime and importing the handler's module
 * graph produces a byte-identical VM every time. A snapshot pays that cost once
 * per function version and turns every subsequent cold start into a memory
 * restore.
 *
 * Only backends that can freeze and resume a whole machine implement this. The
 * process backend cannot -- there is no portable way to snapshot a live Node
 * heap -- so it simply does not offer the capability, and the orchestration
 * above it degrades to ordinary cold boots.
 */
export interface SnapshotStore {
  /**
   * Boot one sandbox, let it load the handler, then freeze it. Must be safe to
   * call for a version that already has a snapshot.
   */
  capture(spec: FunctionSpec): Promise<SnapshotInfo>;
  /**
   * Resume a sandbox from `spec`'s snapshot, or null if there is none. The
   * returned sandbox already has the handler loaded -- callers must not load
   * it again.
   */
  restore(spec: FunctionSpec): Promise<Sandbox | null>;
  /** Delete snapshots for `fn` other than `keepVersion`. Returns the count. */
  evict(fn: string, keepVersion: number | null): Promise<number>;
  list(): Promise<SnapshotInfo[]>;
}

export interface SnapshotCapableBackend extends SandboxBackend {
  readonly snapshots: SnapshotStore;
}

export function isSnapshotCapable(b: SandboxBackend): b is SnapshotCapableBackend {
  return typeof (b as SnapshotCapableBackend).snapshots === 'object';
}
