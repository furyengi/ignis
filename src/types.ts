/** Core domain types shared across the control plane, scheduler and backends. */

/** A user-supplied function, as registered with the control plane. */
export interface FunctionSpec {
  /** Unique, URL-safe function name. */
  name: string;
  /** Absolute path to the ESM module exporting a `handler`. */
  entrypoint: string;
  /** Hard wall-clock limit for a single invocation. */
  timeoutMs: number;
  /** Memory ceiling for the sandbox, in MiB. Enforced by the backend. */
  memoryMib: number;
  /** Sandboxes to keep warm at all times. 0 means fully on-demand. */
  minWarm: number;
  /** Upper bound on live sandboxes for this function. */
  maxConcurrency: number;
  /** Environment exposed to the guest. */
  env: Record<string, string>;
  /** Monotonic version, bumped on each deploy. Warm sandboxes of an old
   *  version are drained rather than reused. */
  version: number;
}

export type FunctionInput = Partial<FunctionSpec> & Pick<FunctionSpec, 'name' | 'entrypoint'>;

/** Defaults applied to any field the caller omits at deploy time. */
export const FUNCTION_DEFAULTS = {
  timeoutMs: 5_000,
  memoryMib: 128,
  minWarm: 0,
  maxConcurrency: 32,
  env: {} as Record<string, string>,
} as const;

/** The request handed to a sandbox for one invocation. */
export interface InvokeRequest {
  /** Correlation id, echoed in logs and the response. */
  id: string;
  /** Arbitrary JSON payload passed through to the handler. */
  payload: unknown;
  /** Remaining budget when the request reached the sandbox. */
  deadlineMs: number;
}

export interface InvokeResult {
  id: string;
  ok: boolean;
  /** Handler return value, present when `ok`. */
  body?: unknown;
  error?: { message: string; stack?: string };
  /** Time spent inside the handler, measured by the guest shim. */
  handlerMs: number;
}

/** Where the latency for one end-to-end invocation went. */
export interface InvokeTiming {
  /** Time waiting for a sandbox to become available (queueing). */
  queueMs: number;
  /** Sandbox creation, zero on a warm hit. */
  coldStartMs: number;
  /** Guest-reported handler execution time. */
  handlerMs: number;
  /** Full control-plane-to-response wall time. */
  totalMs: number;
  /** False when the invocation paid for a sandbox boot. */
  warm: boolean;
}

export interface InvokeResponse {
  result: InvokeResult;
  timing: InvokeTiming;
  /** Which sandbox served it, for log correlation. */
  sandboxId: string;
  backend: string;
}

/** Breakdown of a cold start, reported by the backend. */
export interface ColdStartBreakdown {
  /** Process/microVM spawn to shim handshake. */
  bootMs: number;
  /** Handshake to handler module loaded and callable. */
  loadMs: number;
  /** bootMs + loadMs. */
  totalMs: number;
  /**
   * True when this sandbox was resumed from a snapshot rather than booted.
   * Restored and booted cold starts differ by an order of magnitude, so
   * averaging them together hides the thing worth knowing.
   */
  restored?: boolean;
}

/**
 * Grace period between the guest's AbortSignal firing and the host killing the
 * sandbox.
 *
 * Without a gap the two timers race: a cooperative handler that unwinds exactly
 * at the deadline may or may not get its result back before the host kills it,
 * which makes timeout behaviour nondeterministic. The window makes the contract
 * explicit -- abort first, kill only if that did not work.
 */
export const TIMEOUT_GRACE_MS = 250;

export class IgnisError extends Error {
  constructor(
    message: string,
    readonly code: string,
    readonly status: number = 500,
  ) {
    super(message);
    this.name = 'IgnisError';
  }
}

export const errors = {
  notFound: (what: string) => new IgnisError(`${what} not found`, 'NOT_FOUND', 404),
  timeout: (ms: number) => new IgnisError(`invocation exceeded ${ms}ms`, 'TIMEOUT', 504),
  capacity: (fn: string) => new IgnisError(`no capacity for ${fn}`, 'CAPACITY', 429),
  badRequest: (msg: string) => new IgnisError(msg, 'BAD_REQUEST', 400),
  crashed: (msg: string) => new IgnisError(msg, 'SANDBOX_CRASHED', 502),
};
