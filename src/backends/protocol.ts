/**
 * Host <-> guest wire protocol.
 *
 * Deliberately transport-agnostic: the process backend carries these messages
 * over Node's fork IPC, the Firecracker backend over a vsock stream with
 * newline-delimited JSON framing. Keeping one message shape means the
 * scheduler never learns which backend it is talking to.
 */

export type HostMessage =
  /** Import the handler module. Sent once, immediately after handshake. */
  | { t: 'load'; entrypoint: string }
  /** Run the handler. At most one in flight per sandbox. */
  | { t: 'invoke'; id: string; payload: unknown; deadlineMs: number }
  /** Cooperative shutdown; the guest exits 0. */
  | { t: 'shutdown' };

export type GuestMessage =
  /** Runtime is up and listening. Ends the boot phase. */
  | { t: 'ready'; pid: number }
  /** Handler module imported. Ends the load phase. */
  | { t: 'loaded'; loadMs: number }
  /** Handler module failed to import; the sandbox is unusable. */
  | { t: 'load_failed'; message: string; stack?: string }
  /** Invocation finished, successfully or not. */
  | {
      t: 'result';
      id: string;
      ok: boolean;
      body?: unknown;
      error?: { message: string; stack?: string };
      handlerMs: number;
    };

/**
 * Newline-delimited JSON decoder for stream transports.
 * Buffers partial lines across chunk boundaries.
 */
export class NdjsonDecoder {
  private buffer = '';

  constructor(private readonly onMessage: (msg: unknown) => void) {}

  push(chunk: Buffer | string): void {
    this.buffer += chunk.toString('utf8');
    let idx: number;
    while ((idx = this.buffer.indexOf('\n')) !== -1) {
      const line = this.buffer.slice(0, idx).trim();
      this.buffer = this.buffer.slice(idx + 1);
      if (!line) continue;
      try {
        this.onMessage(JSON.parse(line));
      } catch {
        // A malformed frame means the guest is writing garbage to the control
        // channel. Drop it; the health check will retire the sandbox.
      }
    }
  }
}

export function encodeNdjson(msg: unknown): string {
  return JSON.stringify(msg) + '\n';
}
