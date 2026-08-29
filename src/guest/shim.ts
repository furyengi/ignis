/**
 * The guest agent. Runs inside the sandbox, not the control plane.
 *
 * Responsibilities, in order: announce readiness, import the handler module
 * once, then serve invocations one at a time until told to shut down. It owns
 * the `handlerMs` measurement because only the guest can see the handler
 * boundary without including IPC time.
 *
 * Transport is selected by IGNIS_CHANNEL:
 *   "ipc"   - Node fork IPC (process backend)
 *   "vsock" - AF_VSOCK stream to the host (Firecracker backend)
 */
import net from 'node:net';
import { pathToFileURL } from 'node:url';
import { NdjsonDecoder, encodeNdjson, type GuestMessage, type HostMessage } from '../backends/protocol.js';

type Handler = (payload: unknown, ctx: InvokeContext) => unknown | Promise<unknown>;

export interface InvokeContext {
  /** Correlation id for this invocation. */
  requestId: string;
  /** Ms remaining before the host abandons the call. */
  remainingMs(): number;
  /** Rejects when the deadline passes; race it against slow I/O. */
  signal: AbortSignal;
}

let handler: Handler | null = null;
let send: (m: GuestMessage) => void = () => {};

async function loadHandler(entrypoint: string): Promise<void> {
  const started = performance.now();
  try {
    // A file:// URL keeps this working on Windows, where a bare absolute path
    // ("C:\...") is not a valid ESM specifier.
    const url = pathToFileURL(entrypoint);
    const mod = (await import(url.href)) as { handler?: Handler; default?: Handler };
    const fn = mod.handler ?? mod.default;
    if (typeof fn !== 'function') {
      throw new Error(`module ${entrypoint} exports no \`handler\` function`);
    }
    handler = fn;
    send({ t: 'loaded', loadMs: performance.now() - started });
  } catch (err) {
    const e = err as Error;
    send({ t: 'load_failed', message: e.message, stack: e.stack });
    // Exit non-zero so the host records a boot failure rather than a timeout.
    process.exit(3);
  }
}

async function runInvoke(msg: Extract<HostMessage, { t: 'invoke' }>): Promise<void> {
  const controller = new AbortController();
  const deadlineAt = performance.now() + msg.deadlineMs;
  // The host enforces the real timeout; this only lets a cooperative handler
  // notice and unwind early.
  const timer = setTimeout(() => controller.abort(), Math.max(0, msg.deadlineMs));
  timer.unref?.();

  const ctx: InvokeContext = {
    requestId: msg.id,
    remainingMs: () => Math.max(0, deadlineAt - performance.now()),
    signal: controller.signal,
  };

  const started = performance.now();
  try {
    const body = await handler!(msg.payload, ctx);
    send({ t: 'result', id: msg.id, ok: true, body, handlerMs: performance.now() - started });
  } catch (err) {
    const e = err as Error;
    send({
      t: 'result',
      id: msg.id,
      ok: false,
      error: { message: e?.message ?? String(err), stack: e?.stack },
      handlerMs: performance.now() - started,
    });
  } finally {
    clearTimeout(timer);
  }
}

function dispatch(raw: unknown): void {
  const msg = raw as HostMessage;
  switch (msg?.t) {
    case 'load':
      void loadHandler(msg.entrypoint);
      break;
    case 'invoke':
      void runInvoke(msg);
      break;
    case 'shutdown':
      process.exit(0);
    default:
      break;
  }
}

function startIpc(): void {
  if (!process.send) throw new Error('IGNIS_CHANNEL=ipc but no IPC channel on this process');
  send = (m) => process.send!(m);
  process.on('message', dispatch);
  send({ t: 'ready', pid: process.pid });
}

/**
 * Firecracker transport.
 *
 * Node has no AF_VSOCK binding, so the guest does not talk to the vsock device
 * directly. The rootfs runs a one-line bridge under the init system:
 *
 *   socat UNIX-LISTEN:/run/ignis.sock,fork VSOCK-CONNECT:2:5252
 *
 * and the shim connects to that Unix socket. Everything downstream -- framing,
 * message shapes, timing -- is identical to the IPC path, which is why the
 * backend can be swapped without the scheduler noticing.
 */
function startVsock(): void {
  const bridgePath = process.env.IGNIS_VSOCK_BRIDGE ?? '/run/ignis.sock';
  const socket = net.connect({ path: bridgePath });
  const decoder = new NdjsonDecoder(dispatch);
  socket.on('data', (c) => decoder.push(c));
  socket.on('error', (e) => {
    process.stderr.write(`shim vsock error: ${(e as Error).message}\n`);
    process.exit(4);
  });
  send = (m) => socket.write(encodeNdjson(m));
  socket.on('connect', () => send({ t: 'ready', pid: process.pid }));
}

const channel = process.env.IGNIS_CHANNEL ?? 'ipc';
if (channel === 'vsock') startVsock();
else startIpc();

// Never let a handler's unhandled rejection take down the sandbox silently;
// the host needs to see a non-zero exit to retire it.
process.on('uncaughtException', (err) => {
  process.stderr.write(`guest uncaught: ${(err as Error).stack ?? err}\n`);
  process.exit(5);
});
