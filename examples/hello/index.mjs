/** Minimal handler: near-zero work, so latency is almost pure runtime overhead. */
export async function handler(payload, ctx) {
  return {
    message: `hello, ${payload?.name ?? 'world'}`,
    requestId: ctx.requestId,
    pid: process.pid,
  };
}
