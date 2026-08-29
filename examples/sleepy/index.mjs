/**
 * I/O-bound handler that honours the deadline. Demonstrates the guest-side
 * AbortSignal: the host will kill the sandbox at the timeout regardless, but a
 * cooperative handler can unwind cleanly and return a partial result.
 */
export async function handler(payload, ctx) {
  const requestedMs = Number(payload?.ms ?? 100);
  const started = performance.now();

  await new Promise((resolve) => {
    const timer = setTimeout(resolve, requestedMs);
    ctx.signal.addEventListener('abort', () => {
      clearTimeout(timer);
      resolve();
    });
  });

  return {
    requestedMs,
    sleptMs: performance.now() - started,
    aborted: ctx.signal.aborted,
    remainingMs: ctx.remainingMs(),
  };
}
