/**
 * CPU-bound handler. Exists to show that once the runtime is warm the latency
 * is the handler's own cost, not the platform's.
 */
function fib(n) {
  return n < 2 ? n : fib(n - 1) + fib(n - 2);
}

export async function handler(payload) {
  const n = Math.min(Number(payload?.n ?? 25), 35);
  const started = performance.now();
  const value = fib(n);
  return { n, value, computeMs: performance.now() - started };
}
