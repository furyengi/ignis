/**
 * Fixture: an uncooperative handler. Blocks the event loop in a synchronous
 * spin, so it can neither observe the AbortSignal nor send a result. The only
 * thing that can stop it is the host killing the sandbox, which is exactly the
 * backstop this fixture exists to prove.
 */
export async function handler(payload) {
  const spinMs = Number(payload?.ms ?? 10_000);
  const until = Date.now() + spinMs;
  while (Date.now() < until) {
    /* deliberately blocking */
  }
  return { spun: spinMs };
}
