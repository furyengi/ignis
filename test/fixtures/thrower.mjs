/** Fixture: always throws, to exercise error propagation and sandbox retirement. */
export async function handler() {
  throw new Error('deliberate failure from test fixture');
}
