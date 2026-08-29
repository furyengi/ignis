/**
 * Postgres store tests.
 *
 * These need a real database -- there is no fake here, because the properties
 * worth testing are properties of Postgres: does the upsert allocate versions
 * atomically under concurrency, does the advisory lock keep two nodes from
 * deadlocking on migration, does jsonb round-trip. A mock would assert that my
 * mock behaves like my mock.
 *
 * CI supplies one via a service container. Without IGNIS_TEST_DATABASE_URL the
 * suite skips rather than failing, so `npm test` still works on a laptop.
 */
import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { FunctionRegistry } from '../src/registry/functions.js';
import { PostgresStore } from '../src/registry/postgres.js';
import type { PendingSpec } from '../src/registry/store.js';

const DATABASE_URL = process.env.IGNIS_TEST_DATABASE_URL;
const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const HELLO = path.join(REPO_ROOT, 'examples/hello/index.mjs');

function pending(name: string, over: Partial<PendingSpec> = {}): PendingSpec {
  return {
    name,
    entrypoint: HELLO,
    timeoutMs: 5_000,
    memoryMib: 128,
    minWarm: 0,
    maxConcurrency: 32,
    env: {},
    ...over,
  };
}

describe(
  'postgres store',
  { skip: DATABASE_URL ? false : 'set IGNIS_TEST_DATABASE_URL to run' },
  () => {
    let store: PostgresStore;

    before(async () => {
      store = new PostgresStore({ connectionString: DATABASE_URL! });
      await store.init();
      // Start from a known state; the container may be reused across suites.
      for (const spec of await store.load()) await store.delete(spec.name);
    });

    after(async () => {
      await store.close();
    });

    it('is idempotent on init, so a restart is not a migration', async () => {
      // Second init on the same database must be a no-op, not an error.
      await store.init();
      await store.init();
    });

    it('allocates the first version as 1 and bumps thereafter', async () => {
      const v1 = await store.put(pending('versioned'));
      assert.equal(v1.version, 1);
      const v2 = await store.put(pending('versioned'));
      assert.equal(v2.version, 2);
      const v3 = await store.put(pending('versioned'));
      assert.equal(v3.version, 3);
    });

    it('allocates distinct versions under concurrent deploys', async () => {
      // The property that makes this store safe for more than one control
      // plane. A read-modify-write in the application would let both callers
      // read v0 and both write v1, leaving two code versions claiming one
      // number -- and the pool decides what is stale by comparing versions.
      const N = 12;
      const results = await Promise.all(
        Array.from({ length: N }, () => store.put(pending('contended'))),
      );

      const versions = results.map((r) => r.version).sort((a, b) => a - b);
      assert.deepEqual(
        versions,
        Array.from({ length: N }, (_, i) => i + 1),
        'every concurrent deploy must get its own version',
      );
    });

    it('round-trips env and limits', async () => {
      const stored = await store.put(
        pending('shaped', {
          memoryMib: 512,
          timeoutMs: 1_234,
          minWarm: 3,
          maxConcurrency: 9,
          env: { REGION: 'eu-west-1', 'ODD KEY': 'a=b,c' },
        }),
      );

      const loaded = (await store.load()).find((s) => s.name === 'shaped');
      assert.ok(loaded, 'stored function should come back from load()');
      assert.equal(loaded.memoryMib, 512);
      assert.equal(loaded.timeoutMs, 1_234);
      assert.equal(loaded.minWarm, 3);
      assert.equal(loaded.maxConcurrency, 9);
      assert.deepEqual(loaded.env, { REGION: 'eu-west-1', 'ODD KEY': 'a=b,c' });
      assert.equal(loaded.version, stored.version);
    });

    it('reports whether a delete removed anything', async () => {
      await store.put(pending('doomed'));
      assert.equal(await store.delete('doomed'), true);
      assert.equal(await store.delete('doomed'), false, 'second delete removes nothing');
      assert.equal(
        (await store.load()).some((s) => s.name === 'doomed'),
        false,
      );
    });

    it('survives a control plane restart', async () => {
      const first = new FunctionRegistry(new PostgresStore({ connectionString: DATABASE_URL! }));
      await first.hydrate();
      await first.deploy({ name: 'durable', entrypoint: HELLO, memoryMib: 256, minWarm: 2 });
      await first.deploy({ name: 'durable', entrypoint: HELLO });
      await first.close();

      // A brand new process, sharing nothing but the database.
      const second = new FunctionRegistry(new PostgresStore({ connectionString: DATABASE_URL! }));
      await second.hydrate();

      const spec = second.get('durable');
      assert.equal(spec.version, 2, 'version must not restart at 1');
      assert.equal(spec.memoryMib, 256, 'tuning must survive');
      assert.equal(spec.minWarm, 2);

      // The carried-forward defaults still work after a restart, which only
      // holds because hydrate() repopulated the cache the merge reads from.
      const v3 = await second.deploy({ name: 'durable', entrypoint: HELLO });
      assert.equal(v3.version, 3);
      assert.equal(v3.memoryMib, 256);
      await second.close();
    });

    it('serialises concurrent migrations instead of deadlocking', async () => {
      // Two nodes booting at once both run CREATE TABLE IF NOT EXISTS, which
      // Postgres will deadlock on rather than politely no-op. The advisory
      // lock is what makes this safe.
      const nodes = Array.from(
        { length: 4 },
        () => new PostgresStore({ connectionString: DATABASE_URL! }),
      );
      try {
        await Promise.all(nodes.map((n) => n.init()));
      } finally {
        await Promise.all(nodes.map((n) => n.close()));
      }
    });
  },
);
