/**
 * Cache-invalidation policy tests.
 *
 * These drive a fake change feed rather than Postgres, because what is under
 * test is how the registry *reacts* to events -- version guards, out-of-order
 * reads, resync after a gap. Those are the parts that are easy to get subtly
 * wrong and impossible to observe from the outside once they are. Whether
 * Postgres actually delivers the events is checked in postgres.test.ts against
 * a real server.
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { FunctionRegistry, type RegistryChange } from '../src/registry/functions.js';
import {
  MemoryStore,
  type StoreChange,
  type StoreWatcher,
  type Unwatch,
  type WatchableStore,
} from '../src/registry/store.js';

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const HELLO = path.join(REPO_ROOT, 'examples/hello/index.mjs');

/**
 * A store whose change feed is driven by the test.
 *
 * `getDelayMs` exists to force the out-of-order case: two events arrive, the
 * reads they trigger complete in the wrong order, and the guard has to hold.
 */
class FakeWatchableStore extends MemoryStore implements WatchableStore {
  override readonly name = 'fake-watchable';
  private watcher: StoreWatcher | null = null;
  getDelayMs = 0;
  getCalls = 0;

  async watch(watcher: StoreWatcher): Promise<Unwatch> {
    this.watcher = watcher;
    return async () => {
      this.watcher = null;
    };
  }

  /**
   * Read now, answer later.
   *
   * This models READ COMMITTED faithfully: a SELECT takes its snapshot when the
   * statement starts, so a slow read returns the row as it was then, not as it
   * is when the result finally arrives. Reading *after* the delay would make
   * every read return the newest row and quietly make the out-of-order case
   * untestable.
   */
  override async get(name: string) {
    this.getCalls++;
    const snapshot = await super.get(name);
    if (this.getDelayMs) await new Promise((r) => setTimeout(r, this.getDelayMs));
    return snapshot;
  }

  /** Publish an event as if another node had written. */
  emit(change: StoreChange): void {
    this.watcher?.onChange(change);
  }

  /** Simulate the listener reconnecting after a gap. */
  resync(): void {
    this.watcher?.onResync();
  }

  get listening(): boolean {
    return this.watcher !== null;
  }
}

/** Wait for `check` to hold, so tests do not depend on a fixed delay. */
async function until(check: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!check()) {
    if (Date.now() > deadline) throw new Error('condition never held');
    await new Promise((r) => setTimeout(r, 5));
  }
}

describe('cluster cache invalidation', () => {
  it('does nothing on a store that cannot publish changes', async () => {
    // Single-node with the memory store is still correct; there is simply
    // nobody to hear from.
    const registry = new FunctionRegistry(new MemoryStore());
    assert.equal(await registry.startWatching(), false);
    assert.equal(registry.watching, false);
    await registry.close();
  });

  it('applies another node deploy to the local cache', async () => {
    const store = new FakeWatchableStore();
    const changes: RegistryChange[] = [];
    const registry = new FunctionRegistry(store, {
      onRemoteChange: (c) => changes.push(c),
    });
    await registry.hydrate();
    assert.equal(await registry.startWatching(), true);

    // Another node writes straight to the store, then the feed reports it.
    const remote = await store.put({
      name: 'shared',
      entrypoint: HELLO,
      timeoutMs: 5_000,
      memoryMib: 256,
      minWarm: 0,
      maxConcurrency: 8,
      env: {},
    });
    store.emit({ op: 'upsert', name: 'shared', version: remote.version });

    await until(() => registry.has('shared'));
    assert.equal(registry.get('shared').memoryMib, 256);
    assert.deepEqual(changes, [{ type: 'upsert', spec: registry.get('shared') }]);

    await registry.close();
  });

  it('ignores an event for a version it already has', async () => {
    const store = new FakeWatchableStore();
    const registry = new FunctionRegistry(store);
    await registry.hydrate();
    await registry.startWatching();

    const spec = await registry.deploy({ name: 'echo', entrypoint: HELLO });
    const before = store.getCalls;

    // The trigger echoes our own write back to us. Re-reading it would be
    // pure waste on every deploy in the cluster.
    store.emit({ op: 'upsert', name: 'echo', version: spec.version });
    await new Promise((r) => setTimeout(r, 50));

    assert.equal(store.getCalls, before, 'own write should not trigger a read');
    assert.equal(registry.get('echo').version, spec.version);

    await registry.close();
  });

  it('does not let a slow read overwrite a newer version', async () => {
    const store = new FakeWatchableStore();
    const registry = new FunctionRegistry(store);
    await registry.hydrate();
    await registry.startWatching();

    const base = {
      entrypoint: HELLO,
      timeoutMs: 5_000,
      memoryMib: 128,
      minWarm: 0,
      maxConcurrency: 8,
      env: {},
    };
    await store.put({ name: 'racy', ...base }); // v1

    // The v1 event arrives first and its read is slow, so it snapshots v1 and
    // then stalls.
    store.getDelayMs = 60;
    store.emit({ op: 'upsert', name: 'racy', version: 1 });
    await new Promise((r) => setTimeout(r, 10));

    // v2 is written and its event overtakes the stalled read.
    const v2 = await store.put({ name: 'racy', ...base });
    store.getDelayMs = 0;
    store.emit({ op: 'upsert', name: 'racy', version: 2 });

    await until(() => registry.has('racy') && registry.get('racy').version === 2);
    // Now let the stalled v1 read deliver its stale answer.
    await new Promise((r) => setTimeout(r, 120));

    assert.equal(
      registry.get('racy').version,
      v2.version,
      'an older read must not clobber a newer cached version',
    );

    await registry.close();
  });

  it('removes a function deleted on another node', async () => {
    const store = new FakeWatchableStore();
    const changes: RegistryChange[] = [];
    const registry = new FunctionRegistry(store, { onRemoteChange: (c) => changes.push(c) });
    await registry.hydrate();
    await registry.startWatching();

    await registry.deploy({ name: 'doomed', entrypoint: HELLO });
    assert.equal(registry.has('doomed'), true);

    await store.delete('doomed');
    store.emit({ op: 'delete', name: 'doomed' });

    await until(() => !registry.has('doomed'));
    assert.deepEqual(changes, [{ type: 'delete', name: 'doomed' }]);

    await registry.close();
  });

  it('rebuilds the whole cache after a gap in the feed', async () => {
    const store = new FakeWatchableStore();
    const changes: RegistryChange[] = [];
    const registry = new FunctionRegistry(store, { onRemoteChange: (c) => changes.push(c) });
    await registry.hydrate();
    await registry.startWatching();

    await registry.deploy({ name: 'stays', entrypoint: HELLO });
    await registry.deploy({ name: 'vanishes', entrypoint: HELLO });

    // While "disconnected": one function redeployed, one removed, one added.
    // None of these events reach us -- that is what a gap means.
    const base = {
      entrypoint: HELLO,
      timeoutMs: 5_000,
      memoryMib: 512,
      minWarm: 0,
      maxConcurrency: 8,
      env: {},
    };
    await store.put({ name: 'stays', ...base });
    await store.delete('vanishes');
    await store.put({ name: 'appeared', ...base });

    changes.length = 0;
    store.resync();

    await until(() => registry.has('appeared') && !registry.has('vanishes'));

    assert.equal(registry.get('stays').memoryMib, 512, 'missed redeploy must be picked up');
    assert.equal(registry.has('vanishes'), false, 'missed delete must be applied');
    assert.equal(registry.has('appeared'), true, 'missed deploy must be applied');

    const kinds = changes.map((c) => `${c.type}:${c.type === 'delete' ? c.name : c.spec.name}`);
    assert.ok(kinds.includes('delete:vanishes'), `expected a delete, got ${kinds.join(', ')}`);
    assert.ok(kinds.includes('upsert:appeared'), `expected an upsert, got ${kinds.join(', ')}`);

    await registry.close();
  });

  it('stops listening when closed', async () => {
    const store = new FakeWatchableStore();
    const registry = new FunctionRegistry(store);
    await registry.hydrate();
    await registry.startWatching();
    assert.equal(store.listening, true);

    await registry.close();
    assert.equal(store.listening, false, 'close must tear the listener down');
    assert.equal(registry.watching, false);
  });
});
