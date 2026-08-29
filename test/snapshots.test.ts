/**
 * Snapshot orchestration tests.
 *
 * These drive a fake store rather than Firecracker, and that is the point: the
 * policy under test -- capture once per version, restore on cold start, evict
 * superseded images, degrade instead of failing -- is backend-agnostic, so it
 * can be tested on a laptop with no KVM. What is *not* covered here is whether
 * Firecracker's snapshot ioctls work, which no amount of mocking could tell us.
 */
import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import type {
  Sandbox,
  SnapshotCapableBackend,
  SnapshotInfo,
  SnapshotStore,
} from '../src/backends/backend.js';
import { ProcessBackend } from '../src/backends/process.js';
import { parseSnapshotName } from '../src/backends/firecracker.js';
import { Scheduler } from '../src/scheduler.js';
import type { FunctionSpec, InvokeRequest, InvokeResult } from '../src/types.js';

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const HELLO = path.join(REPO_ROOT, 'examples/hello/index.mjs');

class FakeSandbox implements Sandbox {
  static seq = 0;
  readonly id = `fake-${FakeSandbox.seq++}`;
  lastUsedAt = performance.now();
  alive = true;

  constructor(
    readonly functionName: string,
    readonly version: number,
    readonly coldStart: { bootMs: number; loadMs: number; totalMs: number; restored?: boolean },
  ) {}

  async invoke(req: InvokeRequest): Promise<InvokeResult> {
    return { id: req.id, ok: true, body: { from: this.id }, handlerMs: 0.1 };
  }

  async destroy(): Promise<void> {
    this.alive = false;
  }
}

interface FakeControls {
  captureCalls: number;
  restoreCalls: number;
  bootCalls: number;
  evictCalls: Array<{ fn: string; keep: number | null }>;
  failCapture: Error | null;
  failRestore: Error | null;
  /** Delay injected into capture, to exercise single-flight and timeouts. */
  captureDelayMs: number;
}

/**
 * A backend that can "snapshot" by remembering which versions were captured.
 * Boot is 100ms, restore is 5ms -- the same order-of-magnitude gap Firecracker
 * shows, so assertions about which path ran are meaningful.
 */
function makeFakeBackend(): { backend: SnapshotCapableBackend; ctl: FakeControls } {
  const images = new Map<string, SnapshotInfo>();
  const ctl: FakeControls = {
    captureCalls: 0,
    restoreCalls: 0,
    bootCalls: 0,
    evictCalls: [],
    failCapture: null,
    failRestore: null,
    captureDelayMs: 0,
  };

  const key = (fn: string, v: number) => `${fn}@${v}`;

  const store: SnapshotStore = {
    async capture(spec) {
      ctl.captureCalls++;
      if (ctl.captureDelayMs) await new Promise((r) => setTimeout(r, ctl.captureDelayMs));
      if (ctl.failCapture) throw ctl.failCapture;
      const info: SnapshotInfo = {
        fn: spec.name,
        version: spec.version,
        bytes: spec.memoryMib * 1024 * 1024,
        captureMs: 120,
        capturedAt: Date.now(),
      };
      images.set(key(spec.name, spec.version), info);
      return info;
    },
    async restore(spec) {
      ctl.restoreCalls++;
      if (ctl.failRestore) throw ctl.failRestore;
      if (!images.has(key(spec.name, spec.version))) return null;
      return new FakeSandbox(spec.name, spec.version, {
        bootMs: 5,
        loadMs: 0,
        totalMs: 5,
        restored: true,
      });
    },
    async evict(fn, keepVersion) {
      ctl.evictCalls.push({ fn, keep: keepVersion });
      let removed = 0;
      for (const [k, info] of [...images]) {
        if (info.fn === fn && info.version !== keepVersion) {
          images.delete(k);
          removed++;
        }
      }
      return removed;
    },
    async list() {
      return [...images.values()];
    },
  };

  const backend: SnapshotCapableBackend = {
    name: 'fake',
    snapshots: store,
    async preflight() {},
    async shutdown() {},
    async create(spec: FunctionSpec) {
      ctl.bootCalls++;
      return new FakeSandbox(spec.name, spec.version, {
        bootMs: 100,
        loadMs: 20,
        totalMs: 120,
      });
    },
  };

  return { backend, ctl };
}

describe('snapshot orchestration', () => {
  it('captures once at deploy and serves cold starts from the image', async () => {
    const { backend, ctl } = makeFakeBackend();
    const sched = new Scheduler(backend);
    await sched.start();

    await sched.deploy({ name: 'snap', entrypoint: HELLO });
    assert.equal(ctl.captureCalls, 1, 'deploy should capture exactly once');

    const res = await sched.invoke('snap', {});
    assert.equal(res.result.ok, true);
    assert.equal(ctl.bootCalls, 0, 'cold start should restore, not boot');
    assert.equal(ctl.restoreCalls, 1);

    await sched.shutdown();
  });

  it('prewarms from the snapshot rather than booting', async () => {
    const { backend, ctl } = makeFakeBackend();
    const sched = new Scheduler(backend);
    await sched.start();

    // Capture must happen before prewarm, or the deploy pays full boot price
    // for exactly the sandboxes the snapshot exists to make cheap.
    await sched.deploy({ name: 'warm', entrypoint: HELLO, minWarm: 3 });

    assert.equal(ctl.bootCalls, 0, 'prewarm should not boot when an image exists');
    assert.equal(ctl.restoreCalls, 3, 'each prewarmed sandbox is a restore');

    await sched.shutdown();
  });

  it('captures once per version under concurrent deploys', async () => {
    const { backend, ctl } = makeFakeBackend();
    ctl.captureDelayMs = 40;
    const sched = new Scheduler(backend);
    await sched.start();

    // Same version, captured concurrently: without single-flight both callers
    // boot a VM and race to write the same files.
    const spec = { name: 'race', entrypoint: HELLO, version: 1 } as FunctionSpec;
    const [a, b, c] = await Promise.all([
      sched.snapshots!.capture(spec),
      sched.snapshots!.capture(spec),
      sched.snapshots!.capture(spec),
    ]);

    assert.equal(ctl.captureCalls, 1, 'concurrent captures should coalesce');
    assert.deepEqual(a, b);
    assert.deepEqual(b, c);

    await sched.shutdown();
  });

  it('keeps the deploy successful when capture fails', async () => {
    const { backend, ctl } = makeFakeBackend();
    ctl.failCapture = new Error('no space left on device');
    const sched = new Scheduler(backend);
    await sched.start();

    // A backend that cannot snapshot today is a performance regression, not a
    // failed deploy.
    const spec = await sched.deploy({ name: 'nospace', entrypoint: HELLO });
    assert.equal(spec.version, 1);

    const res = await sched.invoke('nospace', {});
    assert.equal(res.result.ok, true);
    assert.equal(ctl.bootCalls, 1, 'should fall back to a real boot');

    const stats = await sched.stats();
    assert.equal(stats.snapshots?.captureFailures, 1);

    await sched.shutdown();
  });

  it('falls back to booting when a restore fails, and stops retrying it', async () => {
    const { backend, ctl } = makeFakeBackend();
    const sched = new Scheduler(backend);
    await sched.start();
    await sched.deploy({ name: 'corrupt', entrypoint: HELLO });

    ctl.failRestore = new Error('snapshot file is truncated');

    const first = await sched.invoke('corrupt', {});
    assert.equal(first.result.ok, true, 'a bad image must not fail the invocation');
    assert.equal(ctl.bootCalls, 1);

    const restoresAfterFirst = ctl.restoreCalls;

    // Poisoned: subsequent cold starts should not pay for a doomed restore.
    await sched.invoke('corrupt', {});
    await sched.invoke('corrupt', {});
    assert.equal(
      ctl.restoreCalls,
      restoresAfterFirst,
      'poisoned version should not be retried on every cold start',
    );

    const stats = await sched.stats();
    assert.equal(stats.snapshots?.restoreFailures, 1);

    await sched.shutdown();
  });

  it('evicts the superseded image on redeploy', async () => {
    const { backend, ctl } = makeFakeBackend();
    const sched = new Scheduler(backend);
    await sched.start();

    await sched.deploy({ name: 'bump', entrypoint: HELLO });
    await sched.deploy({ name: 'bump', entrypoint: HELLO });

    const live = (await sched.stats()).snapshots!.live;
    assert.equal(live.length, 1, 'only the current version should survive');
    assert.equal(live[0]!.version, 2);
    assert.ok(
      ctl.evictCalls.some((e) => e.fn === 'bump' && e.keep === 2),
      'redeploy should evict everything but v2',
    );

    await sched.shutdown();
  });

  it('evicts every image when the function is removed', async () => {
    const { backend } = makeFakeBackend();
    const sched = new Scheduler(backend);
    await sched.start();

    await sched.deploy({ name: 'gone', entrypoint: HELLO });
    assert.equal((await sched.stats()).snapshots!.live.length, 1);

    await sched.remove('gone');
    assert.equal(
      (await sched.stats()).snapshots!.live.length,
      0,
      'removing a function must not leave its memory images on disk',
    );

    await sched.shutdown();
  });

  it('does not hang the deploy when capture stalls', async () => {
    const { backend, ctl } = makeFakeBackend();
    ctl.captureDelayMs = 5_000;
    const sched = new Scheduler(backend, undefined, { captureTimeoutMs: 150 });
    await sched.start();

    const started = performance.now();
    await sched.deploy({ name: 'stalled', entrypoint: HELLO });
    const elapsed = performance.now() - started;

    assert.ok(elapsed < 2_000, `deploy should not wait for a stalled capture (took ${elapsed}ms)`);
    assert.equal((await sched.stats()).snapshots?.captureFailures, 1);

    await sched.shutdown();
  });
});

describe('backends without snapshot support', () => {
  let sched: Scheduler;

  before(async () => {
    sched = new Scheduler(new ProcessBackend());
    await sched.start();
  });

  after(async () => {
    await sched.shutdown();
  });

  it('degrades to ordinary cold boots instead of erroring', async () => {
    // The process backend cannot freeze a live Node heap, so it declines the
    // capability rather than faking it.
    assert.equal(sched.snapshots, null);

    await sched.deploy({ name: 'plain', entrypoint: HELLO });
    const res = await sched.invoke('plain', { name: 'x' });

    assert.equal(res.result.ok, true);
    assert.equal(res.timing.warm, false);
    assert.ok(res.timing.coldStartMs > 0, 'should be a real boot');
    assert.equal((await sched.stats()).snapshots, null);
  });
});

describe('snapshot filenames', () => {
  it('anchors on the last -v<n>, so hyphenated names survive', () => {
    // Function names may contain hyphens. A non-greedy match would read
    // "api-gateway-v2.snap" as fn="api", and evicting "api" would then delete
    // a different function's image.
    assert.deepEqual(parseSnapshotName('hello-v1.snap'), { fn: 'hello', version: 1 });
    assert.deepEqual(parseSnapshotName('api-gateway-v2.mem'), { fn: 'api-gateway', version: 2 });
    assert.deepEqual(parseSnapshotName('svc-v1-v12.snap'), { fn: 'svc-v1', version: 12 });
  });

  it('ignores anything that is not a snapshot pair', () => {
    const junk = ['README', 'hello-v1.tmp', 'hello.snap', 'hello-vx.snap', 'hello-v1.snap.bak'];
    for (const name of junk) {
      assert.equal(parseSnapshotName(name), null, name);
    }
  });
});
