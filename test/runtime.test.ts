/**
 * Integration tests. These drive the real scheduler against the real process
 * backend -- no mocks -- because the properties worth testing here (warm reuse,
 * concurrency ceilings, timeout enforcement, version draining) only exist once
 * actual sandboxes are booting.
 */
import assert from 'node:assert/strict';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { after, before, describe, it } from 'node:test';
import { ProcessBackend } from '../src/backends/process.js';
import { Histogram } from '../src/metrics.js';
import { POOL_DEFAULTS } from '../src/pool.js';
import { Scheduler } from '../src/scheduler.js';
import { FunctionRegistry } from '../src/registry/functions.js';
import { MemoryStore } from '../src/registry/store.js';

process.env.IGNIS_LOG = 'off';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, '../..');
const HELLO = path.join(REPO_ROOT, 'examples/hello/index.mjs');
const SLEEPY = path.join(REPO_ROOT, 'examples/sleepy/index.mjs');

describe('metrics', () => {
  it('computes nearest-rank percentiles', () => {
    const h = new Histogram();
    for (let i = 1; i <= 100; i++) h.record(i);
    const s = h.snapshot();
    assert.equal(s.count, 100);
    assert.equal(s.min, 1);
    assert.equal(s.max, 100);
    assert.equal(s.p50, 50);
    assert.equal(s.p99, 99);
    assert.equal(s.mean, 50.5);
  });

  it('returns zeros for an empty histogram rather than NaN', () => {
    const s = new Histogram().snapshot();
    assert.equal(s.count, 0);
    assert.equal(s.p99, 0);
    assert.ok(!Number.isNaN(s.mean));
  });
});

describe('registry', () => {
  it('bumps the version on every deploy', async () => {
    const r = new FunctionRegistry();
    assert.equal((await r.deploy({ name: 'a', entrypoint: HELLO })).version, 1);
    assert.equal((await r.deploy({ name: 'a', entrypoint: HELLO })).version, 2);
  });

  it('carries forward fields the redeploy omits', async () => {
    const r = new FunctionRegistry();
    await r.deploy({ name: 'a', entrypoint: HELLO, memoryMib: 256, minWarm: 2 });
    const v2 = await r.deploy({ name: 'a', entrypoint: HELLO });
    assert.equal(v2.memoryMib, 256);
    assert.equal(v2.minWarm, 2);
  });

  it('rejects invalid names, missing entrypoints and impossible limits', async () => {
    const r = new FunctionRegistry();
    await assert.rejects(
      () => r.deploy({ name: 'Bad Name', entrypoint: HELLO }),
      /invalid function name/,
    );
    await assert.rejects(
      () => r.deploy({ name: 'a', entrypoint: '/nope/nope.mjs' }),
      /does not exist/,
    );
    await assert.rejects(
      () => r.deploy({ name: 'a', entrypoint: HELLO, minWarm: 5, maxConcurrency: 2 }),
      /minWarm cannot exceed/,
    );
  });

  it('restores persisted specs into the read cache', async () => {
    // Two registries over one store stands in for a control-plane restart.
    const store = new MemoryStore();
    const first = new FunctionRegistry(store);
    await first.deploy({ name: 'survivor', entrypoint: HELLO, memoryMib: 192 });
    await first.deploy({ name: 'survivor', entrypoint: HELLO });

    const second = new FunctionRegistry(store);
    assert.equal(second.has('survivor'), false, 'cache starts empty');

    await second.hydrate();
    const spec = second.get('survivor');
    assert.equal(spec.version, 2, 'version must survive the restart');
    assert.equal(spec.memoryMib, 192);

    // And the next deploy continues the sequence rather than restarting it.
    assert.equal((await second.deploy({ name: 'survivor', entrypoint: HELLO })).version, 3);
  });
});

describe('scheduler + process backend', () => {
  let scheduler: Scheduler;

  before(async () => {
    scheduler = new Scheduler(new ProcessBackend(), { ...POOL_DEFAULTS, acquireTimeoutMs: 15_000 });
    await scheduler.start();
  });

  after(async () => {
    await scheduler.shutdown();
  });

  it('invokes a handler and returns its value', async () => {
    await scheduler.deploy({ name: 'hello', entrypoint: HELLO });
    const res = await scheduler.invoke('hello', { name: 'ignis' });
    assert.equal(res.result.ok, true);
    assert.deepEqual((res.result.body as { message: string }).message, 'hello, ignis');
  });

  it('reports the first invocation cold and the second warm', async () => {
    await scheduler.deploy({ name: 'coldwarm', entrypoint: HELLO, minWarm: 0 });
    const first = await scheduler.invoke('coldwarm', {});
    const second = await scheduler.invoke('coldwarm', {});

    assert.equal(first.timing.warm, false, 'first invocation should be cold');
    assert.ok(first.timing.coldStartMs > 0, 'cold start should be measured');
    assert.equal(second.timing.warm, true, 'second invocation should reuse the sandbox');
    assert.equal(second.timing.coldStartMs, 0);
    // Reuse means the same sandbox, which is the observable proof of warmth.
    assert.equal(first.sandboxId, second.sandboxId);
  });

  it('prewarms so the very first request is already warm', async () => {
    await scheduler.deploy({ name: 'prewarmed', entrypoint: HELLO, minWarm: 2 });
    const res = await scheduler.invoke('prewarmed', {});
    assert.equal(res.timing.warm, true);
  });

  it('lets a cooperative handler unwind on the deadline instead of killing it', async () => {
    // The handler watches ctx.signal, so it should return a real result at the
    // deadline rather than being destroyed. The grace window is what makes this
    // deterministic instead of a race with the host's kill timer.
    await scheduler.deploy({ name: 'cooperative', entrypoint: SLEEPY, timeoutMs: 150 });
    const res = await scheduler.invoke('cooperative', { ms: 5_000 });

    assert.equal(res.result.ok, true);
    const body = res.result.body as { aborted: boolean; sleptMs: number };
    assert.equal(body.aborted, true, 'handler should observe the abort');
    assert.ok(body.sleptMs < 1_000, `handler should unwind early, slept ${body.sleptMs}ms`);
  });

  it('hard-kills a handler that ignores the deadline', async () => {
    const blocker = path.join(REPO_ROOT, 'test/fixtures/blocker.mjs');
    await scheduler.deploy({ name: 'blocker', entrypoint: blocker, timeoutMs: 200 });
    await assert.rejects(
      () => scheduler.invoke('blocker', { ms: 10_000 }),
      (err: Error & { code?: string }) => err.code === 'TIMEOUT',
    );

    // The killed sandbox must not poison the pool for the next caller.
    const ok = await scheduler.invoke('hello', { name: 'after-kill' });
    assert.equal(ok.result.ok, true);
  });

  it('surfaces handler errors without killing the runtime', async () => {
    const thrower = path.join(REPO_ROOT, 'test/fixtures/thrower.mjs');
    await scheduler.deploy({ name: 'thrower', entrypoint: thrower });
    const res = await scheduler.invoke('thrower', {});
    assert.equal(res.result.ok, false);
    assert.match(res.result.error!.message, /deliberate/);

    // The runtime must still serve the next request.
    const ok = await scheduler.invoke('hello', { name: 'still-here' });
    assert.equal(ok.result.ok, true);
  });

  it('respects maxConcurrency under load', async () => {
    await scheduler.deploy({
      name: 'limited',
      entrypoint: SLEEPY,
      maxConcurrency: 2,
      timeoutMs: 10_000,
    });

    const results = await Promise.all(
      Array.from({ length: 6 }, () => scheduler.invoke('limited', { ms: 60 })),
    );
    const distinctSandboxes = new Set(results.map((r) => r.sandboxId));
    assert.ok(
      distinctSandboxes.size <= 2,
      `expected at most 2 sandboxes, saw ${distinctSandboxes.size}`,
    );
    // Requests beyond the ceiling must have queued, not been dropped.
    assert.equal(results.length, 6);
  });

  it('drains sandboxes running a superseded version', async () => {
    await scheduler.deploy({ name: 'versioned', entrypoint: HELLO, minWarm: 1 });
    const before = await scheduler.invoke('versioned', {});

    await scheduler.deploy({ name: 'versioned', entrypoint: HELLO, minWarm: 1 });
    const afterDeploy = await scheduler.invoke('versioned', {});

    assert.notEqual(
      before.sandboxId,
      afterDeploy.sandboxId,
      'a redeploy must not serve from a sandbox running the old version',
    );
  });

  it('counts warm hits and cold starts separately', async () => {
    await scheduler.deploy({ name: 'counted', entrypoint: HELLO, minWarm: 0, maxConcurrency: 1 });
    const coldBefore = scheduler.metrics.counters.get('cold_starts_total');
    const warmBefore = scheduler.metrics.counters.get('warm_hits_total');

    await scheduler.invoke('counted', {});
    await scheduler.invoke('counted', {});
    await scheduler.invoke('counted', {});

    assert.equal(scheduler.metrics.counters.get('cold_starts_total') - coldBefore, 1);
    assert.equal(scheduler.metrics.counters.get('warm_hits_total') - warmBefore, 2);
  });

  it('exposes Prometheus-formatted metrics', () => {
    const text = scheduler.metrics.toPrometheus();
    assert.match(text, /ignis_invocations_total \d+/);
    assert.match(text, /ignis_cold_start_ms\{quantile="0.99"\}/);
  });

  it('404s on an unknown function', async () => {
    await assert.rejects(
      () => scheduler.invoke('does-not-exist', {}),
      (err: Error & { code?: string }) => err.code === 'NOT_FOUND',
    );
  });
});
