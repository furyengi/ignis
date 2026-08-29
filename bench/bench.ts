/**
 * Cold-start and throughput benchmark.
 *
 * Runs the scheduler in-process rather than over HTTP, so the numbers are the
 * runtime's own cost with no loopback socket or JSON-over-TCP in the way.
 *
 * Four scenarios:
 *   cold     - every invocation pays for a boot (minWarm=0, maxConcurrency=1,
 *              pool drained between calls)
 *   warm     - single hot sandbox, sequential
 *   warmpool - prewarmed pool under concurrency, the realistic steady state
 *   burst    - N concurrent requests against a cold pool, the scale-out case
 *
 * Usage: node dist/bench/bench.js [--iterations 200] [--concurrency 16] [--json out.json]
 */
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { fileURLToPath } from 'node:url';
import { resolveBackend } from '../src/backends/registry.js';
import { Histogram, type Percentiles } from '../src/metrics.js';
import { POOL_DEFAULTS } from '../src/pool.js';
import { Scheduler } from '../src/scheduler.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, '../..');
const HELLO = path.join(REPO_ROOT, 'examples/hello/index.mjs');

interface Args {
  iterations: number;
  concurrency: number;
  json: string | null;
}

function parseArgs(argv: string[]): Args {
  const get = (flag: string): string | undefined => {
    const i = argv.indexOf(flag);
    return i === -1 ? undefined : argv[i + 1];
  };
  return {
    iterations: Number(get('--iterations') ?? 200),
    concurrency: Number(get('--concurrency') ?? 16),
    json: get('--json') ?? null,
  };
}

interface Scenario {
  name: string;
  description: string;
  latency: Percentiles;
  coldStart?: Percentiles;
  throughputRps?: number;
  errors: number;
}

function fmt(ms: number): string {
  return ms < 10 ? ms.toFixed(2) : ms.toFixed(1);
}

function row(label: string, p: Percentiles): string {
  return (
    `  ${label.padEnd(14)}` +
    `${fmt(p.min).padStart(8)}` +
    `${fmt(p.p50).padStart(9)}` +
    `${fmt(p.p90).padStart(9)}` +
    `${fmt(p.p99).padStart(9)}` +
    `${fmt(p.max).padStart(9)}` +
    `${String(p.count).padStart(8)}`
  );
}

const HEADER =
  `  ${'metric'.padEnd(14)}${'min'.padStart(8)}${'p50'.padStart(9)}` +
  `${'p90'.padStart(9)}${'p99'.padStart(9)}${'max'.padStart(9)}${'n'.padStart(8)}`;

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  // The benchmark's own output is the product here; runtime logs would drown it.
  process.env.IGNIS_LOG = process.env.IGNIS_LOG ?? 'off';

  const backend = await resolveBackend(
    (process.env.IGNIS_BACKEND as 'process' | 'firecracker' | 'auto') ?? 'auto',
  );

  console.log('');
  console.log('  ignis benchmark');
  console.log(`  backend=${backend.name}  node=${process.version}  ${os.cpus()[0]?.model ?? 'cpu'}`);
  console.log(`  cores=${os.cpus().length}  iterations=${args.iterations}  concurrency=${args.concurrency}`);
  console.log('');

  const scenarios: Scenario[] = [];

  // ---- cold: pay for a boot every single time -----------------------------
  {
    const scheduler = new Scheduler(backend, { ...POOL_DEFAULTS, maxIdle: 0, idleTtlMs: 0 });
    await scheduler.start();
    await scheduler.deploy({
      name: 'bench-cold',
      entrypoint: HELLO,
      minWarm: 0,
      maxConcurrency: 1,
    });

    const latency = new Histogram();
    const cold = new Histogram();
    let errors = 0;
    // maxIdle=0 means every release destroys the sandbox, so the next
    // invocation is guaranteed cold without any special-casing here.
    const n = Math.min(args.iterations, 100);
    for (let i = 0; i < n; i++) {
      try {
        const res = await scheduler.invoke('bench-cold', { name: 'bench' });
        latency.record(res.timing.totalMs);
        cold.record(res.timing.coldStartMs);
      } catch {
        errors++;
      }
    }
    scenarios.push({
      name: 'cold',
      description: 'every invocation boots a fresh sandbox',
      latency: latency.snapshot(),
      coldStart: cold.snapshot(),
      errors,
    });
    await scheduler.shutdown();
  }

  // ---- warm: one hot sandbox, sequential ----------------------------------
  {
    const scheduler = new Scheduler(backend, POOL_DEFAULTS);
    await scheduler.start();
    await scheduler.deploy({
      name: 'bench-warm',
      entrypoint: HELLO,
      minWarm: 1,
      maxConcurrency: 1,
    });

    // Discard the first few: V8 has not tiered up the handler yet.
    for (let i = 0; i < 20; i++) await scheduler.invoke('bench-warm', { name: 'warmup' });

    const latency = new Histogram();
    let errors = 0;
    const started = performance.now();
    for (let i = 0; i < args.iterations; i++) {
      try {
        const res = await scheduler.invoke('bench-warm', { name: 'bench' });
        latency.record(res.timing.totalMs);
      } catch {
        errors++;
      }
    }
    const elapsedS = (performance.now() - started) / 1000;
    scenarios.push({
      name: 'warm',
      description: 'single prewarmed sandbox, sequential',
      latency: latency.snapshot(),
      throughputRps: args.iterations / elapsedS,
      errors,
    });
    await scheduler.shutdown();
  }

  // ---- warmpool: prewarmed, concurrent ------------------------------------
  {
    const scheduler = new Scheduler(backend, POOL_DEFAULTS);
    await scheduler.start();
    await scheduler.deploy({
      name: 'bench-pool',
      entrypoint: HELLO,
      minWarm: args.concurrency,
      maxConcurrency: args.concurrency,
    });

    await Promise.all(
      Array.from({ length: args.concurrency * 2 }, () =>
        scheduler.invoke('bench-pool', { name: 'warmup' }),
      ),
    );

    const latency = new Histogram();
    let errors = 0;
    const started = performance.now();
    let issued = 0;

    // Keep exactly `concurrency` requests in flight until the budget is spent.
    const worker = async () => {
      while (issued < args.iterations) {
        issued++;
        try {
          const res = await scheduler.invoke('bench-pool', { name: 'bench' });
          latency.record(res.timing.totalMs);
        } catch {
          errors++;
        }
      }
    };
    await Promise.all(Array.from({ length: args.concurrency }, worker));
    const elapsedS = (performance.now() - started) / 1000;

    scenarios.push({
      name: 'warmpool',
      description: `prewarmed pool, ${args.concurrency} concurrent`,
      latency: latency.snapshot(),
      throughputRps: args.iterations / elapsedS,
      errors,
    });
    await scheduler.shutdown();
  }

  // ---- burst: cold pool hit by N at once ----------------------------------
  {
    const scheduler = new Scheduler(backend, POOL_DEFAULTS);
    await scheduler.start();
    await scheduler.deploy({
      name: 'bench-burst',
      entrypoint: HELLO,
      minWarm: 0,
      maxConcurrency: args.concurrency,
    });

    const latency = new Histogram();
    const cold = new Histogram();
    let errors = 0;
    const started = performance.now();
    const results = await Promise.allSettled(
      Array.from({ length: args.concurrency }, () => scheduler.invoke('bench-burst', { name: 'burst' })),
    );
    const elapsedS = (performance.now() - started) / 1000;

    for (const r of results) {
      if (r.status === 'fulfilled') {
        latency.record(r.value.timing.totalMs);
        if (!r.value.timing.warm) cold.record(r.value.timing.coldStartMs);
      } else {
        errors++;
      }
    }
    scenarios.push({
      name: 'burst',
      description: `${args.concurrency} concurrent requests, empty pool`,
      latency: latency.snapshot(),
      coldStart: cold.snapshot(),
      throughputRps: args.concurrency / elapsedS,
      errors,
    });
    await scheduler.shutdown();
  }

  // ---- report -------------------------------------------------------------
  for (const s of scenarios) {
    console.log(`  ${s.name}  --  ${s.description}`);
    console.log(HEADER);
    console.log(row('e2e latency', s.latency));
    if (s.coldStart && s.coldStart.count > 0) console.log(row('cold start', s.coldStart));
    if (s.throughputRps) console.log(`  throughput   ${s.throughputRps.toFixed(0)} req/s`);
    if (s.errors) console.log(`  errors       ${s.errors}`);
    console.log('');
  }

  const coldP50 = scenarios.find((s) => s.name === 'cold')?.coldStart?.p50 ?? 0;
  const warmP50 = scenarios.find((s) => s.name === 'warm')?.latency.p50 ?? 0;
  if (coldP50 && warmP50) {
    console.log(`  warm pool removes ${fmt(coldP50)}ms of p50 latency (${(coldP50 / warmP50).toFixed(0)}x)`);
    console.log('');
  }

  if (args.json) {
    const out = {
      generatedAt: new Date().toISOString(),
      backend: backend.name,
      node: process.version,
      cpu: os.cpus()[0]?.model ?? 'unknown',
      cores: os.cpus().length,
      args,
      scenarios,
    };
    await fs.mkdir(path.dirname(path.resolve(args.json)), { recursive: true });
    await fs.writeFile(path.resolve(args.json), JSON.stringify(out, null, 2));
    console.log(`  wrote ${args.json}`);
    console.log('');
  }
}

await main();
