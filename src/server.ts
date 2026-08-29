/**
 * HTTP control plane and data plane.
 *
 *   POST   /functions            deploy (body: FunctionInput)
 *   GET    /functions            list
 *   DELETE /functions/:name      drain and remove
 *   POST   /invoke/:name         run (body: arbitrary JSON payload)
 *   GET    /stats                pools, versions, latency percentiles
 *   GET    /metrics              Prometheus text exposition
 *   GET    /healthz              liveness
 *
 * Data plane and control plane share a port for demo convenience; in a real
 * deployment `/invoke` would be the only route on the public listener.
 */
import http from 'node:http';
import { resolveBackend, type BackendName } from './backends/registry.js';
import { log } from './log.js';
import { POOL_DEFAULTS } from './pool.js';
import { Scheduler } from './scheduler.js';
import { MemoryStore, type FunctionStore } from './registry/store.js';
import { PostgresStore } from './registry/postgres.js';
import { IgnisError } from './types.js';

const MAX_BODY_BYTES = 1024 * 1024;

function readJsonBody(req: http.IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    req.on('data', (c: Buffer) => {
      size += c.length;
      if (size > MAX_BODY_BYTES) {
        reject(new IgnisError('request body too large', 'PAYLOAD_TOO_LARGE', 413));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString('utf8').trim();
      if (!raw) return resolve(undefined);
      try {
        resolve(JSON.parse(raw));
      } catch {
        reject(new IgnisError('body is not valid JSON', 'BAD_REQUEST', 400));
      }
    });
    req.on('error', reject);
  });
}

function send(res: http.ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body, null, 2);
  res.writeHead(status, {
    'Content-Type': 'application/json',
    'Content-Length': Buffer.byteLength(payload),
  });
  res.end(payload);
}

export interface ServerOptions {
  port: number;
  backend: BackendName;
  /**
   * Postgres connection string. Omitted means the in-memory store, which
   * forgets every deployment on restart.
   */
  databaseUrl?: string;
}

/**
 * Pick a store from configuration.
 *
 * Defaulting to memory keeps `npm start` working with no database, and makes
 * the durable option an explicit choice rather than a silent dependency.
 */
export function resolveStore(databaseUrl: string | undefined): FunctionStore {
  if (!databaseUrl) return new MemoryStore();
  return new PostgresStore({ connectionString: databaseUrl });
}

export async function createServer(opts: ServerOptions) {
  const backend = await resolveBackend(opts.backend);
  const store = resolveStore(opts.databaseUrl);
  const scheduler = new Scheduler(backend, POOL_DEFAULTS, undefined, store);
  await scheduler.start();
  // Bring persisted deployments back before accepting traffic, so a restart
  // does not serve 404s for functions that are still deployed.
  await scheduler.hydrate();
  // Then follow other nodes. Started after hydrate so the first thing the
  // listener can do is apply a change on top of a complete cache.
  await scheduler.startWatching();

  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url ?? '/', 'http://localhost');
    const segments = url.pathname.split('/').filter(Boolean);
    const method = req.method ?? 'GET';

    try {
      // --- data plane ---
      if (method === 'POST' && segments[0] === 'invoke' && segments[1]) {
        const payload = await readJsonBody(req);
        const response = await scheduler.invoke(segments[1], payload);
        // Surface the timing split on every response; this is what makes the
        // cold-start story observable from a plain curl.
        res.setHeader('X-Ignis-Cold-Start-Ms', response.timing.coldStartMs.toFixed(2));
        res.setHeader('X-Ignis-Warm', String(response.timing.warm));
        res.setHeader('X-Ignis-Sandbox', response.sandboxId);
        return send(res, response.result.ok ? 200 : 500, response);
      }

      // --- control plane ---
      if (method === 'POST' && segments[0] === 'functions' && !segments[1]) {
        const body = (await readJsonBody(req)) as Record<string, unknown>;
        if (!body || typeof body !== 'object') {
          throw new IgnisError('expected a function spec object', 'BAD_REQUEST', 400);
        }
        const spec = await scheduler.deploy(body as never);
        return send(res, 201, spec);
      }

      if (method === 'GET' && segments[0] === 'functions' && !segments[1]) {
        return send(res, 200, scheduler.registry.list());
      }

      if (method === 'DELETE' && segments[0] === 'functions' && segments[1]) {
        await scheduler.remove(segments[1]);
        return send(res, 200, { removed: segments[1] });
      }

      if (method === 'GET' && segments[0] === 'stats') {
        return send(res, 200, await scheduler.stats());
      }

      if (method === 'GET' && segments[0] === 'metrics') {
        const text = scheduler.metrics.toPrometheus();
        res.writeHead(200, {
          'Content-Type': 'text/plain; version=0.0.4',
          'Content-Length': Buffer.byteLength(text),
        });
        return res.end(text);
      }

      if (method === 'GET' && segments[0] === 'healthz') {
        return send(res, 200, { ok: true, backend: backend.name });
      }

      return send(res, 404, { error: 'no such route', code: 'NOT_FOUND' });
    } catch (err) {
      if (err instanceof IgnisError) {
        return send(res, err.status, { error: err.message, code: err.code });
      }
      const e = err as Error;
      log.error('unhandled request error', { path: url.pathname, err: e.message, stack: e.stack });
      return send(res, 500, { error: e.message, code: 'INTERNAL' });
    }
  });

  return { server, scheduler, backend };
}

/** Entry point when run directly: `node dist/src/server.js`. */
const isMain = process.argv[1]?.replace(/\\/g, '/').endsWith('/dist/src/server.js');
if (isMain) {
  const port = Number(process.env.PORT ?? 8080);
  const backendName = (process.env.IGNIS_BACKEND as BackendName) ?? 'auto';

  const { server, scheduler, backend } = await createServer({
    port,
    backend: backendName,
    databaseUrl: process.env.IGNIS_DATABASE_URL,
  });

  server.listen(port, () => {
    log.info('ignis listening', { port, backend: backend.name, store: scheduler.registry.backend });
  });

  const shutdown = async (signal: string) => {
    log.info('shutting down', { signal });
    server.close();
    await scheduler.shutdown();
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}
