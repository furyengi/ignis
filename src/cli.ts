#!/usr/bin/env node
/**
 * ignisctl -- thin client for the control plane.
 *
 * Deliberately has no logic of its own beyond argument parsing: everything it
 * does is an HTTP call any other client could make. If a capability only works
 * through the CLI, it does not really exist.
 */
import path from 'node:path';

const BASE = process.env.IGNIS_ADDR ?? 'http://127.0.0.1:8080';

const USAGE = `
ignisctl -- control plane client for ignis

  deploy <name> <entrypoint> [options]   register or update a function
      --memory <mib>          memory ceiling            (default 128)
      --timeout <ms>          per-invocation timeout    (default 5000)
      --min-warm <n>          sandboxes kept hot        (default 0)
      --max-concurrency <n>   ceiling on live sandboxes (default 32)
      --env KEY=VALUE         repeatable

  invoke <name> [json]                   run a function
  list                                   list deployed functions
  remove <name>                          drain and remove
  stats                                  pools, versions, latency percentiles

Environment:
  IGNIS_ADDR   control plane address (default ${BASE})
`;

interface Flags {
  positional: string[];
  values: Map<string, string>;
  env: Record<string, string>;
}

function parse(argv: string[]): Flags {
  const positional: string[] = [];
  const values = new Map<string, string>();
  const env: Record<string, string> = {};

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]!;
    if (!arg.startsWith('--')) {
      positional.push(arg);
      continue;
    }
    const key = arg.slice(2);
    const value = argv[++i];
    if (value === undefined) throw new Error(`flag --${key} needs a value`);
    if (key === 'env') {
      const eq = value.indexOf('=');
      if (eq === -1) throw new Error(`--env expects KEY=VALUE, got "${value}"`);
      env[value.slice(0, eq)] = value.slice(eq + 1);
    } else {
      values.set(key, value);
    }
  }
  return { positional, values, env };
}

async function api(method: string, route: string, body?: unknown): Promise<unknown> {
  let res: Response;
  try {
    res = await fetch(`${BASE}${route}`, {
      method,
      headers: body === undefined ? {} : { 'Content-Type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch (err) {
    throw new Error(`cannot reach ignis at ${BASE} -- is it running? (${(err as Error).message})`);
  }

  const text = await res.text();
  const parsed: unknown = text ? JSON.parse(text) : null;
  if (!res.ok) {
    const detail = parsed as { error?: string; code?: string };
    throw new Error(`${detail?.code ?? res.status}: ${detail?.error ?? text}`);
  }
  return parsed;
}

function num(flags: Flags, key: string): number | undefined {
  const raw = flags.values.get(key);
  if (raw === undefined) return undefined;
  const n = Number(raw);
  if (!Number.isFinite(n)) throw new Error(`--${key} must be a number, got "${raw}"`);
  return n;
}

async function main(): Promise<void> {
  const [command, ...rest] = process.argv.slice(2);
  if (!command || command === 'help' || command === '--help') {
    console.log(USAGE);
    return;
  }

  const flags = parse(rest);

  switch (command) {
    case 'deploy': {
      const [name, entrypoint] = flags.positional;
      if (!name || !entrypoint) throw new Error('usage: ignisctl deploy <name> <entrypoint>');
      const spec = await api('POST', '/functions', {
        name,
        // Resolve client-side so the server never has to guess a relative path
        // against its own working directory.
        entrypoint: path.resolve(entrypoint),
        memoryMib: num(flags, 'memory'),
        timeoutMs: num(flags, 'timeout'),
        minWarm: num(flags, 'min-warm'),
        maxConcurrency: num(flags, 'max-concurrency'),
        ...(Object.keys(flags.env).length ? { env: flags.env } : {}),
      });
      const s = spec as { name: string; version: number; minWarm: number };
      console.log(`deployed ${s.name} v${s.version} (minWarm=${s.minWarm})`);
      break;
    }

    case 'invoke': {
      const [name, payloadRaw] = flags.positional;
      if (!name) throw new Error('usage: ignisctl invoke <name> [json]');
      let payload: unknown = {};
      if (payloadRaw) {
        try {
          payload = JSON.parse(payloadRaw);
        } catch {
          throw new Error(`payload is not valid JSON: ${payloadRaw}`);
        }
      }
      const res = (await api('POST', `/invoke/${name}`, payload)) as {
        result: { ok: boolean; body?: unknown; error?: { message: string } };
        timing: { totalMs: number; coldStartMs: number; handlerMs: number; warm: boolean };
        sandboxId: string;
      };

      if (res.result.ok) {
        console.log(JSON.stringify(res.result.body, null, 2));
      } else {
        console.error(`handler error: ${res.result.error?.message}`);
      }
      const t = res.timing;
      console.error(
        `\n  ${t.warm ? 'warm' : 'COLD'}  total=${t.totalMs.toFixed(2)}ms` +
          `  cold=${t.coldStartMs.toFixed(2)}ms` +
          `  handler=${t.handlerMs.toFixed(2)}ms` +
          `  sandbox=${res.sandboxId}`,
      );
      if (!res.result.ok) process.exitCode = 1;
      break;
    }

    case 'list': {
      const fns = (await api('GET', '/functions')) as Array<{
        name: string;
        version: number;
        minWarm: number;
        maxConcurrency: number;
        memoryMib: number;
        timeoutMs: number;
      }>;
      if (fns.length === 0) {
        console.log('no functions deployed');
        break;
      }
      console.log('NAME'.padEnd(24) + 'VER'.padStart(5) + 'WARM'.padStart(6) + 'MAXC'.padStart(6) + 'MEM'.padStart(7) + 'TIMEOUT'.padStart(10));
      for (const f of fns) {
        console.log(
          f.name.padEnd(24) +
            String(f.version).padStart(5) +
            String(f.minWarm).padStart(6) +
            String(f.maxConcurrency).padStart(6) +
            `${f.memoryMib}Mi`.padStart(7) +
            `${f.timeoutMs}ms`.padStart(10),
        );
      }
      break;
    }

    case 'remove': {
      const [name] = flags.positional;
      if (!name) throw new Error('usage: ignisctl remove <name>');
      await api('DELETE', `/functions/${name}`);
      console.log(`removed ${name}`);
      break;
    }

    case 'stats': {
      console.log(JSON.stringify(await api('GET', '/stats'), null, 2));
      break;
    }

    default:
      throw new Error(`unknown command "${command}" -- run \`ignisctl help\``);
  }
}

try {
  await main();
} catch (err) {
  console.error(`error: ${(err as Error).message}`);
  process.exitCode = 1;
}
