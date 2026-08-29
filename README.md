# ignis

[![ci](https://github.com/furyengi/ignis/actions/workflows/ci.yml/badge.svg)](https://github.com/furyengi/ignis/actions/workflows/ci.yml)

A self-hosted serverless runtime. Deploy a function, invoke it over HTTP, and get told exactly where every millisecond went.

Most "build your own Lambda" projects stop at "it runs the function." The interesting part is what happens on request #1 versus request #2 — so this one measures the difference and publishes the numbers.

```
  cold  --  every invocation boots a fresh sandbox
  metric             min      p50      p90      p99      max       n
  e2e latency       96.5    127.1    143.3    159.9    161.2     100
  cold start        95.6    125.5    141.7    158.7    159.7     100

  warm  --  single prewarmed sandbox, sequential
  metric             min      p50      p90      p99      max       n
  e2e latency       0.35     0.43     0.51     0.97     1.11     300
  throughput   2036 req/s

  warmpool  --  prewarmed pool, 16 concurrent
  metric             min      p50      p90      p99      max       n
  e2e latency       0.93     2.14     2.63     4.91     5.49     300
  throughput   6975 req/s

  burst  --  16 concurrent requests, empty pool
  metric             min      p50      p90      p99      max       n
  e2e latency      229.6    284.4    330.3    341.4    341.4      16
  cold start       227.8    282.3    328.9    340.1    340.1      16

  warm pool removes 125.5ms of p50 latency (289x)
```

<sub>`process` backend, Node v24.19.0, 16 cores. Reproduce with `npm run bench`. Raw JSON in `bench-results/`.</sub>

Read the burst row rather than the cold row if you want the honest number: 16 simultaneous cold starts cost **284ms at p50**, not 125ms, because they contend for the same cores. A cold-start figure measured one-at-a-time is a best case that no real traffic pattern produces.

---

## Quick start

Needs Node 22 or newer. CI covers 22 and 24 on Linux, macOS and Windows.

```bash
npm install && npm run build
npm start
```

```bash
node dist/src/cli.js deploy hello examples/hello/index.mjs --min-warm 2
```

```bash
node dist/src/cli.js invoke hello '{"name":"world"}'
```

```
{
  "message": "hello, world",
  "requestId": "2d2b4311-9386-4356-9645-17337d352d71",
  "pid": 7056
}

  warm  total=2.03ms  cold=0.00ms  handler=0.11ms  sandbox=sb-0ef23d4c
```

That first invocation is already warm because `--min-warm 2` made prewarming part of the deploy. Drop the flag and the same command reports `COLD total=127ms`.

A function is an ES module exporting `handler`:

```js
export async function handler(payload, ctx) {
  return { message: `hello, ${payload.name}`, requestId: ctx.requestId };
}
```

`ctx` carries `requestId`, `remainingMs()`, and an `AbortSignal` that fires at the deadline.

## How it works

```
  POST /invoke/:name
         |
    Scheduler ................ owns the timing breakdown
         |                     queue / cold start / handler, recorded separately
    FunctionPool ............. LIFO warm pool, reservations, version pinning
         |
    SandboxBackend ........... process | firecracker
         |
    guest shim ............... loads the handler, times it, serves one at a time
```

The scheduler never learns which backend it is talking to. Both speak the same
message protocol (`load` / `invoke` / `shutdown`) over different transports —
Node IPC for processes, vsock for microVMs. That seam is why the pool and the
cold-start accounting could be built and measured on a laptop and still be the
code that runs on a Firecracker host.

### Three decisions worth explaining

**LIFO, not FIFO.** Idle sandboxes are reused most-recently-used first. Under
partial load that keeps a small subset genuinely hot and lets the rest age out.
A FIFO pool round-robins across every sandbox and keeps all of them lukewarm —
worse cache locality, and the reaper can never retire anything.

**Reservations before creation.** Capacity is claimed synchronously, then the
sandbox boots. Without that, N concurrent requests all observe `total < max`,
all decide to create, and the concurrency ceiling is silently exceeded by N-1.

**Version-pinned reuse.** Every deploy bumps a version. Sandboxes running the
old version are drained from the idle pool immediately and retired on release
if busy. This is the only reason a redeploy cannot serve stale code.

### Timeouts have a grace window

The guest's `AbortSignal` fires at the deadline; the host kills the sandbox
`TIMEOUT_GRACE_MS` later. Setting both to the same value looks tidier and is a
race — a cooperative handler that unwinds exactly at the deadline sometimes
returns its result and sometimes gets killed mid-write. The gap makes the
contract explicit: *abort first, kill only if that did not work.*

Both paths are tested — a cooperative handler returns `aborted: true`, an
uncooperative one spinning in a synchronous loop gets `TIMEOUT`.

## Backends

| | `process` | `firecracker` |
|---|---|---|
| Isolation | separate process, heap ceiling, scrubbed env | hardware-virtualised microVM |
| Shared kernel | yes | no |
| Multi-tenant safe | **no** | yes |
| Runs on | anything with Node | Linux + `/dev/kvm` |
| Cold start | ~125ms | ~125ms boot, ~10ms from snapshot |

`IGNIS_BACKEND=auto` (the default) picks `firecracker` on Linux and falls back
to `process` with a warning if KVM or the images are missing — so a laptop gets
a working runtime instead of a stack trace, and is told what it lost.

**The `process` backend is not a security boundary.** Same kernel, same user, no
seccomp. It exists so the interesting engineering is testable anywhere. Do not
run untrusted code on it.

See [docs/FIRECRACKER.md](docs/FIRECRACKER.md) for building the kernel and
rootfs, and for the snapshot-restore path that takes cold starts to ~10ms by
resuming a VM that is already past handler load.

## API

| | |
|---|---|
| `POST /functions` | deploy; body is a function spec |
| `GET /functions` | list |
| `DELETE /functions/:name` | drain and remove |
| `POST /invoke/:name` | invoke; body is the payload |
| `GET /stats` | pools, versions, latency percentiles |
| `GET /metrics` | Prometheus text exposition |
| `GET /healthz` | liveness |

Every invocation response carries its timing split, and the headers
`X-Ignis-Cold-Start-Ms`, `X-Ignis-Warm` and `X-Ignis-Sandbox` expose it to a
plain `curl`.

## Benchmarking

```bash
npm run bench -- --iterations 500 --concurrency 32 --json bench-results/run.json
```

Scenarios: `cold` (pool disabled, every call boots), `warm` (one hot sandbox,
sequential), `warmpool` (prewarmed, concurrent — the realistic steady state),
`burst` (N at once against an empty pool — the scale-out case).

The benchmark drives the scheduler in-process, so the numbers are the runtime's
own cost with no loopback socket in the way.

## Tests

```bash
npm test
```

16 integration tests against the real process backend, no mocks. The properties
worth testing here — warm reuse, concurrency ceilings, timeout enforcement,
version draining — only exist once sandboxes are actually booting.

## Known limits

- The registry is in-memory: restarting the control plane forgets every
  deployment. It sits behind an interface for exactly this reason, but there is
  no etcd or Postgres implementation yet.
- One control plane, no clustering. Scheduling is per-node.
- The Firecracker backend needs a prebuilt rootfs; there is no image builder in
  this repo beyond the documented steps.
- Node has no `AF_VSOCK` binding, so the guest reaches the host through a
  `socat` bridge inside the rootfs rather than opening the vsock directly.
- No auth on the control plane. It binds loopback by default and should stay
  there until that changes.

## License

MIT
