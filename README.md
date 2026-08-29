# ignis

[![ci](https://github.com/furyengi/ignis/actions/workflows/ci.yml/badge.svg)](https://github.com/furyengi/ignis/actions/workflows/ci.yml)

A self-hosted serverless runtime. Deploy a function, invoke it over HTTP, and get told exactly where every millisecond went.

Most "build your own Lambda" projects stop at "it runs the function." The interesting part is what happens on request #1 versus request #2 — so this one measures the difference and publishes the numbers.

```
  cold  --  every invocation boots a fresh sandbox
  metric             min      p50      p90      p99      max       n
  e2e latency       81.1     93.8    108.7    122.4    124.6     100
  cold start        79.4     93.0    107.7    121.7    122.5     100

  warm  --  single prewarmed sandbox, sequential
  metric             min      p50      p90      p99      max       n
  e2e latency       0.22     0.37     0.47     0.75     1.19     300
  throughput   2412 req/s

  warmpool  --  prewarmed pool, 16 concurrent
  metric             min      p50      p90      p99      max       n
  e2e latency       0.81     1.92     2.63     3.33     3.48     300
  throughput   7781 req/s

  burst  --  16 concurrent requests, empty pool
  metric             min      p50      p90      p99      max       n
  e2e latency      215.8    250.6    334.0    352.7    352.7      16
  cold start       214.8    249.2    332.7    351.2    351.2      16

  warm pool removes 93.0ms of p50 latency (248x)
```

<sub>`process` backend, Node v24.19.0, Intel i7-8650U, 8 cores. Reproduce with `npm run bench`. Raw JSON in `bench-results/latest.json`.</sub>

Read the burst row rather than the cold row if you want the honest number: 16 simultaneous cold starts cost **250ms at p50**, not 93ms, because they contend for the same cores. A cold-start figure measured one-at-a-time is a best case that no real traffic pattern produces.

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

That first invocation is already warm because `--min-warm 2` made prewarming part of the deploy. Drop the flag and the same command reports `COLD total=94ms`.

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

### Snapshots: capture once, restore many

Booting a kernel, starting Node and importing the handler's module graph
produces a byte-identical VM every time. A snapshot pays that once per function
version and turns every later cold start into a memory restore.

The *when* is separate from the *how*. `snapshots.ts` decides policy — capture
at deploy, restore on cold start, evict superseded versions — and the backend
only supplies `capture` / `restore` / `evict` / `list`. Firecracker is the sole
backend that can freeze a machine, but none of the policy is Firecracker-shaped,
so it is tested against a fake store on any laptop. That matters, because the
policy is where the bugs are, not the ioctls.

Restore is wired in as a decorator over the backend, not a branch inside the
pool. The pool's job is capacity, not provenance: it asks for a sandbox and gets
one, and the only visible difference is that `coldStart.restored` is true and
the number is small.

What the orchestration guarantees, each with a test:

- **Capture happens before prewarm.** Otherwise the deploy pays full boot price
  for exactly the sandboxes the snapshot exists to make cheap.
- **One capture per version**, even under concurrent deploys. Without
  single-flight, two callers each boot a VM and race to write the same files.
- **Capture failure is a performance regression, not a failed deploy.** Out of
  disk means cold boots, not a broken function.
- **A corrupt image cannot poison the hot path.** Restore failure falls back to
  booting and marks the version bad for 30s, so one bad file does not add a
  doomed restore to every cold start.
- **Removing a function deletes its images.** A stale memory image is 128MiB+
  of disk nobody can reach.

Snapshots are opt-in (`IGNIS_FC_SNAPSHOT=1`). Capturing costs a full boot and a
memory image per version — not something to start doing because the backend
merely could.

### The registry is durable, and still not on the hot path

By default deployments live in memory and a restart forgets them. Point ignis at
Postgres and they survive:

```bash
IGNIS_DATABASE_URL=postgres://ignis:ignis@localhost:5432/ignis npm start
```

Restart the control plane and the deployments come back — versions intact, warm
pools refilled during startup, before the listener accepts traffic.

The registry is a **write-through cache**, not a database wrapper. `get()` runs
on every invocation, so it stays synchronous and in-memory; the store is touched
only on deploy, delete and startup. Writes go to the store first and update the
cache once durable — the reverse would let a failed write leave the cache
serving a version that does not exist.

**Version allocation belongs to the database.** The upsert increments in the
same statement:

```sql
ON CONFLICT (name) DO UPDATE SET
  version = ignis_functions.version + 1
```

Read-modify-write from the application would let two concurrent deploys both
read v3 and both write v4 — two different code versions claiming one number,
while the pool uses exactly that number to decide which sandboxes are stale. A
test fires twelve concurrent deploys and asserts the versions come back as
`1..12` with no duplicates.

Migrations run under a `pg_advisory_lock`, because concurrent
`CREATE TABLE IF NOT EXISTS` from several nodes deadlocks in Postgres rather
than politely no-opping.

## Backends

| | `process` | `firecracker` |
|---|---|---|
| Isolation | separate process, heap ceiling, scrubbed env | hardware-virtualised microVM |
| Shared kernel | yes | no |
| Multi-tenant safe | **no** | yes |
| Runs on | anything with Node | Linux + `/dev/kvm` |
| Cold start | ~93ms | ~125ms boot, ~10ms from snapshot |

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
| `GET /stats` | pools, versions, store, snapshots, latency percentiles |
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

28 tests. The integration suite drives the real process backend with no mocks —
the properties worth testing (warm reuse, concurrency ceilings, timeout
enforcement, version draining) only exist once sandboxes are actually booting.
The snapshot suite drives a fake store, because the policy under test is
backend-agnostic and running it should not require KVM. The Postgres suite is
the opposite: it needs a real server, because what it checks (atomic version
allocation, advisory locks, jsonb round-trips) are properties of Postgres, not
of my code. CI supplies one as a service container; locally it skips unless
`IGNIS_TEST_DATABASE_URL` is set.

## Known limits

- One control plane, no clustering. Two nodes sharing a database each allocate
  versions correctly, but neither is told when the other deploys, so their read
  caches drift until restart. LISTEN/NOTIFY on the functions table is the fix
  and is not implemented.
- Snapshots are host-local, so a second node starts cold even for a function
  another node has already captured.
- The Firecracker backend needs a prebuilt rootfs; there is no image builder in
  this repo beyond the documented steps.
- Node has no `AF_VSOCK` binding, so the guest reaches the host through a
  `socat` bridge inside the rootfs rather than opening the vsock directly.
- No auth on the control plane. It binds loopback by default and should stay
  there until that changes.
- The snapshot **orchestration** is tested; the snapshot **backend** is not.
  Capture and restore are written against Firecracker's documented API and
  exercised only through a fake store, because CI has no KVM. Treat the ~10ms
  restore figure as Firecracker's published number, not as something this repo
  has measured.
- Restoring many VMs from one memory image gives them all identical RNG state.
  Guests must reseed on resume; see [docs/FIRECRACKER.md](docs/FIRECRACKER.md).

## License

MIT
