/**
 * Firecracker microVM backend.
 *
 * Requires Linux with /dev/kvm. On EC2 that means a `.metal` instance or one
 * of the nested-virtualisation-capable types; locally it means a Linux host or
 * a VM with nested virt enabled. `preflight()` refuses to pretend otherwise.
 *
 * Boot sequence, all over the Firecracker HTTP API on a Unix socket:
 *
 *   1. spawn `firecracker --api-sock <sock>`         (VMM up, no guest yet)
 *   2. PUT /boot-source     kernel + boot args
 *   3. PUT /drives/rootfs   read-only rootfs + per-VM overlay
 *   4. PUT /machine-config  vCPU and memory
 *   5. PUT /vsock           guest<->host control channel
 *   6. PUT /actions         InstanceStart
 *   7. accept the guest shim's vsock connection, then `load`
 *
 * With `snapshot.enabled` the backend also exposes a SnapshotStore. Restore
 * replaces steps 2-7 with a single `PUT /snapshot/load` of a VM already past
 * handler load: no kernel boot, no Node startup, no module graph. That is the
 * difference between a ~125ms cold start and a ~10ms one.
 *
 * Restore is a separate entry point rather than a branch inside `create`. When
 * the two were fused, the restore path fell through into `loadHandler` and
 * re-imported the module the snapshot had been taken to preserve -- paying the
 * exact cost it existed to remove. Deciding *when* to restore belongs to
 * `snapshots.ts`; this file only knows how.
 */
import { spawn, type ChildProcess } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import fs from 'node:fs/promises';
import http from 'node:http';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { log } from '../log.js';
import {
  errors,
  type ColdStartBreakdown,
  type FunctionSpec,
  type InvokeRequest,
  type InvokeResult,
  TIMEOUT_GRACE_MS,
} from '../types.js';
import type {
  Sandbox,
  SandboxBackend,
  SnapshotInfo,
  SnapshotStore,
} from './backend.js';
import {
  NdjsonDecoder,
  encodeNdjson,
  type GuestMessage,
  type HostMessage,
} from './protocol.js';

export interface FirecrackerConfig {
  /** Path to the `firecracker` binary. */
  binary: string;
  /** Uncompressed kernel image (vmlinux, not bzImage). */
  kernelImage: string;
  /** ext4 rootfs containing Node and the compiled guest shim. */
  rootfsImage: string;
  /** Kernel command line. `quiet` and `pci=off` are worth real milliseconds. */
  bootArgs: string;
  /** vCPUs per microVM. */
  vcpus: number;
  /** Directory for API sockets, vsock sockets and overlays. */
  runDir: string;
  /** vsock port the guest shim dials on the host (CID 2). */
  vsockPort: number;
  /** Restore from a post-load snapshot instead of booting from scratch. */
  snapshot: { enabled: boolean; dir: string };
}

export const FIRECRACKER_DEFAULTS: FirecrackerConfig = {
  binary: process.env.IGNIS_FC_BINARY ?? '/usr/bin/firecracker',
  kernelImage: process.env.IGNIS_FC_KERNEL ?? '/var/lib/ignis/vmlinux',
  rootfsImage: process.env.IGNIS_FC_ROOTFS ?? '/var/lib/ignis/rootfs.ext4',
  bootArgs: 'console=ttyS0 reboot=k panic=1 pci=off quiet loglevel=0 i8042.noaux i8042.nomux',
  vcpus: 1,
  runDir: process.env.IGNIS_FC_RUNDIR ?? '/run/ignis',
  vsockPort: 5252,
  snapshot: {
    enabled: process.env.IGNIS_FC_SNAPSHOT === '1',
    dir: process.env.IGNIS_FC_SNAPSHOT_DIR ?? '/var/lib/ignis/snapshots',
  },
};

/** Minimal HTTP client for the Firecracker API socket. */
async function fcApi(
  socketPath: string,
  method: 'PUT' | 'PATCH' | 'GET',
  route: string,
  body?: unknown,
): Promise<void> {
  const payload = body === undefined ? undefined : JSON.stringify(body);
  await new Promise<void>((resolve, reject) => {
    const req = http.request(
      {
        socketPath,
        path: route,
        method,
        headers: {
          'Content-Type': 'application/json',
          ...(payload ? { 'Content-Length': Buffer.byteLength(payload) } : {}),
        },
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (c: Buffer) => chunks.push(c));
        res.on('end', () => {
          const status = res.statusCode ?? 0;
          if (status >= 200 && status < 300) return resolve();
          const detail = Buffer.concat(chunks).toString('utf8');
          reject(new Error(`firecracker ${method} ${route} -> ${status}: ${detail}`));
        });
      },
    );
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

/** Poll for the API socket; the VMM creates it a few ms after exec. */
async function waitForSocket(socketPath: string, timeoutMs = 3_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      await fs.access(socketPath);
      return;
    } catch {
      if (Date.now() > deadline) throw new Error(`firecracker API socket never appeared at ${socketPath}`);
      await new Promise((r) => setTimeout(r, 2));
    }
  }
}

class FirecrackerSandbox implements Sandbox {
  readonly id: string;
  lastUsedAt = performance.now();
  alive = true;

  private pending: {
    id: string;
    resolve: (r: InvokeResult) => void;
    reject: (e: Error) => void;
    timer: NodeJS.Timeout;
  } | null = null;

  constructor(
    id: string,
    readonly functionName: string,
    readonly version: number,
    readonly coldStart: ColdStartBreakdown,
    private readonly vmm: ChildProcess,
    private readonly conn: net.Socket,
    private readonly cleanupPaths: string[],
    private readonly logger = log.child({ component: 'sandbox', backend: 'firecracker' }),
  ) {
    this.id = id;
    const decoder = new NdjsonDecoder((raw) => this.onGuestMessage(raw as GuestMessage));
    this.conn.on('data', (c) => decoder.push(c));
    this.conn.on('close', () => this.onDead('vsock closed'));
    this.conn.on('error', (e) => this.onDead(`vsock error: ${(e as Error).message}`));
    this.vmm.on('exit', (code) => this.onDead(`vmm exited code=${code}`));
  }

  private onGuestMessage(msg: GuestMessage): void {
    if (msg.t !== 'result') return;
    const p = this.pending;
    if (!p || p.id !== msg.id) return;
    clearTimeout(p.timer);
    this.pending = null;
    this.lastUsedAt = performance.now();
    p.resolve({ id: msg.id, ok: msg.ok, body: msg.body, error: msg.error, handlerMs: msg.handlerMs });
  }

  private onDead(reason: string): void {
    if (!this.alive) return;
    this.alive = false;
    const p = this.pending;
    if (p) {
      clearTimeout(p.timer);
      this.pending = null;
      p.reject(errors.crashed(`microVM died mid-invocation: ${reason}`));
    }
    this.logger.debug('microVM dead', { id: this.id, reason });
  }

  invoke(req: InvokeRequest): Promise<InvokeResult> {
    if (!this.alive) return Promise.reject(errors.crashed('microVM is not alive'));
    if (this.pending) {
      return Promise.reject(new Error(`sandbox ${this.id} already has an in-flight invocation`));
    }
    return new Promise<InvokeResult>((resolve, reject) => {
      // Mirrors the process backend: the guest aborts at `deadlineMs`, this is
      // the backstop for a handler that ignored the signal.
      const timer = setTimeout(() => {
        this.pending = null;
        void this.destroy('invocation timeout');
        reject(errors.timeout(req.deadlineMs));
      }, req.deadlineMs + TIMEOUT_GRACE_MS);
      timer.unref?.();
      this.pending = { id: req.id, resolve, reject, timer };
      const msg: HostMessage = {
        t: 'invoke',
        id: req.id,
        payload: req.payload,
        deadlineMs: req.deadlineMs,
      };
      this.conn.write(encodeNdjson(msg));
    });
  }

  async destroy(reason: string): Promise<void> {
    if (!this.alive) return;
    this.alive = false;
    this.logger.debug('destroying microVM', { id: this.id, reason });
    // A microVM has no graceful shutdown worth waiting for -- there is no
    // filesystem state to flush, so SIGKILL on the VMM is the fast path.
    try {
      this.conn.destroy();
    } catch {
      /* already gone */
    }
    this.vmm.kill('SIGKILL');
    await Promise.all(
      this.cleanupPaths.map((p) => fs.rm(p, { force: true, recursive: true }).catch(() => {})),
    );
  }
}

export class FirecrackerBackend implements SandboxBackend {
  readonly name = 'firecracker';
  private readonly cfg: FirecrackerConfig;
  private readonly logger = log.child({ component: 'backend', backend: 'firecracker' });

  constructor(overrides: Partial<FirecrackerConfig> = {}) {
    this.cfg = { ...FIRECRACKER_DEFAULTS, ...overrides };
    if (this.cfg.snapshot.enabled) this.snapshots = this.makeSnapshotStore();
  }

  async preflight(): Promise<void> {
    if (os.platform() !== 'linux') {
      throw new Error(
        `firecracker backend requires Linux with /dev/kvm (host is ${os.platform()}). ` +
          `Use IGNIS_BACKEND=process for local development.`,
      );
    }
    for (const [label, p] of [
      ['kvm device', '/dev/kvm'],
      ['firecracker binary', this.cfg.binary],
      ['kernel image', this.cfg.kernelImage],
      ['rootfs image', this.cfg.rootfsImage],
    ] as const) {
      try {
        await fs.access(p);
      } catch {
        throw new Error(`firecracker backend: ${label} not found at ${p}`);
      }
    }
    await fs.mkdir(this.cfg.runDir, { recursive: true });
  }

  /**
   * Allocate paths, start the VMM process and begin listening for the guest.
   *
   * Shared by boot, restore and capture -- all three need a live VMM with an
   * API socket and a pending guest connection before they diverge.
   */
  private startVmm(): {
    vmId: string;
    apiSock: string;
    vsockUds: string;
    overlay: string;
    listenPath: string;
    accepted: Promise<net.Socket>;
    vmm: ChildProcess;
    paths: string[];
  } {
    const vmId = `vm-${randomUUID().slice(0, 8)}`;
    const apiSock = path.join(this.cfg.runDir, `${vmId}.api.sock`);
    const vsockUds = path.join(this.cfg.runDir, `${vmId}.vsock`);
    const overlay = path.join(this.cfg.runDir, `${vmId}.overlay.ext4`);
    // The host listens; the guest dials CID 2 and Firecracker bridges the
    // connection to `<uds_path>_<port>`.
    const listenPath = `${vsockUds}_${this.cfg.vsockPort}`;
    const accepted = this.listenForGuest(listenPath);

    const vmm = spawn(this.cfg.binary, ['--api-sock', apiSock, '--id', vmId], {
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    vmm.stderr?.on('data', (b: Buffer) =>
      this.logger.warn('vmm stderr', { vmId, line: b.toString().trimEnd() }),
    );

    return {
      vmId,
      apiSock,
      vsockUds,
      overlay,
      listenPath,
      accepted,
      vmm,
      paths: [apiSock, vsockUds, listenPath, overlay],
    };
  }

  /**
   * Boot a VM from scratch and load the handler into it.
   *
   * Note there is no snapshot branch here any more: restore is a separate
   * entry point on the snapshot store, chosen by the orchestration layer. When
   * the two were fused, the restore path went on to call `loadHandler` against
   * a VM that already had the module loaded -- paying the import cost the
   * snapshot existed to eliminate.
   */
  async create(spec: FunctionSpec): Promise<Sandbox> {
    const bootStart = performance.now();
    const vm = this.startVmm();

    try {
      await waitForSocket(vm.apiSock);
      await this.configureAndBoot(vm.apiSock, spec, vm.vsockUds, vm.overlay);

      const conn = await vm.accepted;
      const bootMs = performance.now() - bootStart;
      const loadMs = await this.loadHandler(conn, spec);

      return new FirecrackerSandbox(
        vm.vmId,
        spec.name,
        spec.version,
        { bootMs, loadMs, totalMs: performance.now() - bootStart, restored: false },
        vm.vmm,
        conn,
        vm.paths,
      );
    } catch (err) {
      vm.vmm.kill('SIGKILL');
      await Promise.all(vm.paths.map((p) => fs.rm(p, { force: true }).catch(() => {})));
      throw err;
    }
  }

  /** Cold path: full VM configuration then InstanceStart. */
  private async configureAndBoot(
    apiSock: string,
    spec: FunctionSpec,
    vsockUds: string,
    overlay: string,
  ): Promise<void> {
    await fcApi(apiSock, 'PUT', '/boot-source', {
      kernel_image_path: this.cfg.kernelImage,
      boot_args: `${this.cfg.bootArgs} ignis.fn=${spec.name} ignis.port=${this.cfg.vsockPort}`,
    });

    // The base rootfs is shared read-only across every microVM on the host;
    // per-VM writes land in the overlay, so nothing leaks between tenants.
    await fcApi(apiSock, 'PUT', '/drives/rootfs', {
      drive_id: 'rootfs',
      path_on_host: this.cfg.rootfsImage,
      is_root_device: true,
      is_read_only: true,
    });
    await fs.writeFile(overlay, '').catch(() => {});
    await fcApi(apiSock, 'PUT', '/drives/overlay', {
      drive_id: 'overlay',
      path_on_host: overlay,
      is_root_device: false,
      is_read_only: false,
    });

    await fcApi(apiSock, 'PUT', '/machine-config', {
      vcpu_count: this.cfg.vcpus,
      mem_size_mib: spec.memoryMib,
      smt: false,
    });

    await fcApi(apiSock, 'PUT', '/vsock', {
      vsock_id: 'ctrl',
      guest_cid: 3,
      uds_path: vsockUds,
    });

    await fcApi(apiSock, 'PUT', '/actions', { action_type: 'InstanceStart' });
  }

  /** `<dir>/<fn>-v<version>` -- the `.snap` and `.mem` pair share this stem. */
  private snapshotBase(fn: string, version: number): string {
    return path.join(this.cfg.snapshot.dir, `${fn}-v${version}`);
  }

  /**
   * Snapshot support, as consumed by SnapshotManager.
   *
   * Left undefined when snapshots are disabled, so `isSnapshotCapable` reports
   * false and the orchestration layer stays out of the deploy path entirely.
   * Capturing costs a full boot and a memory image per version -- not something
   * to start doing because the backend merely could.
   */
  readonly snapshots?: SnapshotStore;

  private makeSnapshotStore(): SnapshotStore {
    return {
    capture: async (spec: FunctionSpec): Promise<SnapshotInfo> => {
      const started = performance.now();
      await fs.mkdir(this.cfg.snapshot.dir, { recursive: true });
      const base = this.snapshotBase(spec.name, spec.version);
      const vm = this.startVmm();

      try {
        await waitForSocket(vm.apiSock);
        await this.configureAndBoot(vm.apiSock, spec, vm.vsockUds, vm.overlay);
        const conn = await vm.accepted;
        // Capture *after* the handler is loaded -- that module import is the
        // expensive half of a cold start and the main thing being frozen.
        await this.loadHandler(conn, spec);

        // Pausing first is required: Firecracker refuses to snapshot a running
        // VM, and a half-quiesced guest would produce a torn memory image.
        await fcApi(vm.apiSock, 'PATCH', '/vm', { state: 'Paused' });
        await fcApi(vm.apiSock, 'PUT', '/snapshot/create', {
          snapshot_type: 'Full',
          snapshot_path: `${base}.snap`,
          mem_file_path: `${base}.mem`,
        });

        const bytes = await totalBytes([`${base}.snap`, `${base}.mem`]);
        return {
          fn: spec.name,
          version: spec.version,
          bytes,
          captureMs: performance.now() - started,
          capturedAt: Date.now(),
        };
      } finally {
        // The capture VM is scaffolding; it never serves traffic.
        vm.vmm.kill('SIGKILL');
        await Promise.all(vm.paths.map((p) => fs.rm(p, { force: true }).catch(() => {})));
      }
    },

    restore: async (spec: FunctionSpec): Promise<Sandbox | null> => {
      const base = this.snapshotBase(spec.name, spec.version);
      try {
        await Promise.all([fs.access(`${base}.snap`), fs.access(`${base}.mem`)]);
      } catch {
        // No image for this version: tell the caller to boot normally.
        return null;
      }

      const started = performance.now();
      const vm = this.startVmm();

      try {
        await waitForSocket(vm.apiSock);
        await fcApi(vm.apiSock, 'PUT', '/snapshot/load', {
          snapshot_path: `${base}.snap`,
          // Demand-paging beats reading a whole memory image off disk before
          // the VM can run -- that read is the cost being avoided.
          mem_backend: { backend_type: 'UffdOverFile', backend_path: `${base}.mem` },
          enable_diff_snapshots: false,
          resume_vm: true,
        });
        // The restored VM carries the captured VM's socket path; repoint it.
        await fcApi(vm.apiSock, 'PATCH', '/vsock', { vsock_id: 'ctrl', uds_path: vm.vsockUds });

        const conn = await vm.accepted;
        const totalMs = performance.now() - started;

        // No loadHandler: the snapshot was taken with the module already
        // imported, so the guest is ready to serve as soon as it reconnects.
        return new FirecrackerSandbox(
          vm.vmId,
          spec.name,
          spec.version,
          { bootMs: totalMs, loadMs: 0, totalMs, restored: true },
          vm.vmm,
          conn,
          vm.paths,
        );
      } catch (err) {
        vm.vmm.kill('SIGKILL');
        await Promise.all(vm.paths.map((p) => fs.rm(p, { force: true }).catch(() => {})));
        throw err;
      }
    },

    evict: async (fn: string, keepVersion: number | null): Promise<number> => {
      let entries: string[];
      try {
        entries = await fs.readdir(this.cfg.snapshot.dir);
      } catch {
        return 0;
      }

      const removed = new Set<number>();
      await Promise.all(
        entries.map(async (name) => {
          const parsed = parseSnapshotName(name);
          if (!parsed || parsed.fn !== fn) return;
          if (keepVersion !== null && parsed.version === keepVersion) return;
          await fs.rm(path.join(this.cfg.snapshot.dir, name), { force: true }).catch(() => {});
          removed.add(parsed.version);
        }),
      );
      // Each version is two files; count versions, not unlinks.
      return removed.size;
    },

    list: async (): Promise<SnapshotInfo[]> => {
      let entries: string[];
      try {
        entries = await fs.readdir(this.cfg.snapshot.dir);
      } catch {
        return [];
      }

      const byVersion = new Map<string, SnapshotInfo>();
      for (const name of entries) {
        const parsed = parseSnapshotName(name);
        if (!parsed) continue;
        const key = `${parsed.fn}@${parsed.version}`;
        let info = byVersion.get(key);
        if (!info) {
          info = {
            fn: parsed.fn,
            version: parsed.version,
            bytes: 0,
            captureMs: 0,
            capturedAt: 0,
          };
          byVersion.set(key, info);
        }
        try {
          const st = await fs.stat(path.join(this.cfg.snapshot.dir, name));
          info.bytes += st.size;
          info.capturedAt = Math.max(info.capturedAt, st.mtimeMs);
        } catch {
          /* raced with an eviction */
        }
      }
      return [...byVersion.values()];
      },
    };
  }

  /** Accept exactly one guest connection on the bridged vsock socket. */
  private listenForGuest(listenPath: string): Promise<net.Socket> {
    return new Promise((resolve, reject) => {
      const server = net.createServer();
      const timer = setTimeout(() => {
        server.close();
        reject(new Error(`guest never connected on ${listenPath}`));
      }, 10_000);
      timer.unref?.();

      server.once('connection', (socket) => {
        clearTimeout(timer);
        // One guest per VM; stop accepting so a compromised guest cannot open
        // a second control channel.
        server.close();
        resolve(socket);
      });
      server.on('error', (e) => {
        clearTimeout(timer);
        reject(e);
      });
      fs.rm(listenPath, { force: true })
        .catch(() => {})
        .then(() => server.listen(listenPath));
    });
  }

  /** Send `load` and wait for the guest to confirm the handler is callable. */
  private loadHandler(conn: net.Socket, spec: FunctionSpec): Promise<number> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`handler load timed out for ${spec.name}`)), 10_000);
      timer.unref?.();

      const decoder = new NdjsonDecoder((raw) => {
        const msg = raw as GuestMessage;
        if (msg.t === 'loaded') {
          clearTimeout(timer);
          conn.off('data', onData);
          resolve(msg.loadMs);
        } else if (msg.t === 'load_failed') {
          clearTimeout(timer);
          conn.off('data', onData);
          reject(new Error(`handler load failed for ${spec.name}: ${msg.message}`));
        }
      });
      const onData = (c: Buffer) => decoder.push(c);
      conn.on('data', onData);

      const msg: HostMessage = { t: 'load', entrypoint: spec.entrypoint };
      conn.write(encodeNdjson(msg));
    });
  }

  async shutdown(): Promise<void> {
    // Sockets and overlays are removed per-sandbox in destroy(); sweep any
    // orphans left by a hard crash.
    await fs
      .readdir(this.cfg.runDir)
      .then((entries) =>
        Promise.all(
          entries
            .filter((e) => e.startsWith('vm-'))
            .map((e) => fs.rm(path.join(this.cfg.runDir, e), { force: true, recursive: true })),
        ),
      )
      .catch(() => {});
  }
}

/**
 * Parse `<fn>-v<version>.<snap|mem>`.
 *
 * Function names allow hyphens, so the split has to anchor on the *last*
 * `-v<digits>` group -- splitting on the first one would mangle every
 * hyphenated name.
 */
export function parseSnapshotName(file: string): { fn: string; version: number } | null {
  const m = /^(.+)-v(\d+)\.(snap|mem)$/.exec(file);
  if (!m) return null;
  return { fn: m[1]!, version: Number(m[2]) };
}

/** Sum the sizes of files that exist, ignoring the ones that do not. */
async function totalBytes(paths: string[]): Promise<number> {
  const sizes = await Promise.all(
    paths.map((p) =>
      fs
        .stat(p)
        .then((s) => s.size)
        .catch(() => 0),
    ),
  );
  return sizes.reduce((a, b) => a + b, 0);
}
