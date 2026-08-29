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
 * With `snapshot.enabled`, steps 2-6 are replaced by a snapshot restore of a
 * VM already past handler load. That is the difference between a ~125ms cold
 * start and a ~10ms one, because it skips kernel boot, Node startup and the
 * handler's own module graph.
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
import type { Sandbox, SandboxBackend } from './backend.js';
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

  async create(spec: FunctionSpec): Promise<Sandbox> {
    const bootStart = performance.now();
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

    try {
      await waitForSocket(apiSock);

      if (this.cfg.snapshot.enabled) {
        await this.restoreSnapshot(apiSock, spec, vsockUds);
      } else {
        await this.configureAndBoot(apiSock, spec, vsockUds, overlay);
      }

      const conn = await accepted;
      const bootMs = performance.now() - bootStart;
      const loadMs = await this.loadHandler(conn, spec);

      return new FirecrackerSandbox(
        vmId,
        spec.name,
        spec.version,
        { bootMs, loadMs, totalMs: performance.now() - bootStart },
        vmm,
        conn,
        [apiSock, vsockUds, listenPath, overlay],
      );
    } catch (err) {
      vmm.kill('SIGKILL');
      await Promise.all(
        [apiSock, vsockUds, listenPath, overlay].map((p) => fs.rm(p, { force: true }).catch(() => {})),
      );
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

  /**
   * Warm path: restore a VM captured after the handler was already loaded.
   * Skips kernel boot and module import entirely.
   */
  private async restoreSnapshot(apiSock: string, spec: FunctionSpec, vsockUds: string): Promise<void> {
    const base = path.join(this.cfg.snapshot.dir, `${spec.name}-v${spec.version}`);
    await fcApi(apiSock, 'PUT', '/snapshot/load', {
      snapshot_path: `${base}.snap`,
      mem_backend: { backend_type: 'UffdOverFile', backend_path: `${base}.mem` },
      enable_diff_snapshots: false,
      resume_vm: true,
    });
    await fcApi(apiSock, 'PATCH', '/vsock', { vsock_id: 'ctrl', uds_path: vsockUds });
  }

  /**
   * Capture a snapshot of a booted, handler-loaded VM. Run once per function
   * version at deploy time; every subsequent cold start restores from it.
   */
  async captureSnapshot(apiSock: string, spec: FunctionSpec): Promise<void> {
    const base = path.join(this.cfg.snapshot.dir, `${spec.name}-v${spec.version}`);
    await fs.mkdir(this.cfg.snapshot.dir, { recursive: true });
    await fcApi(apiSock, 'PATCH', '/vm', { state: 'Paused' });
    await fcApi(apiSock, 'PUT', '/snapshot/create', {
      snapshot_type: 'Full',
      snapshot_path: `${base}.snap`,
      mem_file_path: `${base}.mem`,
    });
    this.logger.info('snapshot captured', { fn: spec.name, version: spec.version, path: base });
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
