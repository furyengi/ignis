# Running on Firecracker

The `process` backend exists so the scheduler can be developed and measured
anywhere. This is the one that actually isolates tenants.

## Requirements

- Linux with `/dev/kvm` readable by the ignis user
- On EC2: a `.metal` instance, or `m5.*`/`c5.*` with nested virtualisation. On a
  laptop, bare-metal Linux — nested virt inside VirtualBox/Hyper-V generally
  will not expose KVM.

```bash
lsmod | grep kvm && ls -l /dev/kvm
```

## 1. Firecracker binary

```bash
ARCH=$(uname -m) && VERSION=v1.10.1
curl -sSL "https://github.com/firecracker-microvm/firecracker/releases/download/${VERSION}/firecracker-${VERSION}-${ARCH}.tgz" | tar -xz
sudo install "release-${VERSION}-${ARCH}/firecracker-${VERSION}-${ARCH}" /usr/bin/firecracker
```

## 2. Kernel

Firecracker boots an **uncompressed** `vmlinux`, not a `bzImage`. The stock
distro kernel will not work.

```bash
curl -sSLo /var/lib/ignis/vmlinux https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.10/x86_64/vmlinux-6.1.102
```

Building your own is worth real milliseconds — strip every driver you do not
need, since each one costs probe time on every single cold start.

## 3. Rootfs

The rootfs needs Node, the compiled guest shim, and a vsock bridge. Node has no
`AF_VSOCK` binding, so `socat` translates between a Unix socket the shim can
open and the vsock device Firecracker exposes.

```bash
dd if=/dev/zero of=/var/lib/ignis/rootfs.ext4 bs=1M count=512
mkfs.ext4 /var/lib/ignis/rootfs.ext4
mkdir -p /mnt/ignis && sudo mount /var/lib/ignis/rootfs.ext4 /mnt/ignis
```

Populate it with a minimal userland (Alpine works well), then:

```bash
sudo cp -r dist/src/guest /mnt/ignis/opt/ignis/
sudo cp -r dist/src/backends /mnt/ignis/opt/ignis/
```

Init script — the bridge must be up before the shim starts:

```sh
#!/bin/sh
socat UNIX-LISTEN:/run/ignis.sock,fork VSOCK-CONNECT:2:5252 &
export IGNIS_CHANNEL=vsock IGNIS_VSOCK_BRIDGE=/run/ignis.sock
exec node /opt/ignis/guest/shim.js
```

```bash
sudo umount /mnt/ignis
```

The base image is mounted **read-only** and shared across every microVM on the
host; per-VM writes land in a separate overlay drive. That is what makes it safe
to run one image for many tenants.

## 4. Run

```bash
IGNIS_BACKEND=firecracker npm start
```

Configuration is by environment: `IGNIS_FC_BINARY`, `IGNIS_FC_KERNEL`,
`IGNIS_FC_ROOTFS`, `IGNIS_FC_RUNDIR`.

Boot args matter. `pci=off`, `quiet`, `loglevel=0`, `i8042.noaux` and
`i8042.nomux` between them remove tens of milliseconds of device probing and
console I/O from every cold start.

## 5. Snapshots — the actual win

Booting a kernel, starting Node and importing the handler's module graph costs
~125ms and produces an identical VM every time. Snapshot restore skips all of
it: capture the VM once, after the handler is loaded, then resume a copy per
cold start.

```bash
IGNIS_FC_SNAPSHOT=1 IGNIS_FC_SNAPSHOT_DIR=/var/lib/ignis/snapshots npm start
```

`captureSnapshot()` pauses a booted VM and writes `<fn>-v<version>.snap` plus a
`.mem` file; `restoreSnapshot()` loads it with `resume_vm: true` and re-points
vsock at the new socket. Run capture once per function version at deploy time.

Two things to know before trusting it:

- **Memory is the cost.** Every snapshot is a full memory image on disk.
  128MiB per function version adds up quickly; `UffdOverFile` demand-pages it
  rather than reading it all up front, which is why the config uses that backend.
- **Restored VMs share entropy state.** Every VM resumed from one snapshot
  starts with an identical RNG state and an identical view of the clock. For
  anything cryptographic the guest must reseed on resume. This is a real
  vulnerability class, not a footnote — Firecracker's own docs cover it.

## Snapshot capture and rootfs building are not automated here

The API calls are implemented in `src/backends/firecracker.ts`. Wiring them into
the deploy path — build the image, boot once, capture, store, garbage-collect
old versions — is the obvious next piece of work and is not done.
