# Operations

## Host setup

Satchel needs the Linux NBD module and one `/dev/nbdN` device per mounted volume. Load the module before starting Docker or Satchel:

```sh
modprobe nbd nbds_max=64
```

The service also needs `CAP_SYS_ADMIN`, `mkfs.ext4`, and `mount`.

## S3 requirements

The bucket must implement conditional `PUT` requests with `If-Match` and `If-None-Match`. Satchel verifies both operations at startup. AWS S3, MinIO, and compatible services with those semantics work.

## Lease recovery

The active lease lives inside the volume's `state.json`. If a node dies, another node can take over after the lease expires. The default TTL is 30 seconds.

`satchel vol lease break <volume>` clears the lease with a conditional write. A stale node cannot publish another generation after this operation, but applications on that node may keep receiving successful local writes until Satchel fences its NBD mount. Stop or isolate the old node before breaking a lease.

## Backpressure

Each write copies the affected 4 KiB blocks into an in-memory generation. Failed uploads remain queued. Satchel pauses new writes once unpublished block data reaches `--dirty-limit`, which defaults to 256 MiB. Reads continue.

The queue is memory-backed. In async mode, a process or host crash loses unpublished generations. In remote mode, Satchel does not acknowledge a filesystem flush until that queue has reached S3.

## Database durability

Volumes default to `durability=remote`. An ext4 flush or FUA write syncs the local image, uploads all dirty blocks, writes a manifest, and conditionally advances `state.json` before returning success. If S3 is unavailable, the flush fails instead of claiming the transaction is durable.

This adds S3 latency to database commits that call `fsync`. Ordinary reads and writes still use the local block device. Set `durability=async` only when the lower commit latency is worth a recovery-point window of up to the sync interval.

## Checkpoints

Satchel creates a full checkpoint when the delta chain reaches `--checkpoint-interval`. The default is 128 generations. In remote durability mode this happens on a filesystem flush, when NBD I/O is paused at a consistent boundary. Async volumes checkpoint during clean unmount. A checkpoint scans and uploads every non-zero block, so the flush or unmount that creates one takes longer for a large, full volume.

Checkpoint parent links retain point-in-time history. `satchel vol restore` accepts either `--generation` or `--timestamp`. The defaults retain seven days of history and wait another 24 hours before deleting expired objects. Set `SATCHEL_HISTORY_RETENTION` and `SATCHEL_GC_GRACE` to change those windows. Clean unmounts run the collector automatically. `satchel vol gc <volume>` runs it for an idle volume and refuses to proceed while another node holds the lease.

## Local state

`<state-dir>/volumes` contains the local Docker registry. `<state-dir>/images` contains sparse images while mounted. `<state-dir>/mounts` contains ext4 mountpoints. Images are disposable and Satchel removes them after a clean unmount.

## Shutdown

Stop containers before Satchel. On SIGTERM, Satchel unmounts each filesystem, publishes its final generation, releases the lease, detaches NBD, and removes the local image. A busy filesystem makes shutdown fail rather than discarding writes.
