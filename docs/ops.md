# Operations

## Host setup

Satchel needs the Linux NBD module and one `/dev/nbdN` device per mounted volume. Load the module before starting Docker or Satchel:

```sh
modprobe nbd nbds_max=64
```

The service also needs `CAP_SYS_ADMIN`, `mkfs.ext4`, and `mount`.

## S3 requirements

The default head protocol (`--s3-head=conditional`) needs conditional `PUT` requests with `If-Match` and `If-None-Match`. Satchel verifies both operations at startup and refuses to run if the backend ignores them. AWS S3, MinIO, Cloudflare R2, and compatible services with those semantics work.

Garage does not implement conditional writes and its maintainers consider them out of scope, so run Satchel against Garage with `--s3-head=append` (`SATCHEL_S3_HEAD=append`). That mode replaces the single `If-Match` object with an append-only version log and a settle window on takeover; the startup probe then checks read-after-write listing instead. The append head is safe for one writer per volume but relies on bounded clock skew between nodes rather than a server-side compare-and-swap, so keep NTP running on every node and give it its own bucket: a volume published in one head mode cannot be opened in the other, and Satchel reports which mode a volume needs when that happens.

### Append head takeover

With the append head, lease takeover writes a claim version, waits for the settle window (three seconds by default), lists the head prefix, and proceeds only if no other node claimed a newer epoch in the meantime. Two nodes racing for an expired lease each write a claim and wait out the settle window; exactly one proceeds, and the other's mount fails with a lease-lost error rather than retrying on its own. A claim that took longer than half the settle window to reach the bucket is discarded rather than trusted. A stale writer is fenced by its next state write: every write lists the prefix afterwards and fails with lease loss when a newer epoch exists. That check costs one LIST per publish, so commit latency on Garage is one PUT plus one LIST rather than one conditional PUT.

## Lease recovery

The active lease lives inside the volume's `state.json`. If a node dies, another node can take over after the lease expires. The default TTL is 30 seconds.

`satchel vol lease break <volume>` clears the lease with a conditional write. A stale node cannot publish another generation after this operation, but applications on that node may keep receiving successful local writes until Satchel fences its NBD mount. Stop or isolate the old node before breaking a lease.

## S3 outage and self-fencing

Under `durability=remote`, S3 availability is coupled to database availability.
Satchel renews its lease with a conditional write to `state.json`. A commit that
cannot reach S3 stalls rather than failing: the remote flush keeps retrying with
backoff while the filesystem flush stays blocked, so a database in the commit
path waits instead of taking an I/O error. This is safe because a blocked flush
is an unacknowledged commit, and publication only succeeds once the conditional
state update confirms this node still holds the lease.

An outage that clears within the lease TTL therefore only stalls commits; the
database keeps running and resumes when S3 returns. If S3 stays unreachable past
the lease TTL (default 30 seconds), Satchel can no longer prove it holds the
lease, so it fences the volume to stop a second node from writing concurrently.
Fencing returns I/O errors, and a database in the commit path then crashes.

This is a safety property, not a bug: a writer that cannot renew its lease must
stop before someone else takes over. The stall-then-fence behavior bounds the
availability cost to the lease TTL. To ride out longer S3 interruptions without
crashing the database, raise `SATCHEL_LEASE_TTL` (`--lease-ttl`); the cost is
slower failover when a node genuinely dies, since takeover waits for the longer
TTL to expire.

Recovery is clean. No acknowledged commit is lost, because a remote-mode commit
is not acknowledged until its state update reaches S3; commits in flight during
the outage fail rather than reporting false durability. After S3 returns, remount
the volume and start the database; it recovers to the last acknowledged commit.

Size your S3 availability against your database availability target: the
database tolerates outages up to the lease TTL without crashing, and any longer
outage crashes it but loses no acknowledged commit.

Under `durability=local`, filesystem flushes can continue against the local
journal during a short S3 outage. The lease still depends on S3. Satchel fences
the volume when it can no longer prove ownership, preserves the journal, and
requires the same node and state disk for automatic recovery. Do not move the
volume to another node if the unpublished local branch must be retained.

## Backpressure

Each write copies the affected 4 KiB blocks into a generation. Failed uploads remain queued. Satchel pauses new writes once unpublished block data reaches `--dirty-limit`, which defaults to 256 MiB. Reads continue.

In local mode, filesystem flushes write queued generations to the persistent journal before returning. In remote mode, Satchel does not acknowledge a filesystem flush until the queue has reached S3.

## Database durability

Volumes default to `durability=local`. An ext4 flush or FUA write appends complete changed blocks to `<journal-dir>/<volume>.journal`, syncs that log, and then returns success. Satchel publishes journaled generations to S3 in the background. A successful flush survives a Satchel crash and a same-node reboot, but it does not survive loss of the journal disk until S3 publication finishes.

After a crash, Satchel restores the current S3 image and replays committed journal records. It marks records reclaimable only after S3 advances the volume head, then compacts them in batches. If another writer advanced the remote history, the mount fails and leaves the journal untouched. An operator must choose which branch to keep.

With `durability=remote`, an ext4 flush or FUA write uploads any external segment objects, then conditionally advances `state.json` before returning success. Small manifests travel inside that state update. Satchel does not sync the disposable local image first. If S3 is unavailable, the flush waits until S3 recovers or lease loss fences the volume.

Small database commits need one durable S3 request: the fenced `state.json` update. Once inline history reaches 16 KiB, Satchel starts an immutable bundle upload in the background and installs its pointer in a later state update. The inline area has a 64 KiB hard limit. Commits with external segments or a full inline area still use an upload phase followed by the state update. Ordinary reads and writes use the local block device. Put S3 in the same low-latency region when using this mode.

Replication in local mode runs on the configured interval, wakes early as the unpublished-data limit fills, and applies backpressure at that limit. The sparse image remains a disposable cache. Only the journal and published S3 generations participate in recovery.

## Checkpoints

Satchel compacts the manifest chain when it reaches `--checkpoint-interval`. The default is 128 generations. The replication worker creates a checkpoint after periodic publication, and clean unmount creates one if it is due. A checkpoint reuses existing segment objects, packs pending inline history, and publishes a compact manifest plus `state.json`. Satchel acknowledges the flush that reaches the interval before starting checkpoint work. A closely following flush can queue behind the metadata operation, but no checkpoint scans or re-uploads the volume.

Checkpoint parent links retain point-in-time history. `satchel vol restore` accepts either `--generation` or `--timestamp`. The defaults retain seven days of history and wait another 24 hours before deleting expired objects. Set `SATCHEL_HISTORY_RETENTION` and `SATCHEL_GC_GRACE` to change those windows. Run `satchel vol gc <volume>` as a scheduled maintenance job. It acquires the volume lease and refuses to proceed while another node holds it. Unmount does not run the collector because scanning a long retained history would delay container shutdown.

## Local state

`<state-dir>/volumes` contains the local Docker registry. `<journal-dir>` contains locally durable generations that have not reached S3 and defaults to `<state-dir>/journals`. Put it on low-latency persistent storage. `<state-dir>/images` contains sparse images while mounted. `<state-dir>/mounts` contains ext4 mountpoints. Images are disposable and Satchel removes them after a clean unmount. Clean unmount publishes all journaled data and removes the journal.

On btrfs, create an empty journal directory with NOCOW before Satchel writes its first journal. Satchel journals have their own SHA-256 checksums, so they do not depend on btrfs data checksums:

```sh
sudo install -d -m 0700 /var/lib/satchel/journals
sudo chattr +C /var/lib/satchel/journals
```

An existing writable volume mounts after Satchel reads its manifest chain. Segment bodies arrive on the first block read and remain in the sparse local image for the life of the mount. A cold read can therefore include one S3 GET and decompression of up to 4 MiB of block data. Full-block writes do not fetch the prior remote block. Partial-block writes do. Read-only mounts materialize their fixed snapshot before mounting because they do not hold a lease that can pin old objects against garbage collection.

## Shutdown

Stop containers before Satchel. On SIGTERM, Satchel unmounts each filesystem, publishes its final generation, releases the lease, detaches NBD, and removes the local image. A busy filesystem makes shutdown fail rather than discarding writes.
