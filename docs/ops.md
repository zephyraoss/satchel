# Operations

## Storage node

Any S3-compatible service that supports conditional writes: MinIO, Cloudflare R2, SeaweedFS S3 gateway, AWS S3. Satchel verifies `If-None-Match` and `If-Match` at startup with a probe object under `leases/.probe-*` and refuses to start if the backend ignores them.

Bucket layout:

```
vols/<volume>/...          litestream LTX files
leases/<volume>.json       lease
```

## Lease recovery

If a node dies while holding a lease, the lease expires after the TTL (default 30s) and the next mount takes it over. If you need it sooner, `satchel vol lease break <volume>` deletes it after making you type the holder's name. Before doing that, be certain the old holder is gone: if it is still running it will fence itself on the next heartbeat (so no further replication happens), but any data it wrote since its last sync is lost.

## Read-only replicas

`mode=ro` volumes restore the bucket state at mount time and never refresh while mounted. To roll out a config change to N read-only replicas: `satchel vol put` (or mount the `rw` volume somewhere, edit, unmount), then restart the replicas. They take no lease, so they never block the writer and the writer never blocks them.

## Replica-scoped volumes

`scope=replica` names each container's volume `<name>.r-<12 hex>` from a hash of Docker's mount id. They show up individually in `satchel vol ls`. `docker volume rm <name>` on a node removes that node's record and any replica volumes it knows about; leftovers from containers that died elsewhere have to be removed from the bucket with `satchel vol` or your S3 tooling (`vols/<name>.r-*/` and `leases/<name>.r-*.json`).

## Seeding

`seed=` is applied inside the first mount, after `litestream restore` finds nothing in the bucket and before the FUSE mount, in one transaction. A local path is read on the node doing the mount, so for anything other than a one-node cluster use an `s3://` tarball. Tar entries outside the archive root are rejected.

## Metrics

Enable with `--metrics-addr 127.0.0.1:9464`.

| metric | meaning |
|---|---|
| `satchel_mounted_volumes` | volumes currently mounted on this node |
| `satchel_lease_held{volume}` | 1 while this node holds the lease |
| `satchel_lease_fenced_total{volume}` | times this node lost a lease and fenced |
| `satchel_mount_failures_total{reason}` | `lease_held`, `lease_error`, `restore`, `backend` |
| `satchel_restore_duration_seconds` | litestream restore at mount |
| `satchel_sync_duration_seconds` | litestream shutdown + final sync at unmount |
| `satchel_wal_bytes{volume}` | WAL frames not yet checkpointed; grows when litestream falls behind |
| `satchel_backpressure_events_total{volume}` | times writes were paused because `satchel_wal_bytes` exceeded `--wal-limit` |

## Backpressure

Litestream checkpoints the WAL only after it has staged the frames into its local LTX directory, so un-checkpointed WAL is a direct measure of how far behind it is. When that exceeds `--wal-limit` (default 256 MiB) satchel pauses FUSE writes, logs a warning, and resumes as soon as a checkpoint lands; reads keep working. If the pause persists, Litestream is either stopped or cannot reach the bucket; check its lines in the satchel log at `--log-level debug`.

The staged-but-not-uploaded LTX files live in `<state-dir>/dbs/.<vol>.db-litestream/` and are not counted by the limit. A long bucket outage grows that directory instead; bounding it is on the roadmap.

## Mounting without Docker

`satchel mount <vol>` takes the lease, restores, mounts under `<state-dir>/mounts/<vol>` with live replication, prints the path, and unmounts cleanly on Ctrl-C. Useful for inspecting or seeding a volume from a workstation. It holds the lease, so the service cannot start while you have it mounted.

## Local state

`/var/lib/satchel/volumes/*.json` is the registry of volumes this node knows about. A node that has never seen a volume will adopt it on first `Get`/`Mount` if `vols/<volume>/` exists in the bucket. `dbs/` holds the SQLite file, its WAL, and Litestream's sidecar directory while a volume is mounted; `mounts/` holds the FUSE mountpoints. Both are wiped on every mount and unmount. Put the state dir on a real disk, not tmpfs: the WAL can reach ~1 GB under sustained writes before Litestream truncates it.

Because the local copy is discarded on every mount and the bucket is the only durable copy, SQLite runs with `synchronous=OFF`: commits and checkpoints never fsync the state dir. A satchel crash loses nothing (the page cache survives it); a host crash loses at most what had not been uploaded, which is the same window as before. Checkpointing is left to Litestream (`wal_autocheckpoint=0`, `default checkpoint cadence), so un-checkpointed WAL is exactly the data Litestream has not yet staged.

## Shutdown

SIGTERM unmounts every mounted volume (FUSE unmount, Litestream shutdown with final sync, lease release) before exiting, with a two-minute budget. Stop satchel after the containers that use it; a FUSE unmount with open files is retried for five seconds and then fails, leaving the lease held until the plugin is restarted and unmounts cleanly.
