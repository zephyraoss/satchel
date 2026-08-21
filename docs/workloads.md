# Workload guidance

## Good fits

- Application configuration and small state files
- User uploads and media in the tens of thousands of files, low GB total
- Caches that are nice to keep across a redeploy but cheap to lose
- Single-process tools that keep their state in a directory (Gitea, Vaultwarden, Miniflux, Grafana's sqlite file, etc.)

## Poor fits

- Postgres, MySQL, MongoDB, anything with its own WAL and fsync discipline. Every fsync becomes a SQLite transaction, and replication adds latency you will notice.
- Anything that expects shared access from several nodes at once. Satchel enforces one writer; use JuiceFS, NFS, or an object store directly.
- Volumes over a few GB. Restore-on-migrate downloads the whole database before the container starts.

## What a crash loses

Litestream stages WAL frames into local LTX files every `--sync-interval` (default 5s) and uploads them from there. A node that dies loses at most the last interval's writes plus whatever was staged locally but not yet uploaded. The lease expires after its TTL (default 30s) and the next mount restores the latest state in the bucket.

`fsync` on a satchel file returns immediately: every write is already a committed SQLite transaction, so there is nothing extra to flush locally, and durability to the bucket is governed by the sync interval rather than by fsync. Applications that rely on fsync as a remote-durability barrier do not get that here.

## Performance shape

Files are stored in 64 KiB chunks. Sequential I/O and whole-file rewrites are cheap; random 4 KiB writes rewrite a 64 KiB chunk each (about 1,400/s), so databases and append-heavy logs with small records are the wrong fit. Reads of recently touched data come from the kernel page cache. See [benchmarks.md](benchmarks.md).

## Scaling past one replica

| need | do this |
|---|---|
| many replicas reading the same config | `mode=ro` (v2) |
| each replica with its own state | `scope=replica` (v2) |
| true shared read/write | a shared filesystem, not satchel |
