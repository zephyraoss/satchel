# Benchmarks

Measured 2026-08-21 on a 6-vCPU Xeon Platinum 8260 VM, Fedora, Linux 6.19, Litestream 0.5.16, MinIO on the same host. Reproduce with `go test ./internal/backend/fuse/ -bench . -run xxx` (no replication) and `./test/bench/run.sh` (fio against a live mount with Litestream replicating at a 1s sync interval).

The `go test` benchmarks put the database under `$TMPDIR`, which is tmpfs on Fedora, so the no-replication tables below are page-cache numbers. Set `TMPDIR` to a real disk to see what the filesystem costs; `run.sh` always uses `SATCHEL_STATE_DIR`.

## Local disk and fsync

The same VM's btrfs root (virtio, `compress=zstd`) takes ~80 ms per fsync. With SQLite's defaults (`synchronous=NORMAL`, `wal_autocheckpoint=1000`) every 8 MiB of WAL triggered a checkpoint that fsynced both files inside the committing transaction, which capped the FUSE backend at 20 MB/s sequential on that disk, 2.8 ms per small file and 2.2 ms per random 4 KiB write, regardless of Litestream. The local database is wiped on every mount, so satchel now runs `synchronous=OFF` and leaves checkpointing to Litestream. On the same disk, no replication:

| workload | NORMAL + autocheckpoint | OFF, Litestream checkpoints |
|---|---|---|
| FUSE: sequential write 1 MiB | 20 MB/s | 95 MB/s |
| FUSE: create 4 KiB file | 2.81 ms | 1.06 ms |
| FUSE: random 4 KiB write | 2.21 ms | 0.48 ms |


## SQLite driver

Default build uses `mattn/go-sqlite3` (cgo). `CGO_ENABLED=0` falls back to `modernc.org/sqlite`.

| workload | cgo | pure Go |
|---|---|---|
| store: create + write 4 KiB | 117 µs | 149 µs |
| store: random 4 KiB write | 248 µs | 726 µs |
| FUSE: sequential write 1 MiB | 213 MB/s | 139 MB/s |
| FUSE: random 4 KiB write | 519 µs (1,900 IOPS) | 939 µs (1,060 IOPS) |
| FUSE: random 4 KiB read, `O_DIRECT` | 224 µs (4,500 IOPS) | 377 µs (2,650 IOPS) |
| FUSE: create 4 KiB file | 1.04 ms | 1.10 ms |

The tables below were measured with the pure-Go driver before the switch; scale writes up by the ratios above.

## FUSE backend alone (no Litestream)

| workload | result |
|---|---|
| sequential write, 1 MiB writes | 167 MB/s |
| sequential read, 1 MiB reads (page cache) | 5.5 GB/s |
| random 4 KiB write into a 64 MiB file | 730 µs/op, ~1,400 IOPS |
| random 4 KiB read, `O_DIRECT` | 340 µs/op, ~2,900 IOPS |
| create + write 4 KiB file | 0.9 ms/op, ~1,100 files/s |
| read 4 KiB file (open/read/close) | 110 µs/op |
| `stat` (attr cached 1s) | 1.2 µs/op |

Store layer without FUSE, for reference: create + write 4 KiB is 173 µs, a transaction with one `stat` is 24 µs, a 4 KiB random write is 0.8 ms. Roughly half of each FUSE op is kernel round trips and the rest is SQLite.

## Chunk size

Files are stored in fixed-size chunks, one row each. The size is recorded per volume in `meta.chunk_size` and defaults to 64 KiB.

| chunk | sequential write | random 4 KiB write | create 4 KiB file |
|---|---|---|---|
| 16 KiB | 130 MB/s | 9.9 MB/s (2,400 IOPS) | 1.15 ms |
| **64 KiB** | **167 MB/s** | **5.6 MB/s (1,400 IOPS)** | **0.91 ms** |
| 256 KiB | 225 MB/s | 2.3 MB/s (570 IOPS) | 0.97 ms |

A random 4 KiB write rewrites its whole chunk, which is also how much WAL it generates for Litestream to ship: 64 KiB chunks mean ~16x write amplification for 4 KiB random writes and ~1x for sequential. 64 KiB is the compromise. Two things made a bigger difference than chunk size: `chunks` as a rowid table rather than `WITHOUT ROWID` (3.4x on random writes, because SQLite's index seeks were copying neighbouring blob payloads), and preparing statements once per connection (3x on small transactions).

## With Litestream replicating (fio, 15s per job, 256 MiB file)

| workload | result | un-checkpointed WAL peak |
|---|---|---|
| sequential write, 1 MiB | 20 MB/s | 256 MiB (backpressure engaged) |
| random write, 4 KiB | 394 IOPS | 256 MiB (backpressure engaged) |
| random r/w 70/30, 4 KiB | 193 / 87 IOPS | 256 MiB |
| random read, 4 KiB | 396 IOPS | (draining previous job) |

With replication on, sustained write throughput is whatever Litestream can stage and upload, here ~20–30 MB/s of WAL against a MinIO on the same box: every byte written lands on the one virtio disk four times (WAL, database, staged LTX, MinIO object), so the `synchronous=OFF` change does not move the sequential number on this machine. Paired runs of the same jobs on this VM vary by 2x for random writes and more for mixed r/w because the host is shared, so compare binaries back to back rather than against this table. Isolated random 4 KiB writes with replication went from ~670 to ~1,300 IOPS with the fsync-free configuration. Bursts up to `--wal-limit` run at local speed; after that, writes block until Litestream checkpoints. The random-read number is depressed by Litestream and MinIO competing for the same six cores while draining 256 MiB of backlog; without replication the same read is ~2,900 IOPS.

Write amplification is the number to keep in mind when sizing the bucket link: a workload doing 400 random 4 KiB writes per second pushes ~28 MB/s of WAL, not 1.6 MB/s.

## WAL growth

SQLite reuses the WAL file from the beginning after a checkpoint but never shrinks it; the file size is a high-water mark. Litestream checkpoints PASSIVE after every sync once the WAL has at least 1,000 pages, and TRUNCATE once it passes ~121k pages (~1 GB at satchel's 8 KiB page size). Under sustained writes expect the WAL file to sit between a few hundred MB and 1 GB on disk. The meaningful number is un-checkpointed frames (`satchel_wal_bytes`), which satchel bounds at `--wal-limit`.
