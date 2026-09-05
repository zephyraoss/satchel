# satchel-bench storage report

Tested on 2026-09-04 UTC. Raw fio JSON is stored on the server at:

```text
/var/lib/satchel-pgbench/results/disk-perf-20260904-v1
```

## Summary

The disk has decent throughput and scales well with queued I/O. Its weak point
is durable-write latency. A single 4 KiB write followed by `fdatasync` takes
10.48 ms on average in the NOCOW directory. That limits a serial workload to
about 95 durable operations per second before Satchel or PostgreSQL does any
other work.

This is reasonable for a replicated Ceph volume backed by SAS SSDs. It would be
slow for a directly attached SSD and far behind power-loss-protected NVMe. The
guest cannot see Ceph pool, network, or OSD timing, so this test cannot separate
those parts of the flush path.

Satchel is not the main source of this limit. Its benchmark reached 75.1 TPS
with one PostgreSQL client, while native PostgreSQL reached 55.4 TPS on the
same virtual disk. Higher concurrency lets both PostgreSQL and Satchel combine
work across storage barriers.

## Host and storage

| Item | Value |
|---|---|
| Host | `satchel-bench`, KVM virtual machine |
| CPU and memory | 4 vCPUs, 7.7 GiB RAM |
| Guest disk | One 100 GiB virtio disk, `/dev/vda` |
| Filesystem | btrfs, `compress=zstd:1`, `discard=async` |
| Guest cache report | Write-back |
| Guest rotational report | Rotational |
| Scheduler | `none` |
| Queue size | 256 requests |
| Physical storage | Ceph backed by SAS SSDs, as reported by the operator |
| fio | 3.40 |

The test ran while Satchel, MinIO, PostgreSQL, and pgbench were stopped. Load
average before the run was 0.14, 0.12, 0.36. btrfs reported zero read, write,
flush, corruption, and generation errors. The kernel logged no disk or btrfs
errors during the run.

## Durable writes

These tests use buffered writes because that matches Satchel's journal. Each
operation writes one block and calls `fdatasync`. The Q1 results are the average
of three runs. Percentiles are averaged across those runs.

| Workload | Durable ops/s | Sync mean | Sync p50 | Sync p95 | Sync p99 |
|---|---:|---:|---:|---:|---:|
| 4 KiB, compressed btrfs, Q1 | 92.5 | 10.79 ms | 10.51 ms | 13.04 ms | 17.63 ms |
| 4 KiB, NOCOW btrfs, Q1 | 95.2 | 10.48 ms | 10.20 ms | 13.00 ms | 16.34 ms |
| 4 KiB, NOCOW btrfs, 4 callers | 211.9 | 18.85 ms | 16.91 ms | 27.39 ms | 33.82 ms |
| 64 KiB, NOCOW btrfs, Q1 | 85.5 | 11.61 ms | 11.21 ms | 14.62 ms | 20.58 ms |
| 4 KiB, tmpfs, Q1 | 188,676 | 0.001 ms | 0.001 ms | 0.001 ms | 0.001 ms |

NOCOW improved the repeated 4 KiB test by 3%. It is still worth using because
it avoids copy-on-write fragmentation as the journal grows, but it does not
remove the Ceph flush cost. Four independent callers produced 212 durable
operations per second, only 2.2 times the Q1 result. The backend becomes slower
per request as the number of concurrent barriers rises.

An `O_SYNC` variant reached 96.9 operations per second with 10.32 ms mean write
latency. Saving the separate `fdatasync` syscall would therefore make little
difference. Satchel's group commit is more useful because one barrier can cover
several journal records.

## Direct I/O

These tests bypass the guest page cache. They measure the virtual disk without
claiming that each write has reached stable storage.

| Workload | IOPS | Throughput | Mean | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|---:|
| 4 KiB random read, Q1 | 943 | 3.7 MiB/s | 1.04 ms | 1.00 ms | 1.22 ms | 3.10 ms |
| 4 KiB random read, Q32 | 35,766 | 139.7 MiB/s | 0.88 ms | 0.67 ms | 1.70 ms | 5.93 ms |
| 4 KiB random write, Q1 | 323 | 1.3 MiB/s | 3.06 ms | 3.03 ms | 3.49 ms | 5.41 ms |
| 4 KiB random write, Q32 | 13,891 | 54.3 MiB/s | 2.29 ms | 2.11 ms | 3.46 ms | 5.21 ms |

Queue depth helps a lot. This fits a distributed block device that can process
many requests in parallel but pays substantial latency for one isolated
request.

The 70% read mixed Q32 test delivered 12,493 total IOPS and 48.8 MiB/s. Reads
averaged 1.51 ms with a 10.55 ms p99. Writes averaged 4.98 ms with a 24.77 ms
p99.

## Sequential I/O

| Workload | Throughput |
|---|---:|
| 1 MiB sequential read, Q32 | 253.9 MiB/s |
| 1 MiB sequential write, Q32 | 240.1 MiB/s |

This is adequate for bulk restore and replication work. It is closer to a
single SAS SSD or a capped virtual volume than an NVMe-backed volume.

## What this means for Satchel

Satchel's local mode must complete one stable-storage barrier before it can
acknowledge an isolated filesystem flush. At the measured 10.48 ms mean, no
software implementation can exceed about 95 serial durable barriers per second
on this volume. The old unsafe async result avoided that barrier, which
is why it cannot be a target for a durable mode on this storage.

The current journal batching helps concurrent workloads. It cannot help a
single client that waits for each commit before issuing the next one. Replacing
`write` plus `fdatasync` with one synchronous write also has little value here,
as the `O_SYNC` test was only 1.7% faster.

## Recommendations

1. Put `SATCHEL_JOURNAL_DIR` on directly attached, power-loss-protected NVMe if
   low commit latency matters. Keep it persistent across a Satchel restart.
2. Keep the journal NOCOW on btrfs. The short test showed only a 3% gain, but it
   avoids long-term copy-on-write fragmentation.
3. Ask the Ceph operator for `ceph osd perf`, pool replica settings, network
   latency, and the location of each OSD's BlueStore DB and WAL. The guest
   cannot inspect those values.
4. Avoid placing the journal, PostgreSQL working set, and MinIO data on the same
   virtual disk in production. They otherwise compete for the same Ceph queue.
5. Judge database capacity at the intended client count. This disk is weak at
   serial durable writes but handles queued I/O much better.

## Method

The suite used a 4 GiB NOCOW test file for direct I/O. Direct random and
sequential workloads ran for 15 seconds after a 3-second ramp. Durable Q1 tests
ran for 10 to 20 seconds after a 2 to 3-second ramp. Random buffers prevented
btrfs compression from turning writes into sparse or highly compressible data.

All test files lived under `/var/lib/satchel-disk-perf-v1` or `/dev/shm` and
were removed after the run. Existing Satchel images, journals, PostgreSQL data,
and benchmark results were not modified.
