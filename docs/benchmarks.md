# Benchmarks

These results compare the last SQLite and Litestream revision, the first block
revision, and the current Satchel Block implementation.

They are development-machine results, not production sizing guidance. Each
number is one 10-second fio sample. The tests use `durability=async`, so they
measure filesystem throughput while replication runs in the background. They
do not measure database commit latency with S3-backed durability. PostgreSQL
results appear after the fio sections.

## Test machine

The tests ran on 2026-08-31 with this setup:

- 6 vCPUs and 11 GiB RAM
- Linux 6.19.10 on x86-64
- fio 3.40 with one `psync` job and buffered I/O
- 256 MiB test files and a 10-second runtime
- local Satchel state on tmpfs
- MinIO `RELEASE.2025-09-07T16-13-09Z` on the loopback interface
- SQLite and Litestream revision `9485320`
- initial block revision `8a827d6`
- current Satchel Block worktree based on `8a827d6`

The small-file test extracts 2,000 files of 4 KiB each. The mixed test uses 70%
reads and 30% writes with 4 KiB operations.

## Replication running every second

This is the full `test/bench/run.sh` suite with a one-second sync interval and
the default 256 MiB unpublished-data limit. Every implementation used the same
local MinIO endpoint. Satchel Block used asynchronous durability.

| Workload | SQLite and Litestream | Initial block | Satchel Block | Block vs SQLite |
|---|---:|---:|---:|---:|
| Sequential 1 MiB write | 35.6 MiB/s | 33.3 MiB/s | 37.9 MiB/s | 1.06x |
| Random 4 KiB write | 2,556 IOPS | 9,178 IOPS | 8,225 IOPS | 3.22x |
| Mixed 4 KiB read | 2,038 IOPS | 12,977 IOPS | 12,883 IOPS | 6.32x |
| Mixed 4 KiB write | 864 IOPS | 5,541 IOPS | 5,570 IOPS | 6.45x |
| Random 4 KiB read | 4,363 IOPS | 15,676 IOPS | 13,555 IOPS | 3.11x |
| Create 4 KiB files | 655 files/s | 33,255 files/s | 27,127 files/s | 41.42x |

The script runs these jobs one after another on one mount. Upload work left by
one job can overlap the next job, so small differences between the two block
columns are noise until a multi-run benchmark on dedicated hardware confirms
them. The comparison with the SQLite backend is large enough to be useful.

## Buffered write-path ceiling

This pass used a one-hour sync interval and a 2 GiB unpublished-data limit to
keep remote publication out of the timed section. It measures the local path,
not replication throughput. The current worktree completed the two write
workloads twice before the test host was reset, so its cells show the observed
range.

| Workload | SQLite and Litestream | Initial block | Satchel Block | Block vs SQLite |
|---|---:|---:|---:|---:|
| Sequential 1 MiB write | 147.9 MiB/s | 371.9 MiB/s | 454.4 to 557.3 MiB/s | 3.07x to 3.77x |
| Random 4 KiB write | 2,039 IOPS | 81,582 IOPS | 99,810 to 103,614 IOPS | 48.95x to 50.82x |

The current block changes improved sequential writes by 22% to 50% and random
writes by 22% to 27% over the first block revision in these samples.

## Restore and checkpoint behavior

Writable restores are lazy. Mount preparation reads the manifest chain and
creates a sparse image, but it does not download segment bodies. The first read
of a remote-backed block fetches and verifies its segment. Full-block writes do
not fetch the old segment. `TestLazyRestoreFetchesOnReadAndPreservesOverwrites`
checks these properties, including retry behavior and partial-block writes.

Checkpoints are metadata-only. They select the newest segment reference for
each live extent, then publish one checkpoint manifest and `state.json`.
Checkpointing does not scan the image or upload segment data.
`TestMetadataCheckpointReusesSegments` checks that the segment object count
does not change.

The replica package also has an in-memory microbenchmark with 128 generations
and 128 twenty-block segment objects. These results came from the 4-vCPU
benchmark host. Each row is the median of three samples with 10 iterations per
sample.

| Operation | Median | Observed range | Segment GETs |
|---|---:|---:|---:|
| Metadata checkpoint of 128 deltas | 0.277 ms | 0.268 to 0.287 ms | 0 |
| Prepare lazy restore | 1.29 ms | 0.722 to 1.36 ms | 0 |
| Materialize the same restore | 14.3 ms | 14.0 to 14.6 ms | 128 |

The in-memory object store removes network latency, so these times compare code
paths rather than S3 implementations. The checkpoint uses its in-memory extent
map and performs no manifest or segment GETs. Lazy preparation reads metadata
but no segment bodies. Materialization fetches every segment.

The scale-100 PostgreSQL volume produced a less friendly checkpoint: a 4.3 MiB
manifest with 1,822 segment references and 138,715 extents. The old planner
sorted the growing extent set once per reference and took 13.52 seconds to
prepare a lazy image. Batched validation and one merge per manifest reduced
that to 0.327 seconds on the same volume and manifest. A second mount measured
0.333 seconds. Both mounts left segment bodies remote until the first read.

Run the microbenchmark with:

```sh
go test ./internal/replica -run '^$' \
  -bench 'Benchmark(MetadataCheckpoint128|RestorePreparation128)$' \
  -benchtime=10x -count=3
```

## Reproducing the current suite

Load the NBD module, start an S3-compatible endpoint, then run:

```sh
sudo modprobe nbd nbds_max=16
SATCHEL_S3_ENDPOINT=http://127.0.0.1:9000 \
SATCHEL_S3_BUCKET=satchel \
SATCHEL_S3_ACCESS_KEY=minioadmin \
SATCHEL_S3_SECRET_KEY=minioadmin \
SATCHEL_SYNC_INTERVAL=1s \
SATCHEL_DURABILITY=async \
FIO_RUNTIME=10 \
FIO_SIZE=256M \
FIO_DIRECT=0 \
./test/bench/run.sh
```

For database durability, rerun with `SATCHEL_DURABILITY=remote` and a workload
that calls `fsync` for each transaction. Record p50, p95, and p99 commit
latency. That result depends mainly on S3 request latency, so use the same S3
placement that production will use.

## PostgreSQL on the benchmark host

These results use a dedicated benchmark host with 4 vCPUs, 8 GiB RAM, and one
100 GiB btrfs filesystem using zstd compression. Each mode used a separately
initialized scale-100 database. The native control ran on 2026-09-01 UTC. The
async control and final remote run ran on 2026-09-02 UTC. The final remote run
started at 01:30:59 and finished at 01:38:05. The benchmark harness also writes
its exact UTC start and finish times to `environment.txt`. MinIO
`RELEASE.2025-09-07T16-13-09Z` used the same disk and the loopback interface.
Remote latency is optimistic for networking, while its disk contention is
worse than a separate S3 service.

The host exposes one SSD-backed virtual disk, not NVMe. Its native one-client
result was 55 TPS, so the native column is a same-host control, not a general
SSD or NVMe estimate. On storage capable of 1,000 or more durable commits per
second, native and async may be much closer. Do not use these large ratios as
general product claims.

The block runs used separate 20 GiB scale-100 volumes. The native control used
an equivalent freshly initialized scale-100 data directory. PostgreSQL 18.4
enabled data checksums, `fsync`, synchronous commits, and full-page writes. It
used 1 GiB shared buffers. pgbench used prepared statements, a 30-second
warmup, a 60-second measured run, and a 10% latency sample. Satchel used a
one-second sync interval, a 2 GiB unpublished-data limit, and a 128-generation
checkpoint interval. The async rows used binary SHA-256
`b16f6a782af4f62ee61536427fe3ef1a91522f111e11ea809dd47f09df5b8cac`.
The remote rows used
`5e01914f919da202004d11b25f1abcb98fb8e20d226c8d3460bce99843922028`.

| Mode | Clients | TPS | Average ms | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|---:|---:|
| native | 1 | 55.386 | 18.051 | 17.897 | 21.535 | 29.154 |
| async | 1 | 1,516.639 | 0.658 | 0.605 | 0.990 | 1.452 |
| remote | 1 | 18.645 | 53.629 | 48.255 | 76.584 | 155.042 |
| native | 8 | 231.561 | 34.508 | 32.821 | 51.476 | 71.624 |
| async | 8 | 3,912.648 | 2.040 | 1.361 | 4.674 | 11.296 |
| remote | 8 | 73.266 | 109.118 | 102.129 | 147.575 | 265.731 |
| native | 32 | 790.205 | 40.389 | 34.780 | 72.820 | 106.262 |
| async | 32 | 3,431.157 | 9.277 | 5.804 | 28.991 | 59.498 |
| remote | 32 | 206.489 | 154.663 | 128.852 | 305.388 | 396.184 |

Async delivered 27.4, 16.9, and 4.34 times native throughput at the three
client counts because it does not wait for local or remote stable storage. A
PostgreSQL crash leaves the mounted image intact. A Satchel crash, forced
restart, or host failure can lose every generation not yet published to S3.
The expected window is about one second with this configuration, but an S3
outage can make it longer until the dirty-data limit stops writers.

A separate fresh scale-10 diagnostic at 32 clients measured 6,244.60 TPS with
the serial NBD path, 6,445.14 TPS with an eight-request queue, and 5,953.75 TPS
with a 64-request queue. Shallow request parallelism gained 3.2%, while the
deeper queue regressed 4.7% through lock contention. Satchel therefore keeps
the serial path for async durability. The lower scale-100 result includes the
cost of capturing, compressing, and uploading a much larger working set every
second; it is not a fixed NBD throughput ceiling.

Remote delivered 33.7%, 31.6%, and 26.1% of native throughput. In the earlier
two-PUT build, one-client stage metrics averaged 1.12 ms for encoding, 0.12 ms
for segment publication, 21.90 ms for the immutable manifest PUT, and 30.85 ms
for the conditional `state.json` PUT. The final build embeds each small
manifest in the fenced state update. Its one-client metrics averaged 1.12 ms
for encoding, 0.06 ms for the segment stage, and 45.07 ms for the state update.
Background manifest-bundle writes averaged 24.74 ms.

Removing one request did not double throughput on this host. The remaining
state PUT carries recent manifest data, and background bundle writes contend
with it on the same MinIO and btrfs disk. A lower-latency S3 endpoint will
matter more than changes to the local block path.

### Single-request small commits

This comparison isolates the new inline-manifest path from the preceding
two-PUT build. Both builds include remote flush batching and use separate,
fresh scale-100 databases.

| Clients | Two-PUT TPS | One-PUT TPS | Throughput change | Two-PUT p50 ms | One-PUT p50 ms |
|---:|---:|---:|---:|---:|---:|
| 1 | 16.498 | 18.645 | +13.0% | 53.427 | 48.255 |
| 8 | 67.288 | 73.266 | +8.9% | 111.157 | 102.129 |
| 32 | 210.724 | 206.489 | -2.0% | 123.225 | 128.852 |

At 32 clients, the throughput difference is small enough to be one-run noise.
The sampled p99 rose from 101.782 to 155.042 ms at one client, from 228.879 to
265.731 ms at eight clients, and from 356.514 to 396.184 ms at 32 clients.
Background bundle I/O can delay a state write on this single-disk setup. These
tails are high enough to affect an interactive application that commits once
per request. Size remote durability against p99, not only throughput or median
latency.

The obvious alternative, publishing a deterministic generation key with
`If-None-Match: *`, is not safe. It proves only that the key did not exist. It
does not prove that the writer still owns the lease after another node takes
over. Embedding the manifest in the `If-Match` state update preserves that
fence. Satchel moves accumulated inline history to immutable bundles in the
background, starts at 16 KiB, and enforces a 64 KiB hard limit.

### Remote flush batching experiment

The unbatched block build waited for each remote flush in the NBD request loop.
The batched build seals a fixed generation at each flush boundary, continues
serving later local I/O while S3 is in flight, and merges generations already
waiting for publication. Every acknowledged flush still includes all writes
that preceded it in NBD request order. This experiment predates inline
manifests and isolates batching from the one-request change above.

Both sides of this comparison used separately initialized scale-100 databases
and the settings above. The unbatched binary was
`4e6801565f72f5ba6edd33e35d559158abb9b1fd115b44a62ce764b89c0d4b58`;
the batched two-PUT binary was
`b16f6a782af4f62ee61536427fe3ef1a91522f111e11ea809dd47f09df5b8cac`.

| Clients | Unbatched TPS | Batched TPS | Throughput change | Unbatched p50 ms | Batched p50 ms |
|---:|---:|---:|---:|---:|---:|
| 1 | 16.215 | 16.498 | +1.7% | 55.368 | 53.427 |
| 8 | 53.650 | 67.288 | +25.4% | 154.081 | 111.157 |
| 32 | 174.932 | 210.724 | +20.5% | 175.328 | 123.225 |

The 32-client p95 fell from 341.125 ms to 266.677 ms, and p99 fell from
434.085 ms to 356.514 ms. A fixed 1 ms batching delay was also tested and
discarded. It shifted throughput between client counts without consistently
increasing batch size, so the final code batches only requests already queued.

### Change from the pre-optimization block build

The earlier block implementation used the same host and pgbench settings. It
synced the disposable local image on every flush, uploaded each generation as
one object, and restored every segment before mounting.

| Mode | Clients | Earlier TPS | Current TPS | Throughput change | Earlier p50 ms | Current p50 ms |
|---|---:|---:|---:|---:|---:|---:|
| async | 1 | 54.147 | 1,516.639 | 28.01x | 16.259 | 0.605 |
| async | 8 | 229.130 | 3,912.648 | 17.08x | 32.019 | 1.361 |
| async | 32 | 687.056 | 3,431.157 | 4.99x | 35.896 | 5.804 |
| remote | 1 | 10.166 | 18.645 | 1.83x | 87.065 | 48.255 |
| remote | 8 | 42.152 | 73.266 | 1.74x | 175.563 | 102.129 |
| remote | 32 | 143.091 | 206.489 | 1.44x | 182.012 | 128.852 |

The async gain comes mainly from removing `fdatasync` on the disposable image.
Remote gained from embedding compressed segments up to 64 KiB in the manifest,
parallel work for larger generations, lower S3 request overhead, batching
flush generations that are already waiting, and carrying small manifests in
the conditional head update. The final code keeps immutable historical
manifests and a conditional head, so acknowledged commits do not rely on the
disposable local image.

Every reported run processed zero failed transactions and passed PostgreSQL's
immediate-stop recovery check. Both block modes then unmounted, restored lazily
into a new state directory, completed a read workload, returned no corruption
from `pg_amcheck`, unmounted again, and released the volume lease.

`test/bench/postgres.sh` reproduces one mode:

```sh
SATCHEL_BIN=./satchel \
SATCHEL_S3_ENDPOINT=http://127.0.0.1:9000 \
SATCHEL_S3_BUCKET=satchel \
SATCHEL_PGBENCH_ROOT=/var/lib/satchel-pgbench \
SATCHEL_PGBENCH_RESULTS=/var/lib/satchel-pgbench/results \
SATCHEL_PGBENCH_RUN_ID=pg18-current \
./test/bench/postgres.sh remote
```

The raw pgbench output, latency samples, mount-owner checks, and recovery logs
for these runs are outside the repository. Record the S3 endpoint placement and
keep those artifacts with any published result.
