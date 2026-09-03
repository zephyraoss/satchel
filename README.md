# Satchel Block

Satchel Block provides portable Docker volumes for small clusters. A volume uses a local ext4 filesystem while mounted and replicates changed blocks to an S3-compatible bucket. When a writable service moves to another node, Satchel creates a sparse image and fetches its blocks from S3 as the filesystem reads them.

Satchel permits one writer per volume. It is not a shared filesystem.

## Status

The block format is new and not compatible with the former SQLite and Litestream format. There is no migration path because the old format had no production users.

By default, `fsync` and FUA writes wait until Satchel publishes the current generation to S3. A database can therefore treat a successful `fsync` as durable across node loss. `durability=async` treats `fsync` as an ordering barrier and does not sync the local image or wait for S3. If only the application or PostgreSQL process crashes, the still-running Satchel mount retains its writes. A Satchel process crash, forced restart, or host failure can lose every generation that has not reached S3. Clean Satchel shutdown publishes queued changes. The sync interval defaults to five seconds.

## How it works

Each mounted volume is an ext4 filesystem on a Linux NBD device. Satchel writes through to a sparse local image and records complete 4 KiB blocks in the current generation. At each sync interval, and on every filesystem flush in remote durability mode, Satchel:

1. Seals the current generation.
2. Compresses changed blocks into bounded segments.
3. Uploads any segment too large to keep inline.
4. Advances the volume head with a conditional S3 write that includes the new manifest when it fits within the bounded inline area.

The volume lease and published generation live in the same state object. A stale writer may upload unused objects, but it cannot advance the volume head after another node takes the lease. Backends that lack conditional writes, such as Garage, use `--s3-head=append`, which keeps the head as an append-only version log instead of one `If-Match` object. See [docs/format.md](docs/format.md#append-head) for the tradeoffs.

Satchel starts packing inline history into immutable manifest bundles at 16 KiB, with a 64 KiB hard limit. The state keeps one pointer to the newest bundle, and bundles link to older bundles. This keeps the mutable object bounded without adding a second S3 request to every small commit.

Satchel compacts the manifest chain after 128 generations. A checkpoint reuses existing immutable segments and records only the newest non-zero source for each block range. It does not scan or re-upload the image. Satchel acknowledges the flush that triggered it as soon as the delta is durable, then compacts the chain in the replication worker. Current restores stop at that checkpoint, while its parent link keeps older generations available for point-in-time recovery. The default history window is seven days. Garbage collection marks older objects first, waits 24 hours, then deletes anything that is still outside the retained history.

See [docs/format.md](docs/format.md) for the on-bucket format and failure rules.

## Requirements

- Linux with the `nbd` kernel module
- `mkfs.ext4` and `mount`
- An S3-compatible bucket with working `If-Match` and `If-None-Match` writes, or a bucket on a backend without them (Garage) run with `--s3-head=append`

Load enough NBD devices for the number of volumes that may be mounted on a node:

```sh
sudo modprobe nbd nbds_max=64
```

## Install

### Managed Docker plugin

Load the NBD module before installing the plugin. The bundled plugin manifest exposes `/dev/nbd0` through `/dev/nbd15`, so the managed plugin supports 16 simultaneous mounts per node.

```sh
sudo modprobe nbd nbds_max=16
./deploy/plugin/build.sh
docker plugin set zephyraoss/satchel:dev \
  SATCHEL_S3_ENDPOINT=http://storage-node:9000 \
  SATCHEL_S3_BUCKET=satchel \
  SATCHEL_S3_ACCESS_KEY=... SATCHEL_S3_SECRET_KEY=...
docker plugin enable zephyraoss/satchel:dev
```

### systemd

Install `e2fsprogs`, load `nbd`, then:

```sh
go build -o /usr/local/bin/satchel ./cmd/satchel
install -d /etc/satchel
cp deploy/systemd/satchel.env.example /etc/satchel/satchel.env
cp deploy/systemd/satchel.service /etc/systemd/system/
systemctl enable --now satchel
```

## Use

```sh
docker volume create --driver satchel -o size=10GiB app-data
docker run --rm -v app-data:/data alpine sh -c 'echo hello > /data/hi'
docker run --rm -v app-data:/data alpine cat /data/hi
```

With uncloud, use `driver: satchel` in the Compose volume definition.

### Volume options

| option | values | effect |
|---|---|---|
| `size` | byte count or `KiB`, `MiB`, `GiB` | Virtual volume size. Default `10GiB`, minimum `64MiB`. |
| `mode` | `rw`, `ro` | Read-only mounts restore a fixed snapshot and do not take a lease. |
| `scope` | `volume`, `replica` | Replica scope derives a separate volume name from each Docker mount ID. |
| `durability` | `remote`, `async` | `remote` makes `fsync` wait for S3 and is the default. `async` acknowledges flushes without local or remote stable storage and publishes in the background. |
| `seed` | directory, `.tar`, `.tgz`, or `s3://` URL | Copies initial files into a newly formatted volume. |
| `sync_interval` | Go duration | Overrides the five-second generation interval. |
| `filesystem` | `ext4` | Filesystem written into the block image. |

Volume size and filesystem cannot change after the first mount.

### Operator commands

```sh
satchel vol ls
satchel vol inspect app-data
satchel vol restore app-data ./app-data.img
satchel vol restore app-data ./before.img --timestamp 2026-08-31T12:00:00Z
satchel vol restore app-data ./generation-42.img --generation 42
satchel vol gc app-data
satchel vol lease status app-data
satchel vol lease break app-data
satchel vol rm app-data
```

`restore` writes a sparse block image. Mount it with a loop device if you need to inspect its files.

## Configuration

| flag | environment variable | default |
|---|---|---|
| `--node-id` | `SATCHEL_NODE_ID` | hostname |
| `--state-dir` | `SATCHEL_STATE_DIR` | `/var/lib/satchel` |
| `--s3-endpoint` | `SATCHEL_S3_ENDPOINT` | AWS S3 |
| `--s3-bucket` | `SATCHEL_S3_BUCKET` | required |
| `--s3-access-key` | `SATCHEL_S3_ACCESS_KEY` | `AWS_ACCESS_KEY_ID` |
| `--s3-secret-key` | `SATCHEL_S3_SECRET_KEY` | `AWS_SECRET_ACCESS_KEY` |
| `--s3-head` | `SATCHEL_S3_HEAD` | `conditional` |
| `--lease-ttl` | `SATCHEL_LEASE_TTL` | `30s` |
| `--sync-interval` | `SATCHEL_SYNC_INTERVAL` | `5s` |
| `--dirty-limit` | `SATCHEL_DIRTY_LIMIT` | `256MiB` |
| `--checkpoint-interval` | `SATCHEL_CHECKPOINT_INTERVAL` | `128` |
| `--history-retention` | `SATCHEL_HISTORY_RETENTION` | `168h` |
| `--gc-grace` | `SATCHEL_GC_GRACE` | `24h` |
| `--metrics-addr` | `SATCHEL_METRICS_ADDR` | disabled |

## Development

```sh
go test ./...
sudo modprobe nbd nbds_max=16
./test/e2e/run.sh
./test/bench/run.sh
./test/bench/postgres.sh remote
```

Unit tests do not require NBD, root, or S3. The end-to-end test needs all three.
