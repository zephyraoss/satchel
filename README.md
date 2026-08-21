# satchel

`satchel` allows for portable docker volumes for simple Docker clusters (built for [uncloud](https://github.com/psviderski/uncloud)). A satchel volume will follow your service from node to node allowing simple migrations of apps between nodes.

`satchel` volumes are stored in SQLite databases. While a volume is mounted, [Litestream](https://litestream.io) will replicate it to a remote (for example, an S3 bucket). When a service lands on another node, satchel restores from the remote before the container starts. A lease object in the same bucket guarantees that only one node can have a volume mounted at a time.

## Read this before using it

**This is not a shared volume.** satchel does not support multiple writers. If a second node tries to mount a volume that is already mounted, the mount fails with `volume X is held by node Y`. If you need to scale past one replica, you either need to set your volume as read-only or use `scope=replica` to give each scaled container its own volume.

**Not for databases / heavy workloads.** Postgres, MySQL and other heavy workloads will likely run poorly via satchel. satchel is built for configuration, application state, uploads, caches, and small working sets.

## Install

You need Litestream ≥ 0.5 on the node (or use the managed plugin image, which bundles it) and an S3 bucket on a storage node reachable by every Docker host.

### Managed plugin

```sh
./deploy/plugin/build.sh
docker plugin set zephyraoss/satchel:dev \
  SATCHEL_S3_ENDPOINT=http://storage-node:9000 \
  SATCHEL_S3_BUCKET=satchel \
  SATCHEL_S3_ACCESS_KEY=... SATCHEL_S3_SECRET_KEY=...
docker plugin enable zephyraoss/satchel:dev
```

### systemd

```sh
go build -o /usr/local/bin/satchel ./cmd/satchel
install -d /etc/satchel && cp deploy/systemd/satchel.env.example /etc/satchel/satchel.env   # edit it
cp deploy/systemd/satchel.service /etc/systemd/system/
systemctl enable --now satchel
```

The plugin listens on `/run/docker/plugins/satchel.sock`; Docker discovers it on the next volume operation.

## Use

```sh
docker volume create --driver satchel app-data
docker run -v app-data:/data alpine sh -c 'echo hello > /data/hi'
# redeploy anywhere in the cluster
docker run -v app-data:/data alpine cat /data/hi
```

With uncloud, reference the volume with `driver: satchel` in your compose file; `uc deploy` moves the service, satchel moves the data.

### Volume options

```sh
docker volume create --driver satchel -o mode=ro shared-config     # read-only copy, no lease, safe to mount on N nodes
docker volume create --driver satchel -o scope=replica cache        # each container gets its own volume
docker volume create --driver satchel -o seed=/srv/defaults app     # first mount of an empty volume imports this dir
docker volume create --driver satchel -o seed=s3://bkt/app.tgz app  # ...or a tar / tar.gz from any bucket your creds can read
docker volume create --driver satchel -o sync_interval=1s hot       # per-volume Litestream sync interval
```

| option | values | effect |
|---|---|---|
| `mode` | `rw` (default), `ro` | `ro` restores the latest state, mounts read-only (`EROFS` on writes), takes no lease and runs no replication. Many nodes can mount it at once. The copy is as of mount time; remount to refresh. |
| `scope` | `volume` (default), `replica` | `replica` gives each mount request its own volume named `<name>.r-<hash of mount id>`, so a scaled service never trips over the lease. The data is tied to that container; a redeployed container starts a new one. Old ones stay in the bucket until you remove them with `satchel vol` or `docker volume rm`. |
| `seed` | local dir, `.tar`/`.tgz` file, `s3://bucket/key.tgz` | Imported once, when the volume is mounted for the first time and the bucket has nothing for it. Never re-applied. |
| `sync_interval` | duration | Overrides `--sync-interval` for this volume. |
| `class` | `fuse` | Reserved for a future block backend. |

### Operator CLI

`satchel vol` needs only the S3 credentials and a `litestream` binary; it restores a private snapshot into a temp dir and works on that.

```sh
satchel vol ls                                  # volumes in the bucket, lease holder, expiry
satchel vol ls-files -r app                      # mode, size, owner, mtime, path
satchel vol cat app config/app.yml
satchel vol put app ./local.yml config/app.yml   # or `-` for stdin
satchel vol edit app config/app.yml              # $EDITOR; holds the lease while you edit
satchel vol sql app "select count(*) from inodes"
satchel vol sql --write app "delete from xattrs"
satchel vol restore app ./dump --timestamp 2026-08-21T10:00:00Z
satchel vol lease status app
satchel vol lease break app                      # asks you to type the holder's name
```

Reads work any time. Writes (`put`, `edit`, `sql --write`) take the lease first and fail with `volume X is held by node Y` if a node has it mounted. A successful write is a normal Litestream transaction on the same lineage, so the next mount anywhere sees it.

## How the lease works

`s3://<bucket>/leases/<vol>.json` holds `{holder, token, expires_at, mounted_at}`. Mount acquires it with a conditional PUT (`If-None-Match: *` for a fresh key, `If-Match: <etag>` to take over an expired one) and then heartbeats every `ttl/3` (default TTL 30s). If the heartbeat fails three times in a row, or discovers that someone else now holds the lease, the node fences itself: the mount is abandoned, nothing is replicated, and the volume is marked `fenced` until all containers using it have unmounted. Unmount packs, syncs, and then deletes the lease only if the token still matches.

An operator can break a stuck lease by deleting the object (a `satchel vol lease break` command arrives in v2). Do this only when you are certain the holder is dead.

## Configuration

Every flag has an env-var twin; see `satchel plugin --help`.

| flag | env | default |
|---|---|---|
| `--node-id` | `SATCHEL_NODE_ID` | hostname |
| `--state-dir` | `SATCHEL_STATE_DIR` | `/var/lib/satchel` |
| `--s3-endpoint` | `SATCHEL_S3_ENDPOINT` | (AWS) |
| `--s3-bucket` | `SATCHEL_S3_BUCKET` | required |
| `--s3-access-key` / `--s3-secret-key` | `SATCHEL_S3_ACCESS_KEY` / `SATCHEL_S3_SECRET_KEY` | `AWS_*` |
| `--s3-path-style` | `SATCHEL_S3_PATH_STYLE` | `true` |
| `--lease-ttl` | `SATCHEL_LEASE_TTL` | `30s` |
| `--sync-interval` | `SATCHEL_SYNC_INTERVAL` | `5s` |
| `--backend` | `SATCHEL_BACKEND` | `fuse` |
| `--wal-limit` | `SATCHEL_WAL_LIMIT` | `256MiB` |
| `--metrics-addr` | `SATCHEL_METRICS_ADDR` | disabled |
| `--litestream` | `SATCHEL_LITESTREAM_BIN` | `litestream` |

## Development

```sh
go test ./...            # unit tests; FUSE tests need /dev/fuse and skip otherwise
./test/e2e/run.sh        # two simulated nodes against MinIO + real litestream (needs docker, litestream on PATH)
./test/bench/run.sh      # fio against a live mount with replication running (needs fio, MinIO)
go test ./internal/backend/fuse/ -bench . -run xxx
```

Without Docker, point the e2e suite at any MinIO: `SATCHEL_E2E_S3_ENDPOINT=http://127.0.0.1:9000 go test ./test/e2e/`.

See `docs/` for workload guidance and the roadmap.
