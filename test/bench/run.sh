#!/usr/bin/env bash
set -euo pipefail
: "${SATCHEL_S3_ENDPOINT:=http://127.0.0.1:9000}"
: "${SATCHEL_S3_BUCKET:=satchel}"
: "${SATCHEL_S3_ACCESS_KEY:=minioadmin}"
: "${SATCHEL_S3_SECRET_KEY:=minioadmin}"
: "${SATCHEL_STATE_DIR:=/tmp/satchel-bench}"
: "${SATCHEL_SYNC_INTERVAL:=1s}"
: "${FIO_RUNTIME:=20}"
: "${FIO_SIZE:=256M}"
export SATCHEL_S3_ENDPOINT SATCHEL_S3_BUCKET SATCHEL_S3_ACCESS_KEY SATCHEL_S3_SECRET_KEY SATCHEL_STATE_DIR SATCHEL_SYNC_INTERVAL

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${SATCHEL_BIN:-$ROOT/satchel}"
[ -x "$BIN" ] || (cd "$ROOT" && go build -o "$BIN" ./cmd/satchel)
VOL="bench-$$"

"$BIN" mount "$VOL" --log-level warn > "$SATCHEL_STATE_DIR.mountout" 2>&1 &
MOUNT_PID=$!
cleanup() {
  kill -INT "$MOUNT_PID" 2>/dev/null || true
  wait "$MOUNT_PID" 2>/dev/null || true
  [ -n "${SAMPLER:-}" ] && kill "$SAMPLER" 2>/dev/null || true
}
trap cleanup EXIT
for _ in $(seq 1 100); do
  MNT="$(head -1 "$SATCHEL_STATE_DIR.mountout" 2>/dev/null || true)"
  [ -n "$MNT" ] && [ -d "$MNT" ] && break
  sleep 0.2
done
[ -d "${MNT:-}" ] || { echo "mount failed:"; cat "$SATCHEL_STATE_DIR.mountout"; exit 1; }
WAL="$SATCHEL_STATE_DIR/dbs/$VOL.db-wal"
echo "mounted $VOL at $MNT (wal: $WAL)"

pending_wal_bytes() {
  python3 - "$SATCHEL_STATE_DIR/dbs/$VOL.db-shm" <<'PY'
import struct, sys
try:
    h = open(sys.argv[1], 'rb').read(100)
    page = struct.unpack('<H', h[14:16])[0] or 65536
    mx, backfill = struct.unpack('<I', h[16:20])[0], struct.unpack('<I', h[96:100])[0]
    print(max(mx - backfill, 0) * (page + 24))
except Exception:
    print(0)
PY
}

sample_wal() {
  local peak=0
  while sleep 1; do
    s=$(pending_wal_bytes)
    [ "$s" -gt "$peak" ] && peak=$s
    echo "$peak" > "$SATCHEL_STATE_DIR.walpeak.$1.max"
  done
}

run_fio() {
  local name=$1; shift
  sample_wal "$name" & SAMPLER=$!
  fio --name="$name" --directory="$MNT" --size="$FIO_SIZE" --runtime="$FIO_RUNTIME" --time_based \
      --ioengine=psync --direct=0 --group_reporting --output-format=terse --terse-version=3 "$@" \
    | awk -F';' -v n="$name" '{printf "%-22s read %8.1f MiB/s %7d iops   write %8.1f MiB/s %7d iops\n", n, $7/1024, $8, $48/1024, $49}'
  kill "$SAMPLER"; SAMPLER=
  echo "   peak un-checkpointed WAL during run: $(( $(cat "$SATCHEL_STATE_DIR.walpeak.$name.max") / 1024 / 1024 )) MiB"
  rm -f "$MNT/$name".*
}

run_fio seqwrite-1m   --rw=write     --bs=1m
run_fio randwrite-4k  --rw=randwrite --bs=4k
run_fio randrw-4k     --rw=randrw    --bs=4k --rwmixread=70
run_fio randread-4k   --rw=randread  --bs=4k

echo "small files: extracting 2000 x 4 KiB from a tar"
SMALL="$SATCHEL_STATE_DIR.small"
rm -rf "$SMALL" && mkdir -p "$SMALL/small"
for i in $(seq 1 2000); do head -c 4096 /dev/urandom > "$SMALL/small/$i"; done
tar -C "$SMALL" -cf "$SMALL.tar" small
start=$(date +%s.%N)
tar -C "$MNT" -xf "$SMALL.tar"
end=$(date +%s.%N)
awk -v s="$start" -v e="$end" 'BEGIN { printf "   %.0f files/s\n", 2000 / (e - s) }'
rm -rf "$SMALL" "$SMALL.tar"

echo "WAL file size (high-water mark): $(( $(stat -c %s "$WAL" 2>/dev/null || echo 0) / 1024 / 1024 )) MiB"
echo "un-checkpointed WAL at end: $(( $(pending_wal_bytes) / 1024 / 1024 )) MiB"
echo "db size: $(( $(stat -c %s "$SATCHEL_STATE_DIR/dbs/$VOL.db") / 1024 / 1024 )) MiB"
