#!/usr/bin/env bash
set -euo pipefail
: "${SATCHEL_S3_ENDPOINT:=http://127.0.0.1:9000}"
: "${SATCHEL_S3_BUCKET:=satchel}"
: "${SATCHEL_S3_ACCESS_KEY:=minioadmin}"
: "${SATCHEL_S3_SECRET_KEY:=minioadmin}"
: "${SATCHEL_STATE_DIR:=/tmp/satchel-bench}"
: "${SATCHEL_SYNC_INTERVAL:=1s}"
: "${SATCHEL_DURABILITY:=async}"
: "${FIO_RUNTIME:=20}"
: "${FIO_SIZE:=256M}"
: "${FIO_DIRECT:=1}"
export SATCHEL_S3_ENDPOINT SATCHEL_S3_BUCKET SATCHEL_S3_ACCESS_KEY SATCHEL_S3_SECRET_KEY SATCHEL_STATE_DIR SATCHEL_SYNC_INTERVAL

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${SATCHEL_BIN:-$ROOT/satchel}"
[ -x "$BIN" ] || (cd "$ROOT" && go build -o "$BIN" ./cmd/satchel)
VOL="bench-$$"

"$BIN" mount "$VOL" --durability "$SATCHEL_DURABILITY" --log-level warn > "$SATCHEL_STATE_DIR.mountout" 2>&1 &
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
echo "mounted $VOL at $MNT"

run_fio() {
  local name=$1; shift
  fio --name="$name" --directory="$MNT" --size="$FIO_SIZE" --runtime="$FIO_RUNTIME" --time_based \
      --ioengine=psync --direct="$FIO_DIRECT" --refill_buffers=1 --randrepeat=0 --group_reporting --output-format=terse --terse-version=3 "$@" \
    | awk -F';' -v n="$name" '{printf "%-22s read %8.1f MiB/s %7d iops   write %8.1f MiB/s %7d iops\n", n, $7/1024, $8, $48/1024, $49}'
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

echo "sparse image allocated: $(du -h "$SATCHEL_STATE_DIR/images/$VOL.img" | cut -f1)"
