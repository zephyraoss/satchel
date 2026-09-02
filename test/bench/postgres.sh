#!/usr/bin/env bash
set -euo pipefail

MODE=${1:-}
case "$MODE" in
  native|async|remote) ;;
  *) echo "usage: $0 native|async|remote" >&2; exit 2 ;;
esac

: "${SATCHEL_BIN:=./satchel}"
: "${SATCHEL_S3_ENDPOINT:=http://127.0.0.1:9000}"
: "${SATCHEL_S3_BUCKET:=satchel}"
: "${SATCHEL_S3_ACCESS_KEY:=minioadmin}"
: "${SATCHEL_S3_SECRET_KEY:=minioadmin}"
: "${SATCHEL_PGBENCH_ROOT:=/var/lib/satchel-pgbench}"
: "${SATCHEL_PGBENCH_RESULTS:=$SATCHEL_PGBENCH_ROOT/results}"
: "${SATCHEL_PGBENCH_RUN_ID:=$(date -u +%Y%m%dT%H%M%SZ)}"
: "${SATCHEL_PGBENCH_SCALE:=100}"
: "${SATCHEL_PGBENCH_CLIENTS:=1 8 32}"
: "${SATCHEL_PGBENCH_WARMUP:=30}"
: "${SATCHEL_PGBENCH_RUNTIME:=60}"
: "${SATCHEL_PGBENCH_SAMPLE_RATE:=0.1}"
: "${SATCHEL_PGBENCH_START_TIMEOUT:=300}"
: "${SATCHEL_PGBENCH_MOUNT_TIMEOUT:=300}"
: "${SATCHEL_PGBENCH_VOLUME_SIZE:=20GiB}"
: "${SATCHEL_PGBENCH_EXISTING_VOLUME:=}"
: "${SATCHEL_SYNC_INTERVAL:=1s}"
: "${SATCHEL_DIRTY_LIMIT:=2GiB}"
: "${SATCHEL_CHECKPOINT_INTERVAL:=128}"
: "${SATCHEL_METRICS_ADDR:=127.0.0.1:19100}"

started_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)

case_root="$SATCHEL_PGBENCH_ROOT/$MODE-$SATCHEL_PGBENCH_RUN_ID"
result_dir="$SATCHEL_PGBENCH_RESULTS/$MODE-$SATCHEL_PGBENCH_RUN_ID"
socket_dir="$case_root/socket"
port=55432
volume=${SATCHEL_PGBENCH_EXISTING_VOLUME:-pgbench-$MODE-$SATCHEL_PGBENCH_RUN_ID}
satchel_pid=
satchel_stop_count=0
pg_running=0
mountpoint=
pgdata=

if [[ -e "$case_root" || -e "$result_dir" ]]; then
  echo "benchmark run already exists: $MODE-$SATCHEL_PGBENCH_RUN_ID" >&2
  exit 1
fi
if [[ "$MODE" == native && -n "$SATCHEL_PGBENCH_EXISTING_VOLUME" ]]; then
	echo "SATCHEL_PGBENCH_EXISTING_VOLUME requires async or remote mode" >&2
	exit 2
fi
install -d -m 0755 "$SATCHEL_PGBENCH_ROOT" "$SATCHEL_PGBENCH_RESULTS"
install -d -m 0755 "$case_root" "$result_dir" "$socket_dir"
chown postgres:postgres "$socket_dir"

stop_postgres() {
  if [[ "$pg_running" == 1 ]]; then
		runuser -u postgres -- pg_ctl -D "$pgdata" -t "$SATCHEL_PGBENCH_START_TIMEOUT" -m fast -w stop >/dev/null 2>&1 || \
			runuser -u postgres -- pg_ctl -D "$pgdata" -t "$SATCHEL_PGBENCH_START_TIMEOUT" -m immediate -w stop >/dev/null 2>&1 || true
    pg_running=0
  fi
}

stop_satchel() {
  if [[ -n "$satchel_pid" ]] && kill -0 "$satchel_pid" 2>/dev/null; then
    satchel_stop_count=$((satchel_stop_count + 1))
    if [[ -n "$mountpoint" && -d "$mountpoint" ]]; then
      lsof +f -- "$mountpoint" >"$result_dir/mount-users-$satchel_stop_count.txt" 2>&1 || true
      fuser -vm "$mountpoint" >>"$result_dir/mount-users-$satchel_stop_count.txt" 2>&1 || true
      sync -f "$mountpoint"
    fi
    kill -INT "$satchel_pid" 2>/dev/null || true
    wait "$satchel_pid" || true
  fi
  satchel_pid=
}

cleanup() {
  stop_postgres
  stop_satchel
}
trap cleanup EXIT

start_satchel() {
  local state_dir=$1
  local log_file=$2
  install -d -m 0700 "$state_dir"
  env \
    SATCHEL_S3_ENDPOINT="$SATCHEL_S3_ENDPOINT" \
    SATCHEL_S3_BUCKET="$SATCHEL_S3_BUCKET" \
    SATCHEL_S3_ACCESS_KEY="$SATCHEL_S3_ACCESS_KEY" \
    SATCHEL_S3_SECRET_KEY="$SATCHEL_S3_SECRET_KEY" \
    SATCHEL_STATE_DIR="$state_dir" \
	SATCHEL_SYNC_INTERVAL="$SATCHEL_SYNC_INTERVAL" \
	SATCHEL_DIRTY_LIMIT="$SATCHEL_DIRTY_LIMIT" \
	SATCHEL_CHECKPOINT_INTERVAL="$SATCHEL_CHECKPOINT_INTERVAL" \
	SATCHEL_METRICS_ADDR="$SATCHEL_METRICS_ADDR" \
    "$SATCHEL_BIN" mount "$volume" \
      --size "$SATCHEL_PGBENCH_VOLUME_SIZE" \
      --durability "$MODE" \
      --log-level warn >"$log_file" 2>&1 &
  satchel_pid=$!
  mountpoint=
	for _ in $(seq 1 $((SATCHEL_PGBENCH_MOUNT_TIMEOUT * 10))); do
    mountpoint=$(head -1 "$log_file" 2>/dev/null || true)
    if [[ -d "$mountpoint" ]]; then
      chmod 0711 "$state_dir" "$state_dir/mounts"
      return 0
    fi
    if ! kill -0 "$satchel_pid" 2>/dev/null; then
      cat "$log_file" >&2
      return 1
    fi
    sleep 0.1
  done
  echo "timed out waiting for Satchel mount" >&2
  return 1
}

start_postgres() {
	pg_running=1
  runuser -u postgres -- pg_ctl -D "$pgdata" \
		-t "$SATCHEL_PGBENCH_START_TIMEOUT" \
    -l "$pgdata/postgres.log" \
    -o "-k $socket_dir -p $port -c listen_addresses='' -c shared_buffers=1GB -c max_wal_size=4GB -c min_wal_size=1GB -c checkpoint_timeout=15min -c checkpoint_completion_target=0.9 -c fsync=on -c full_page_writes=on -c synchronous_commit=on -c track_io_timing=on" \
    -w start >/dev/null
}

if [[ "$MODE" == native ]]; then
  pgdata="$case_root/pgdata"
  install -d -m 0700 -o postgres -g postgres "$pgdata"
else
  start_satchel "$case_root/state" "$result_dir/satchel-first.log"
  pgdata="$mountpoint/pgdata"
  install -d -m 0700 -o postgres -g postgres "$pgdata"
fi

{
  echo "started_at_utc=$started_at_utc"
  echo "mode=$MODE"
  echo "run_id=$SATCHEL_PGBENCH_RUN_ID"
  echo "scale=$SATCHEL_PGBENCH_SCALE"
  echo "clients=$SATCHEL_PGBENCH_CLIENTS"
  echo "warmup_seconds=$SATCHEL_PGBENCH_WARMUP"
  echo "runtime_seconds=$SATCHEL_PGBENCH_RUNTIME"
  echo "sample_rate=$SATCHEL_PGBENCH_SAMPLE_RATE"
  echo "start_timeout_seconds=$SATCHEL_PGBENCH_START_TIMEOUT"
	echo "mount_timeout_seconds=$SATCHEL_PGBENCH_MOUNT_TIMEOUT"
	echo "sync_interval=$SATCHEL_SYNC_INTERVAL"
	echo "dirty_limit=$SATCHEL_DIRTY_LIMIT"
	echo "checkpoint_interval=$SATCHEL_CHECKPOINT_INTERVAL"
	echo "metrics_addr=$SATCHEL_METRICS_ADDR"
	echo "s3_endpoint=$SATCHEL_S3_ENDPOINT"
	echo "s3_bucket=$SATCHEL_S3_BUCKET"
	echo "existing_volume=${SATCHEL_PGBENCH_EXISTING_VOLUME:-none}"
  uname -a
  postgres --version
	pgbench --version
	"$SATCHEL_BIN" version 2>/dev/null || true
	sha256sum "$SATCHEL_BIN"
	findmnt -T "$SATCHEL_PGBENCH_ROOT" -no SOURCE,FSTYPE,OPTIONS || true
} >"$result_dir/environment.txt"

if [[ -z "$SATCHEL_PGBENCH_EXISTING_VOLUME" ]]; then
	runuser -u postgres -- initdb -D "$pgdata" --no-locale --encoding=UTF8 --data-checksums >"$result_dir/initdb.txt"
	start_postgres
	createdb -h "$socket_dir" -p "$port" -U postgres pgbench
	pgbench -h "$socket_dir" -p "$port" -U postgres -i -s "$SATCHEL_PGBENCH_SCALE" -q pgbench \
		>"$result_dir/initialize.txt" 2>&1
	if [[ "$MODE" != native ]]; then
		stop_postgres
		stop_satchel
		start_satchel "$case_root/bench-state" "$result_dir/satchel-bench.log"
		pgdata="$mountpoint/pgdata"
		start_postgres
	fi
else
	printf 'reused volume %s\n' "$SATCHEL_PGBENCH_EXISTING_VOLUME" >"$result_dir/initialize.txt"
	start_postgres
fi

printf 'mode\tclients\ttps\taverage_ms\tp50_ms\tp95_ms\tp99_ms\n' >"$result_dir/summary.tsv"
for clients in $SATCHEL_PGBENCH_CLIENTS; do
  jobs=$clients
  if (( jobs > 4 )); then jobs=4; fi
  pgbench -h "$socket_dir" -p "$port" -U postgres -M prepared -n \
    -c "$clients" -j "$jobs" -T "$SATCHEL_PGBENCH_WARMUP" --random-seed=42 pgbench \
    >"$result_dir/warmup-c$clients.txt" 2>&1

  (
    cd "$result_dir"
    pgbench -h "$socket_dir" -p "$port" -U postgres -M prepared -n \
      -c "$clients" -j "$jobs" -T "$SATCHEL_PGBENCH_RUNTIME" -P 10 \
      -l --sampling-rate="$SATCHEL_PGBENCH_SAMPLE_RATE" \
      --log-prefix="transactions-c$clients" --random-seed=42 pgbench \
      >"run-c$clients.txt" 2>&1
  )

  awk '$3 ~ /^[0-9]+$/ {print $3}' "$result_dir"/transactions-c"$clients".* \
    | sort -n >"$result_dir/latency-c$clients.us"
  count=$(wc -l <"$result_dir/latency-c$clients.us")
  if (( count == 0 )); then
    echo "pgbench produced no latency samples" >&2
    exit 1
  fi
  percentile() {
    local pct=$1
    local index=$(( (count * pct + 99) / 100 ))
    sed -n "${index}p" "$result_dir/latency-c$clients.us"
  }
  p50=$(percentile 50)
  p95=$(percentile 95)
  p99=$(percentile 99)
  tps=$(awk '/^tps = / {value=$3} END {print value}' "$result_dir/run-c$clients.txt")
  average=$(awk '/^latency average = / {value=$4} END {print value}' "$result_dir/run-c$clients.txt")
  awk -v mode="$MODE" -v clients="$clients" -v tps="$tps" -v average="$average" \
    -v p50="$p50" -v p95="$p95" -v p99="$p99" \
    'BEGIN {printf "%s\t%d\t%s\t%s\t%.3f\t%.3f\t%.3f\n", mode, clients, tps, average, p50/1000, p95/1000, p99/1000}' \
    >>"$result_dir/summary.tsv"
	if [[ -n "$SATCHEL_METRICS_ADDR" ]] && command -v curl >/dev/null; then
		curl -fsS "http://$SATCHEL_METRICS_ADDR/metrics" >"$result_dir/metrics-c$clients.prom" || true
	fi
done

# Exercise PostgreSQL crash recovery on the mounted filesystem.
runuser -u postgres -- pg_ctl -D "$pgdata" -t "$SATCHEL_PGBENCH_START_TIMEOUT" -m immediate -w stop >"$result_dir/immediate-stop.txt" 2>&1
pg_running=0
start_postgres
pgbench -h "$socket_dir" -p "$port" -U postgres -M prepared -n -S -c 4 -j 4 -T 10 pgbench \
  >"$result_dir/crash-recovery-read.txt" 2>&1
runuser -u postgres -- pg_ctl -D "$pgdata" -t "$SATCHEL_PGBENCH_START_TIMEOUT" -m fast -w stop >"$result_dir/post-recovery-stop.txt" 2>&1
pg_running=0

if [[ "$MODE" != native ]]; then
  stop_satchel
  start_satchel "$case_root/restore-state" "$result_dir/satchel-restore.log"
  pgdata="$mountpoint/pgdata"
  start_postgres
  pgbench -h "$socket_dir" -p "$port" -U postgres -M prepared -n -S -c 4 -j 4 -T 10 pgbench \
    >"$result_dir/remote-restore-read.txt" 2>&1
  pg_amcheck -h "$socket_dir" -p "$port" -U postgres --database=pgbench --install-missing \
    >"$result_dir/pg-amcheck.txt" 2>&1
  stop_postgres
  stop_satchel
fi

printf 'finished_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$result_dir/environment.txt"
echo "$result_dir"
