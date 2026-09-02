#!/usr/bin/env bash
set -euo pipefail

CYCLES=${1:-8}

: "${SATCHEL_BIN:=./satchel}"
: "${SATCHEL_S3_ENDPOINT:=http://127.0.0.1:9000}"
: "${SATCHEL_S3_BUCKET:=satchel}"
: "${SATCHEL_S3_ACCESS_KEY:=minioadmin}"
: "${SATCHEL_S3_SECRET_KEY:=minioadmin}"
: "${SATCHEL_FAULT_ROOT:=/var/lib/satchel-fault}"
: "${SATCHEL_FAULT_RUN_ID:=$(date -u +%Y%m%dT%H%M%SZ)}"
: "${SATCHEL_FAULT_VOLUME:=fault-$SATCHEL_FAULT_RUN_ID}"
: "${SATCHEL_FAULT_SCALE:=10}"
: "${SATCHEL_FAULT_VOLUME_SIZE:=8GiB}"
: "${SATCHEL_FAULT_LOAD_MIN_SECONDS:=8}"
: "${SATCHEL_FAULT_LOAD_MAX_SECONDS:=25}"
: "${SATCHEL_FAULT_FAULTS:=pg satchel-break satchel-ttl s3-outage}"
: "${SATCHEL_FAULT_S3_OUTAGE_SECONDS:=30}"
: "${SATCHEL_FAULT_S3_STOP:=systemctl stop minio}"
: "${SATCHEL_FAULT_S3_START:=systemctl start minio}"
: "${SATCHEL_FAULT_LEASE_TTL_WAIT:=40}"
: "${SATCHEL_FAULT_GC_EVERY:=10}"
: "${SATCHEL_FAULT_START_TIMEOUT:=300}"
: "${SATCHEL_FAULT_MOUNT_TIMEOUT:=300}"
: "${SATCHEL_SYNC_INTERVAL:=1s}"
: "${SATCHEL_DIRTY_LIMIT:=1GiB}"
: "${SATCHEL_CHECKPOINT_INTERVAL:=64}"

run_root="$SATCHEL_FAULT_ROOT/$SATCHEL_FAULT_RUN_ID"
volume=$SATCHEL_FAULT_VOLUME
socket_dir="$run_root/socket"
port=55433
satchel_pid=
mountpoint=
pgdata=
pg_pid=
pg_log=
pg_running=0
load_pids=()

if [[ -e "$run_root" ]]; then
  echo "fault campaign already exists: $SATCHEL_FAULT_RUN_ID" >&2
  exit 1
fi
install -d -m 0755 "$run_root"
install -d -m 0755 "$socket_dir"
chown postgres:postgres "$socket_dir"

preflight_clean() {
  pkill -9 -f "$SATCHEL_BIN mount $volume" 2>/dev/null || true
  local m
  for m in $(findmnt -rn -o TARGET 2>/dev/null | grep "mounts/$volume" || true); do
    umount -f "$m" 2>/dev/null || umount -l "$m" 2>/dev/null || true
  done
}
preflight_clean

log() {
  printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$run_root/campaign.log"
}

fail() {
  log "FAIL: $*"
  log "artifacts preserved under $run_root"
  exit 1
}

satchel_cmd() {
  env \
    SATCHEL_S3_ENDPOINT="$SATCHEL_S3_ENDPOINT" \
    SATCHEL_S3_BUCKET="$SATCHEL_S3_BUCKET" \
    SATCHEL_S3_ACCESS_KEY="$SATCHEL_S3_ACCESS_KEY" \
    SATCHEL_S3_SECRET_KEY="$SATCHEL_S3_SECRET_KEY" \
    "$@"
}

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
    "$SATCHEL_BIN" mount "$volume" \
      --size "$SATCHEL_FAULT_VOLUME_SIZE" \
      --durability remote \
      --log-level warn >"$log_file" 2>&1 &
  satchel_pid=$!
  mountpoint=
  for _ in $(seq 1 $((SATCHEL_FAULT_MOUNT_TIMEOUT * 10))); do
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

stop_satchel_clean() {
  if [[ -n "$satchel_pid" ]] && kill -0 "$satchel_pid" 2>/dev/null; then
    kill -INT "$satchel_pid" 2>/dev/null || true
    wait "$satchel_pid" || true
  fi
  satchel_pid=
  mountpoint=
}

kill_satchel_hard() {
  kill -9 "$satchel_pid" 2>/dev/null || true
  wait "$satchel_pid" 2>/dev/null || true
  satchel_pid=
}

force_unmount() {
  if [[ -n "$mountpoint" ]]; then
    umount -f "$mountpoint" 2>/dev/null || umount -l "$mountpoint" 2>/dev/null || true
  fi
  mountpoint=
}

start_postgres() {
  pg_running=1
  runuser -u postgres -- pg_ctl -D "$pgdata" \
    -t "$SATCHEL_FAULT_START_TIMEOUT" \
    -l "$pg_log" \
    -o "-k $socket_dir -p $port -c listen_addresses='' -c shared_buffers=512MB -c max_wal_size=1GB -c min_wal_size=256MB -c checkpoint_timeout=2min -c checkpoint_completion_target=0.7 -c fsync=on -c full_page_writes=on -c synchronous_commit=on" \
    -w start >/dev/null
  pg_pid=$(head -1 "$pgdata/postmaster.pid")
}

stop_postgres_clean() {
  if [[ "$pg_running" == 1 ]]; then
    runuser -u postgres -- pg_ctl -D "$pgdata" -t "$SATCHEL_FAULT_START_TIMEOUT" -m fast -w stop >/dev/null 2>&1 || \
      runuser -u postgres -- pg_ctl -D "$pgdata" -t "$SATCHEL_FAULT_START_TIMEOUT" -m immediate -w stop >/dev/null 2>&1 || true
    pg_running=0
  fi
  pg_pid=
}

descendants() {
  local pid=$1 child
  for child in $(pgrep -P "$pid" 2>/dev/null); do
    descendants "$child"
    echo "$child"
  done
}

kill_postgres_hard() {
  stop_load
  if [[ -n "$pg_pid" ]]; then
    local tree
    tree=$(descendants "$pg_pid")
    kill -9 "$pg_pid" $tree 2>/dev/null || true
    for _ in $(seq 1 600); do
      local alive=0 p
      kill -0 "$pg_pid" 2>/dev/null && alive=1
      for p in $tree; do kill -0 "$p" 2>/dev/null && alive=1; done
      [[ "$alive" == 0 ]] && break
      sleep 0.1
    done
  fi
  pg_running=0
  pg_pid=
}

psqlc() {
  psql -h "$socket_dir" -p "$port" -U postgres -d fault -qtAc "$1"
}

ledger_writer() {
  local acked=$1
  local seq=$2
  while :; do
    seq=$((seq + 1))
    if psql -h "$socket_dir" -p "$port" -U postgres -d fault -qtAc \
        "INSERT INTO ledger (seq) VALUES ($seq)" >/dev/null 2>&1; then
      echo "$seq" >>"$acked"
    else
      return 0
    fi
  done
}

start_load() {
  local cycle_dir=$1
  local base_seq=$2
  pgbench -h "$socket_dir" -p "$port" -U postgres -M prepared -n \
    -c 2 -j 2 -T 3600 fault >"$cycle_dir/pgbench.log" 2>&1 &
  load_pids+=($!)
  ledger_writer "$cycle_dir/acked.log" "$base_seq" &
  load_pids+=($!)
}

stop_load() {
  local pid
  for pid in "${load_pids[@]:-}"; do
    pkill -9 -P "$pid" 2>/dev/null || true
    kill -9 "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  load_pids=()
  pkill -9 -f "pgbench .*-p $port .*fault" 2>/dev/null || true
  pkill -9 -f "psql -h $socket_dir" 2>/dev/null || true
}

verify_recovery() {
  local cycle_dir=$1
  local base_seq=$2
  local last_acked present drift

  last_acked=$base_seq
  if [[ -s "$cycle_dir/acked.log" ]]; then
    last_acked=$(tail -1 "$cycle_dir/acked.log")
  fi
  present=$(psqlc "SELECT count(*) FROM ledger WHERE seq > $base_seq AND seq <= $last_acked")
  if [[ "$present" != $((last_acked - base_seq)) ]]; then
    fail "acked commits lost: expected seq $((base_seq + 1))..$last_acked, found $present rows"
  fi
  log "ledger ok: $((last_acked - base_seq)) acked commits survived"

  drift=$(psqlc "
    SELECT
      (SELECT count(*) FROM pgbench_accounts a
        LEFT JOIN (SELECT aid, sum(delta) AS s FROM pgbench_history GROUP BY aid) h USING (aid)
        WHERE a.abalance <> COALESCE(h.s, 0)) +
      (SELECT count(*) FROM pgbench_tellers t
        LEFT JOIN (SELECT tid, sum(delta) AS s FROM pgbench_history GROUP BY tid) h USING (tid)
        WHERE t.tbalance <> COALESCE(h.s, 0)) +
      (SELECT count(*) FROM pgbench_branches b
        LEFT JOIN (SELECT bid, sum(delta) AS s FROM pgbench_history GROUP BY bid) h USING (bid)
        WHERE b.bbalance <> COALESCE(h.s, 0))")
  if [[ "$drift" != 0 ]]; then
    fail "pgbench balance/history consistency violated on $drift rows"
  fi
  log "balance/history consistency ok"

  if ! pg_amcheck -h "$socket_dir" -p "$port" -U postgres --database=fault --install-missing \
      >"$cycle_dir/pg-amcheck.txt" 2>&1; then
    fail "pg_amcheck reported corruption, see $cycle_dir/pg-amcheck.txt"
  fi
  log "pg_amcheck ok"
}

initialize_database() {
  local cycle_dir=$1
  runuser -u postgres -- initdb -D "$pgdata" --no-locale --encoding=UTF8 --data-checksums \
    >"$cycle_dir/initdb.log" 2>&1
  start_postgres
  createdb -h "$socket_dir" -p "$port" -U postgres fault
  pgbench -h "$socket_dir" -p "$port" -U postgres -i -s "$SATCHEL_FAULT_SCALE" -q fault \
    >"$cycle_dir/pgbench-init.log" 2>&1
  psqlc "CREATE TABLE ledger (seq bigint PRIMARY KEY)" >/dev/null
}

cleanup() {
  stop_load
  stop_postgres_clean
  stop_satchel_clean
}
trap cleanup EXIT

read -r -a fault_rotation <<<"$SATCHEL_FAULT_FAULTS"

for ((cycle = 1; cycle <= CYCLES; cycle++)); do
  fault=${fault_rotation[$(((cycle - 1) % ${#fault_rotation[@]}))]}
  cycle_dir="$run_root/cycle-$cycle-$fault"
  install -d -m 0755 "$cycle_dir"
  pg_log="$cycle_dir/postgres.log"
  install -m 0644 -o postgres -g postgres /dev/null "$pg_log"
  log "cycle $cycle/$CYCLES fault=$fault"

  start_satchel "$cycle_dir/state" "$cycle_dir/satchel.log" || fail "mount failed, see $cycle_dir/satchel.log"
  pgdata="$mountpoint/pgdata"
  if [[ ! -f "$pgdata/PG_VERSION" ]]; then
    install -d -m 0700 -o postgres -g postgres "$pgdata"
    initialize_database "$cycle_dir"
  else
    start_postgres
  fi

  base_seq=$(psqlc "SELECT COALESCE(max(seq), 0) FROM ledger")
  start_load "$cycle_dir" "$base_seq"
  sleep $((RANDOM % (SATCHEL_FAULT_LOAD_MAX_SECONDS - SATCHEL_FAULT_LOAD_MIN_SECONDS + 1) + SATCHEL_FAULT_LOAD_MIN_SECONDS))

  case "$fault" in
    pg)
      log "kill -9 postgres pid $pg_pid"
      kill_postgres_hard
      start_postgres
      verify_recovery "$cycle_dir" "$base_seq"
      stop_postgres_clean
      stop_satchel_clean
      ;;
    satchel-break | satchel-ttl)
      log "kill -9 satchel pid $satchel_pid"
      kill_satchel_hard
      kill_postgres_hard
      force_unmount
      if [[ "$fault" == satchel-break ]]; then
        satchel_cmd "$SATCHEL_BIN" vol lease break --yes "$volume" >>"$cycle_dir/lease-break.log" 2>&1 || \
          fail "lease break failed, see $cycle_dir/lease-break.log"
      else
        log "waiting ${SATCHEL_FAULT_LEASE_TTL_WAIT}s for lease expiry"
        sleep "$SATCHEL_FAULT_LEASE_TTL_WAIT"
      fi
      start_satchel "$cycle_dir/takeover-state" "$cycle_dir/satchel-takeover.log" || \
        fail "takeover mount failed, see $cycle_dir/satchel-takeover.log"
      pgdata="$mountpoint/pgdata"
      start_postgres
      verify_recovery "$cycle_dir" "$base_seq"
      stop_postgres_clean
      stop_satchel_clean
      ;;
    s3-outage)
      log "stopping S3 for ${SATCHEL_FAULT_S3_OUTAGE_SECONDS}s (under lease TTL stalls commits; over it self-fences)"
      $SATCHEL_FAULT_S3_STOP
      sleep "$SATCHEL_FAULT_S3_OUTAGE_SECONDS"
      $SATCHEL_FAULT_S3_START
      sleep 15
      if kill -0 "$satchel_pid" 2>/dev/null && kill -0 "$pg_pid" 2>/dev/null; then
        log "survived outage without fencing (outage shorter than renewal window)"
      else
        log "writer fenced during outage as designed; verifying recovery after S3 return"
      fi
      stop_load
      kill_postgres_hard
      kill_satchel_hard
      force_unmount
      satchel_cmd "$SATCHEL_BIN" vol lease break --yes "$volume" >>"$cycle_dir/lease-break.log" 2>&1 || \
        fail "lease break failed, see $cycle_dir/lease-break.log"
      start_satchel "$cycle_dir/verify-state" "$cycle_dir/satchel-verify.log" || \
        fail "post-outage mount failed, see $cycle_dir/satchel-verify.log"
      pgdata="$mountpoint/pgdata"
      start_postgres
      verify_recovery "$cycle_dir" "$base_seq"
      stop_postgres_clean
      stop_satchel_clean
      ;;
    *)
      fail "unknown fault: $fault"
      ;;
  esac

  if ((cycle % SATCHEL_FAULT_GC_EVERY == 0)); then
    log "running gc"
    satchel_cmd "$SATCHEL_BIN" vol gc "$volume" >>"$run_root/gc.log" 2>&1 || fail "gc failed, see $run_root/gc.log"
  fi
  log "cycle $cycle ok"
done

log "campaign complete: $CYCLES cycles, volume $volume"
