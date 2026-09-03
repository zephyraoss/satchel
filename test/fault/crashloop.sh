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
: "${SATCHEL_FAULT_FAULTS:=pg satchel-break satchel-ttl s3-outage satchel-checkpoint satchel-upload gc-kill s3-chaos}"
: "${SATCHEL_LEASE_TTL:=30s}"

duration_seconds() {
  local value=$1 total=0
  if [[ "$value" =~ ^[0-9]+$ ]]; then
    echo "$value"
    return 0
  fi
  if ! [[ "$value" =~ ^([0-9]+h)?([0-9]+m)?([0-9]+s)?$ ]] || [[ -z "$value" ]]; then
    return 1
  fi
  [[ -n "${BASH_REMATCH[1]}" ]] && total=$(( total + ${BASH_REMATCH[1]%h} * 3600 ))
  [[ -n "${BASH_REMATCH[2]}" ]] && total=$(( total + ${BASH_REMATCH[2]%m} * 60 ))
  [[ -n "${BASH_REMATCH[3]}" ]] && total=$(( total + ${BASH_REMATCH[3]%s} ))
  echo "$total"
}
if ! lease_ttl_seconds=$(duration_seconds "$SATCHEL_LEASE_TTL"); then
  echo "SATCHEL_LEASE_TTL must be a whole number of seconds or an integer h/m/s duration such as 30s or 1h30m (got $SATCHEL_LEASE_TTL)" >&2
  exit 2
fi
: "${SATCHEL_FAULT_S3_OUTAGE_SECONDS:=$(( lease_ttl_seconds * 2 ))}"
: "${SATCHEL_FAULT_S3_STOP:=systemctl stop minio}"
: "${SATCHEL_FAULT_S3_START:=systemctl start minio}"
: "${SATCHEL_FAULT_LEASE_TTL_WAIT:=40}"
: "${SATCHEL_FAULT_GC_EVERY:=10}"
: "${SATCHEL_FAULT_VERIFY_EVERY:=5}"
: "${SATCHEL_FAULT_START_TIMEOUT:=300}"
: "${SATCHEL_FAULT_MOUNT_TIMEOUT:=300}"
: "${SATCHEL_SYNC_INTERVAL:=1s}"
: "${SATCHEL_DIRTY_LIMIT:=1GiB}"
: "${SATCHEL_CHECKPOINT_INTERVAL:=64}"
: "${SATCHEL_FAULT_CHECKPOINT_FAST_INTERVAL:=1}"
: "${SATCHEL_FAULT_UPLOAD_BURST_MIB:=256}"
: "${SATCHEL_FAULT_GC_GRACE:=0s}"
: "${SATCHEL_FAULT_CHAOS_ADDR:=127.0.0.1:19999}"
: "${SATCHEL_FAULT_CHAOS_SECONDS:=20}"
: "${SATCHEL_FAULT_CHAOSPROXY_BIN:=./chaosproxy}"
: "${SATCHEL_FAULT_KEEP_STATE:=0}"

: "${SATCHEL_FAULT_PG_PORT:=55433}"

run_root="$SATCHEL_FAULT_ROOT/$SATCHEL_FAULT_RUN_ID"
volume=$SATCHEL_FAULT_VOLUME
socket_dir="$run_root/socket"
port=$SATCHEL_FAULT_PG_PORT
satchel_pid=
mountpoint=
pgdata=
pg_pid=
pg_log=
pg_running=0
load_pids=()
burst_pid=
gc_pid=
proxy_pid=
satchel_mount_endpoint=

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
  local checkpoint_interval=${3:-$SATCHEL_CHECKPOINT_INTERVAL}
  install -d -m 0700 "$state_dir"
  env \
    SATCHEL_S3_ENDPOINT="${satchel_mount_endpoint:-$SATCHEL_S3_ENDPOINT}" \
    SATCHEL_S3_BUCKET="$SATCHEL_S3_BUCKET" \
    SATCHEL_S3_ACCESS_KEY="$SATCHEL_S3_ACCESS_KEY" \
    SATCHEL_S3_SECRET_KEY="$SATCHEL_S3_SECRET_KEY" \
    SATCHEL_STATE_DIR="$state_dir" \
    SATCHEL_SYNC_INTERVAL="$SATCHEL_SYNC_INTERVAL" \
    SATCHEL_DIRTY_LIMIT="$SATCHEL_DIRTY_LIMIT" \
    SATCHEL_CHECKPOINT_INTERVAL="$checkpoint_interval" \
    SATCHEL_LEASE_TTL="$SATCHEL_LEASE_TTL" \
    "$SATCHEL_BIN" mount "$volume" \
      --size "$SATCHEL_FAULT_VOLUME_SIZE" \
      --durability remote \
      --log-level warn >"$log_file" 2>&1 &
  satchel_pid=$!
  mountpoint=
  for _ in $(seq 1 $((SATCHEL_FAULT_MOUNT_TIMEOUT * 10))); do
    mountpoint="$state_dir/mounts/$volume"
    if mountpoint -q "$mountpoint" 2>/dev/null; then
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
  pkill -9 -f "pgbench -h $socket_dir " 2>/dev/null || true
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

start_chaos_proxy() {
  local cycle_dir=$1
  if [[ ! -x "$SATCHEL_FAULT_CHAOSPROXY_BIN" ]]; then
    go build -o "$SATCHEL_FAULT_CHAOSPROXY_BIN" ./test/fault/chaosproxy || fail "chaosproxy build failed"
  fi
  case "$SATCHEL_S3_ENDPOINT" in
    http://*) ;;
    *) fail "s3-chaos needs a plain http:// S3 endpoint; the chaos proxy forwards raw TCP and cannot terminate TLS (got $SATCHEL_S3_ENDPOINT)" ;;
  esac
  "$SATCHEL_FAULT_CHAOSPROXY_BIN" \
    -listen "$SATCHEL_FAULT_CHAOS_ADDR" \
    -target "${SATCHEL_S3_ENDPOINT#http://}" \
    -chaos-file "$cycle_dir/chaos-on" >"$cycle_dir/chaosproxy.log" 2>&1 &
  proxy_pid=$!
  sleep 0.3
  kill -0 "$proxy_pid" 2>/dev/null || fail "chaosproxy did not start, see $cycle_dir/chaosproxy.log"
  satchel_mount_endpoint="http://$SATCHEL_FAULT_CHAOS_ADDR"
}

stop_chaos_proxy() {
  if [[ -n "$proxy_pid" ]]; then
    kill -9 "$proxy_pid" 2>/dev/null || true
    wait "$proxy_pid" 2>/dev/null || true
  fi
  proxy_pid=
  satchel_mount_endpoint=
}

cleanup_cycle_state() {
  if [[ "$SATCHEL_FAULT_KEEP_STATE" == 1 ]]; then
    return 0
  fi
  local dir
  for dir in "$cycle_dir/state" "$cycle_dir/takeover-state" "$cycle_dir/verify-state"; do
    if [[ -d "$dir" ]]; then
      rm -rf "$dir"
    fi
  done
}

cleanup() {
  stop_load
  if [[ -n "$burst_pid" ]]; then kill -9 "$burst_pid" 2>/dev/null || true; fi
  if [[ -n "$gc_pid" ]]; then kill -9 "$gc_pid" 2>/dev/null || true; fi
  stop_postgres_clean
  stop_satchel_clean
  stop_chaos_proxy
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

  mount_checkpoint_interval=$SATCHEL_CHECKPOINT_INTERVAL
  if [[ "$fault" == satchel-checkpoint ]]; then
    mount_checkpoint_interval=$SATCHEL_FAULT_CHECKPOINT_FAST_INTERVAL
  fi
  if [[ "$fault" == s3-chaos ]]; then
    start_chaos_proxy "$cycle_dir"
  fi
  start_satchel "$cycle_dir/state" "$cycle_dir/satchel.log" "$mount_checkpoint_interval" || \
    fail "mount failed, see $cycle_dir/satchel.log"
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
      log "stopping S3 for ${SATCHEL_FAULT_S3_OUTAGE_SECONDS}s against lease TTL $SATCHEL_LEASE_TTL"
      $SATCHEL_FAULT_S3_STOP
      sleep "$SATCHEL_FAULT_S3_OUTAGE_SECONDS"
      $SATCHEL_FAULT_S3_START
      sleep 15
      if (( SATCHEL_FAULT_S3_OUTAGE_SECONDS > lease_ttl_seconds )); then
        if ! grep -q "fencing volume" "$cycle_dir/satchel.log"; then
          fail "outage exceeded the lease TTL but the writer did not fence, see $cycle_dir/satchel.log"
        fi
        log "writer fenced during outage as designed; verifying recovery after S3 return"
      elif kill -0 "$satchel_pid" 2>/dev/null && kill -0 "$pg_pid" 2>/dev/null; then
        log "survived outage without fencing (outage shorter than the lease TTL)"
      else
        fail "outage was shorter than the lease TTL but the writer or database died, see $cycle_dir/satchel.log"
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
    satchel-checkpoint)
      sleep "0.$((RANDOM % 10))"
      log "kill -9 satchel pid $satchel_pid with checkpoint interval $mount_checkpoint_interval"
      kill_satchel_hard
      kill_postgres_hard
      force_unmount
      satchel_cmd "$SATCHEL_BIN" vol lease break --yes "$volume" >>"$cycle_dir/lease-break.log" 2>&1 || \
        fail "lease break failed, see $cycle_dir/lease-break.log"
      start_satchel "$cycle_dir/takeover-state" "$cycle_dir/satchel-takeover.log" || \
        fail "takeover mount failed, see $cycle_dir/satchel-takeover.log"
      pgdata="$mountpoint/pgdata"
      start_postgres
      verify_recovery "$cycle_dir" "$base_seq"
      stop_postgres_clean
      stop_satchel_clean
      ;;
    satchel-upload)
      log "dd ${SATCHEL_FAULT_UPLOAD_BURST_MIB}MiB burst to force large segment uploads"
      dd if=/dev/urandom of="$mountpoint/upload-burst.bin" bs=4M \
        count=$((SATCHEL_FAULT_UPLOAD_BURST_MIB / 4)) oflag=direct conv=fsync \
        >"$cycle_dir/dd.log" 2>&1 &
      burst_pid=$!
      sleep "$((RANDOM % 3 + 1)).$((RANDOM % 10))"
      log "kill -9 satchel pid $satchel_pid during upload burst"
      kill_satchel_hard
      kill -9 "$burst_pid" 2>/dev/null || true
      kill_postgres_hard
      force_unmount
      for _ in $(seq 1 100); do
        kill -0 "$burst_pid" 2>/dev/null || break
        sleep 0.1
      done
      if kill -0 "$burst_pid" 2>/dev/null; then
        log "dd burst pid $burst_pid stuck on dead device; continuing without it"
      else
        wait "$burst_pid" 2>/dev/null || true
      fi
      burst_pid=
      satchel_cmd "$SATCHEL_BIN" vol lease break --yes "$volume" >>"$cycle_dir/lease-break.log" 2>&1 || \
        fail "lease break failed, see $cycle_dir/lease-break.log"
      start_satchel "$cycle_dir/takeover-state" "$cycle_dir/satchel-takeover.log" || \
        fail "takeover mount failed, see $cycle_dir/satchel-takeover.log"
      pgdata="$mountpoint/pgdata"
      if [[ -e "$mountpoint/upload-burst.bin" ]]; then
        log "burst file survived with $(stat -c %s "$mountpoint/upload-burst.bin") bytes (unacked tail loss is acceptable)"
        rm -f "$mountpoint/upload-burst.bin"
      else
        log "burst file lost entirely (acceptable, it was never acked)"
      fi
      start_postgres
      verify_recovery "$cycle_dir" "$base_seq"
      stop_postgres_clean
      stop_satchel_clean
      ;;
    s3-chaos)
      log "dropping S3 connections mid-body for ${SATCHEL_FAULT_CHAOS_SECONDS}s"
      touch "$cycle_dir/chaos-on"
      sleep "$SATCHEL_FAULT_CHAOS_SECONDS"
      rm -f "$cycle_dir/chaos-on"
      sleep 10
      if kill -0 "$satchel_pid" 2>/dev/null && kill -0 "$pg_pid" 2>/dev/null; then
        log "survived torn-write chaos; commits stalled and resumed"
        stop_load
        stop_postgres_clean
        stop_satchel_clean
      else
        log "writer fenced during chaos; verifying recovery"
        stop_load
        kill_postgres_hard
        kill_satchel_hard
        force_unmount
      fi
      stop_chaos_proxy
      satchel_cmd "$SATCHEL_BIN" vol lease break --yes "$volume" >>"$cycle_dir/lease-break.log" 2>&1 || \
        fail "lease break failed, see $cycle_dir/lease-break.log"
      start_satchel "$cycle_dir/verify-state" "$cycle_dir/satchel-verify.log" || \
        fail "post-chaos mount failed, see $cycle_dir/satchel-verify.log"
      pgdata="$mountpoint/pgdata"
      start_postgres
      verify_recovery "$cycle_dir" "$base_seq"
      stop_postgres_clean
      stop_satchel_clean
      ;;
    gc-kill)
      stop_load
      stop_postgres_clean
      stop_satchel_clean
      log "starting gc in background with grace $SATCHEL_FAULT_GC_GRACE"
      env \
        SATCHEL_S3_ENDPOINT="$SATCHEL_S3_ENDPOINT" \
        SATCHEL_S3_BUCKET="$SATCHEL_S3_BUCKET" \
        SATCHEL_S3_ACCESS_KEY="$SATCHEL_S3_ACCESS_KEY" \
        SATCHEL_S3_SECRET_KEY="$SATCHEL_S3_SECRET_KEY" \
        "$SATCHEL_BIN" vol gc --gc-grace "$SATCHEL_FAULT_GC_GRACE" "$volume" \
        >"$cycle_dir/gc.log" 2>&1 &
      gc_pid=$!
      gc_delay=$((RANDOM % 26 + 5))
      sleep "$((gc_delay / 10)).$((gc_delay % 10))"
      if kill -0 "$gc_pid" 2>/dev/null; then
        log "kill -9 gc pid $gc_pid mid-run after ${gc_delay}00ms"
        kill -9 "$gc_pid" 2>/dev/null || true
        wait "$gc_pid" 2>/dev/null || true
      else
        gc_status=0
        wait "$gc_pid" 2>/dev/null || gc_status=$?
        if (( gc_status != 0 )); then
          fail "gc exited with status $gc_status before the kill landed, see $cycle_dir/gc.log"
        fi
        log "gc finished before the kill landed; interrupted-sweep fault NOT exercised this cycle"
        echo "$cycle" >>"$run_root/gc-kill-untested.log"
      fi
      gc_pid=
      satchel_cmd "$SATCHEL_BIN" vol lease break --yes "$volume" >>"$cycle_dir/lease-break.log" 2>&1 || \
        fail "lease break failed, see $cycle_dir/lease-break.log"
      start_satchel "$cycle_dir/takeover-state" "$cycle_dir/satchel-takeover.log" || \
        fail "post-gc-kill mount failed, see $cycle_dir/satchel-takeover.log"
      pgdata="$mountpoint/pgdata"
      start_postgres
      verify_recovery "$cycle_dir" "$base_seq"
      stop_postgres_clean
      stop_satchel_clean
      log "running gc to completion"
      satchel_cmd "$SATCHEL_BIN" vol gc --gc-grace "$SATCHEL_FAULT_GC_GRACE" "$volume" \
        >>"$cycle_dir/gc-final.log" 2>&1 || fail "post-kill gc failed, see $cycle_dir/gc-final.log"
      ;;
    *)
      fail "unknown fault: $fault"
      ;;
  esac

  if ((cycle % SATCHEL_FAULT_GC_EVERY == 0)); then
    log "running gc"
    satchel_cmd "$SATCHEL_BIN" vol gc "$volume" >>"$run_root/gc.log" 2>&1 || fail "gc failed, see $run_root/gc.log"
  fi
  if ((cycle % SATCHEL_FAULT_VERIFY_EVERY == 0)); then
    log "running vol verify"
    satchel_cmd "$SATCHEL_BIN" vol verify "$volume" >>"$run_root/verify.log" 2>&1 || \
      fail "metadata verification failed, see $run_root/verify.log"
  fi
  cleanup_cycle_state
  log "cycle $cycle ok"
done

if [[ -s "$run_root/gc-kill-untested.log" ]]; then
  log "warning: gc-kill cycles $(paste -sd, "$run_root/gc-kill-untested.log") finished before the kill and did not exercise an interrupted sweep"
fi
log "campaign complete: $CYCLES cycles, volume $volume"
