#!/usr/bin/env bash
set -euo pipefail

PHASE=${1:-}
RUN=${2:-reboot-$(date -u +%Y%m%dT%H%M%SZ)}
case "$PHASE" in
  crash | verify) ;;
  *) echo "usage: $0 crash|verify [run-id]" >&2; exit 2 ;;
esac

: "${SATCHEL_BIN:=./satchel}"
: "${SATCHEL_S3_ENDPOINT:=http://127.0.0.1:9000}"
: "${SATCHEL_S3_BUCKET:=satchel}"
: "${SATCHEL_S3_ACCESS_KEY:=minioadmin}"
: "${SATCHEL_S3_SECRET_KEY:=minioadmin}"
: "${SATCHEL_FAULT_ROOT:=/var/lib/satchel-fault}"
: "${SATCHEL_FAULT_SCALE:=10}"
: "${SATCHEL_FAULT_VOLUME_SIZE:=8GiB}"
: "${SATCHEL_FAULT_REBOOT_DELAY:=15}"
: "${SATCHEL_SYNC_INTERVAL:=1s}"
: "${SATCHEL_DIRTY_LIMIT:=1GiB}"
: "${SATCHEL_CHECKPOINT_INTERVAL:=64}"
: "${SATCHEL_FAULT_START_TIMEOUT:=300}"
: "${SATCHEL_FAULT_MOUNT_TIMEOUT:=300}"

run_root="$SATCHEL_FAULT_ROOT/$RUN"
volume=fault-$RUN
socket_dir="$run_root/socket"
port=55434
satchel_pid=
mountpoint=
pgdata=
pg_running=0

log() {
  printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$run_root/campaign.log"
}

fail() {
  log "FAIL: $*"
  exit 1
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

start_postgres() {
  pg_running=1
  install -m 0644 -o postgres -g postgres /dev/null "$run_root/postgres-$PHASE.log" 2>/dev/null || true
  runuser -u postgres -- pg_ctl -D "$pgdata" \
    -t "$SATCHEL_FAULT_START_TIMEOUT" \
    -l "$run_root/postgres-$PHASE.log" \
    -o "-k $socket_dir -p $port -c listen_addresses='' -c shared_buffers=512MB -c max_wal_size=1GB -c min_wal_size=256MB -c checkpoint_timeout=2min -c fsync=on -c full_page_writes=on -c synchronous_commit=on" \
    -w start >/dev/null
}

stop_postgres_clean() {
  if [[ "$pg_running" == 1 ]]; then
    runuser -u postgres -- pg_ctl -D "$pgdata" -t "$SATCHEL_FAULT_START_TIMEOUT" -m fast -w stop >/dev/null 2>&1 || true
    pg_running=0
  fi
}

stop_satchel_clean() {
  if [[ -n "$satchel_pid" ]] && kill -0 "$satchel_pid" 2>/dev/null; then
    kill -INT "$satchel_pid" 2>/dev/null || true
    wait "$satchel_pid" || true
  fi
  satchel_pid=
}

psqlc() {
  psql -h "$socket_dir" -p "$port" -U postgres -d fault -qtAc "$1"
}

durable_ledger_writer() {
  local acked=$1
  local seq=$2
  while :; do
    seq=$((seq + 1))
    if psql -h "$socket_dir" -p "$port" -U postgres -d fault -qtAc \
        "INSERT INTO ledger (seq) VALUES ($seq)" >/dev/null 2>&1; then
      echo "$seq" >>"$acked"
      sync -d "$acked"
    else
      return 0
    fi
  done
}

if [[ "$PHASE" == crash ]]; then
  if [[ -e "$run_root" ]]; then
    echo "reboot run already exists: $RUN" >&2
    exit 1
  fi
  install -d -m 0755 "$run_root"
  install -d -m 0755 "$socket_dir"
  chown postgres:postgres "$socket_dir"

  log "setting up volume $volume for hard-reboot test"
  start_satchel "$run_root/state" "$run_root/satchel.log" || fail "mount failed, see $run_root/satchel.log"
  pgdata="$mountpoint/pgdata"
  install -d -m 0700 -o postgres -g postgres "$pgdata"
  runuser -u postgres -- initdb -D "$pgdata" --no-locale --encoding=UTF8 --data-checksums \
    >"$run_root/initdb.log" 2>&1
  start_postgres
  createdb -h "$socket_dir" -p "$port" -U postgres fault
  pgbench -h "$socket_dir" -p "$port" -U postgres -i -s "$SATCHEL_FAULT_SCALE" -q fault \
    >"$run_root/pgbench-init.log" 2>&1
  psqlc "CREATE TABLE ledger (seq bigint PRIMARY KEY)" >/dev/null
  echo 0 >"$run_root/base_seq"
  sync -d "$run_root/base_seq"

  log "starting load; hard reset in ${SATCHEL_FAULT_REBOOT_DELAY}s"
  setsid nohup pgbench -h "$socket_dir" -p "$port" -U postgres -M prepared -n \
    -c 2 -j 2 -T 3600 fault >"$run_root/pgbench.log" 2>&1 &
  setsid nohup bash -c "$(declare -f durable_ledger_writer); socket_dir='$socket_dir' port='$port' durable_ledger_writer '$run_root/acked.log' 0" \
    >/dev/null 2>&1 &
  setsid nohup bash -c "sleep $SATCHEL_FAULT_REBOOT_DELAY; sysctl -w kernel.sysrq=1 >/dev/null; echo b > /proc/sysrq-trigger" \
    >/dev/null 2>&1 &
  log "reboot scheduled; run '$0 verify $RUN' after the host returns"
  trap - EXIT 2>/dev/null || true
  exit 0
fi

[[ -d "$run_root" ]] || fail "run $RUN has no crash-phase artifacts under $run_root"
cleanup_verify() {
  stop_postgres_clean
  stop_satchel_clean
}
trap cleanup_verify EXIT
log "verifying after hard reboot"
rm -rf "$run_root/state"
env \
  SATCHEL_S3_ENDPOINT="$SATCHEL_S3_ENDPOINT" \
  SATCHEL_S3_BUCKET="$SATCHEL_S3_BUCKET" \
  SATCHEL_S3_ACCESS_KEY="$SATCHEL_S3_ACCESS_KEY" \
  SATCHEL_S3_SECRET_KEY="$SATCHEL_S3_SECRET_KEY" \
  "$SATCHEL_BIN" vol lease break --yes "$volume" >>"$run_root/lease-break.log" 2>&1 || \
  fail "lease break failed, see $run_root/lease-break.log"
start_satchel "$run_root/verify-state" "$run_root/satchel-verify.log" || \
  fail "post-reboot mount failed, see $run_root/satchel-verify.log"
pgdata="$mountpoint/pgdata"
start_postgres

base_seq=$(cat "$run_root/base_seq")
last_acked=$base_seq
if [[ -s "$run_root/acked.log" ]]; then
  last_acked=$(grep -E '^[0-9]+$' "$run_root/acked.log" | tail -1)
fi
if [[ "$last_acked" == "$base_seq" ]]; then
  fail "ledger writer recorded no acked commits before the reset; the durability check is vacuous"
fi
present=$(psqlc "SELECT count(*) FROM ledger WHERE seq > $base_seq AND seq <= $last_acked")
if [[ "$present" != $((last_acked - base_seq)) ]]; then
  fail "acked commits lost across reboot: expected seq $((base_seq + 1))..$last_acked, found $present rows"
fi
log "ledger ok: $((last_acked - base_seq)) fsynced acked commits survived the hard reset"

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
    >"$run_root/pg-amcheck.txt" 2>&1; then
  fail "pg_amcheck reported corruption, see $run_root/pg-amcheck.txt"
fi
log "pg_amcheck ok"

stop_postgres_clean
stop_satchel_clean
rm -rf "$run_root/verify-state"
log "hard-reboot cycle ok"
