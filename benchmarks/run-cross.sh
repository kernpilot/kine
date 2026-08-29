#!/usr/bin/env bash
# Cross-instance watch-latency benchmark: two kine instances on ONE database,
# every write to instance A, every watcher on instance B.
#
# This is the topology Kamaji and k0smotron actually deploy, and it is the only
# one where kine's poll ticker is visible. A single kine wakes its own poll loop
# in-process on every insert (sqllog/sql.go:660), so a one-instance benchmark
# measures a fast path that a replicated deployment does not have.
#
# Usage: ./run-cross.sh <label> [duration] [writers] [watchers] [write-rate]
#   KINE_BIN  which kine to run (stock, or the e1-listen-notify build)
#   VARIANT   optional .sql applied after the schema exists
set -euo pipefail

cd "$(dirname "$0")"
LABEL="${1:?usage: run-cross.sh <label> [duration] [writers] [watchers] [rate]}"
DURATION="${2:-60s}"
WRITERS="${3:-4}"
WATCHERS="${4:-16}"
RATE="${5:-1}"
VARIANT="${VARIANT:-}"

KINE_BIN="${KINE_BIN:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/kine-upstream}"
PG_CONTAINER="${PG_CONTAINER:-kine-bench-pg}"
MAX_OPEN="${KINE_MAX_OPEN:-40}"
DSN="postgres://kine:kine@localhost:55432/kine?sslmode=disable"
RESULTS="results"
mkdir -p "$RESULTS"

log() { printf '\033[36m»\033[0m %s\n' "$*"; }
die() { printf '\033[31m✗\033[0m %s\n' "$*" >&2; exit 1; }

[[ -x "$KINE_BIN" ]] || die "kine binary not found at $KINE_BIN"
[[ -x ./loadgen/loadgen ]] || die "loadgen not built"
docker exec "$PG_CONTAINER" pg_isready -U kine -d kine >/dev/null 2>&1 \
  || die "postgres container '$PG_CONTAINER' is not ready"

docker exec "$PG_CONTAINER" psql -U kine -d kine -qc 'DROP TABLE IF EXISTS kine CASCADE' >/dev/null

start_kine() { # port  logsuffix
  "$KINE_BIN" --endpoint "$DSN" --listen-address "127.0.0.1:$1" \
    --metrics-bind-address 0 --datastore-max-open-connections "$MAX_OPEN" \
    >"$RESULTS/$LABEL-$2.kine.log" 2>&1 &
  echo $!
}

wait_port() { # port
  for _ in $(seq 1 60); do
    (exec 3<>/dev/tcp/127.0.0.1/"$1") 2>/dev/null && { exec 3<&- 3>&-; return 0; }
    sleep 0.25
  done
  return 1
}

log "starting kine A (writers) on 2379"
PID_A=$(start_kine 2379 a)
# shellcheck disable=SC2064
trap "kill $PID_A 2>/dev/null || true" EXIT
wait_port 2379 || die "kine A never listened — see $RESULTS/$LABEL-a.kine.log"

if [[ -n "$VARIANT" ]]; then
  [[ -f "$VARIANT" ]] || die "variant sql not found: $VARIANT"
  log "applying variant $(basename "$VARIANT")"
  docker exec -i "$PG_CONTAINER" psql -U kine -d kine -v ON_ERROR_STOP=1 -q < "$VARIANT" \
    || die "variant failed to apply"
fi

log "starting kine B (watchers) on 2380"
PID_B=$(start_kine 2380 b)
# shellcheck disable=SC2064
trap "kill $PID_A $PID_B 2>/dev/null || true" EXIT
wait_port 2380 || die "kine B never listened — see $RESULTS/$LABEL-b.kine.log"
kill -0 "$PID_A" 2>/dev/null || die "kine A died after startup"

# Record whether the notify trigger is actually installed for this run, so a
# result can never be credited to a variant that silently failed to apply.
docker exec "$PG_CONTAINER" psql -U kine -d kine -tAc \
  "select coalesce(string_agg(tgname, ','), 'NONE') from pg_trigger
    where tgrelid='kine'::regclass and not tgisinternal" \
  > "$RESULTS/$LABEL.triggers.txt"
log "triggers on kine: $(cat "$RESULTS/$LABEL.triggers.txt")"

# -latency-sample-every 1: these are low-rate runs. The default of 1-in-50
# yields ~9 samples over 60 s at 4 puts/s, and percentiles from 9 samples come
# out identical to each other and look like a clean result.
log "running: $DURATION, $WRITERS writers @ ${RATE}/s -> 2379, $WATCHERS watchers -> 2380"
./loadgen/loadgen -endpoint 127.0.0.1:2379 -watch-endpoint 127.0.0.1:2380 \
  -duration "$DURATION" -writers "$WRITERS" -watchers "$WATCHERS" \
  -write-rate "$RATE" -keyspace 50 -latency-sample-every 1 \
  -label "$LABEL" -out "$RESULTS/$LABEL.json"

log "done -> $RESULTS/$LABEL.json"
