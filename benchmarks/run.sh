#!/usr/bin/env bash
# kine watch-churn benchmark runner.
#
# Usage: ./run.sh <label> [duration] [writers] [watchers]
#
# Every run starts from a TRUNCATED kine table. That is deliberate: kine's
# list queries scan by revision, so a table left populated by the previous
# run makes each successive benchmark slower than the last and every
# "optimization" look worse than it is. Comparing runs of different table
# sizes is the easiest way to produce a confidently wrong answer here.
#
# The DDL variants (E2/E3/E4) are applied AFTER kine creates its schema and
# BEFORE load starts — kine runs CREATE TABLE/INDEX IF NOT EXISTS at boot,
# so applying them earlier just gets overwritten.
set -euo pipefail

cd "$(dirname "$0")"
LABEL="${1:?usage: run.sh <label> [duration] [writers] [watchers]}"
DURATION="${2:-60s}"
WRITERS="${3:-16}"
WATCHERS="${4:-64}"
VARIANT="${VARIANT:-}"   # optional path to a .sql applied post-schema

KINE_BIN="${KINE_BIN:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/kine-upstream}"
# TWO BACKENDS, on purpose.
#   kine-bench-pg  (55432) — data on btrfs over dm-crypt. The realistic path,
#                            and the only place durability settings mean
#                            anything (synchronous_commit, WAL, replication).
#   kine-bench-mem (55442) — data on tmpfs. Used for everything that studies
#                            kine's LOGIC (indexes, poll batching, watcher
#                            fan-out, payload size), where disk is a confounder
#                            rather than the subject.
# Repeatedly writing and dropping a ~1.5 GB table on copy-on-write encrypted
# storage made consecutive runs non-independent: three identical baseline runs
# came out 4946 / 3146 / 3062 writes per second, a 61 % spread. Separating the
# variable under study from the one confounding it is the fix.
PG_CONTAINER="${PG_CONTAINER:-kine-bench-pg}"
# PG_PORT is the HOST-side port kine dials. PG_EXEC_PORT is the port Postgres
# listens on INSIDE the container, which is not the same thing whenever the
# container publishes a remapped port: kine-bench-pg listens on 5432 and is
# published as 55432, while kine-bench-mem genuinely runs on 55442. Collapsing
# the two made every in-container psql fail with "No such file or directory" on
# the socket, which would have taken out the entire durability arm.
PG_PORT="${PG_PORT:-55432}"
PG_EXEC_PORT="${PG_EXEC_PORT:-$PG_PORT}"
DSN="postgres://kine:kine@localhost:${PG_PORT}/kine?sslmode=disable"
RESULTS="results"
mkdir -p "$RESULTS"

# Every psql in this script goes through here so the port can never drift from
# the DSN the benchmark actually used.
pg() { docker exec "$PG_CONTAINER" psql -U kine -d kine -p "$PG_EXEC_PORT" "$@"; }
pgi() { docker exec -i "$PG_CONTAINER" psql -U kine -d kine -p "$PG_EXEC_PORT" "$@"; }

log() { printf '\033[36m»\033[0m %s\n' "$*"; }
die() { printf '\033[31m✗\033[0m %s\n' "$*" >&2; exit 1; }

# MEMORY GATE. kine's watch buffers are bounded in BATCHES, not bytes:
# 100 slots in the broadcaster (broadcaster.go:28) plus 100 in Watch
# (sql.go:414), each holding up to --poll-batch-size events, each carrying the
# full object value. At 256 watchers and 2 KB values that is ~52 GB of headroom
# kine will happily use. A run of this suite took the host to 120 GB of 124 GB
# with all swap consumed and had to be killed. Refuse to start a run that cannot
# afford it.
MIN_FREE_GB="${MIN_FREE_GB:-25}"
AVAIL_GB=$(awk '/MemAvailable/{printf "%d", $2/1048576}' /proc/meminfo)
[[ "$AVAIL_GB" -ge "$MIN_FREE_GB" ]] \
  || die "only ${AVAIL_GB} GB available, need ${MIN_FREE_GB} — refusing to start (set MIN_FREE_GB to override)"

docker exec "$PG_CONTAINER" pg_isready -U kine -d kine -p "$PG_EXEC_PORT" >/dev/null 2>&1 \
  || die "postgres container '$PG_CONTAINER' is not ready on port $PG_EXEC_PORT"
[[ -x "$KINE_BIN" ]] || die "kine binary not found at $KINE_BIN"
# LOADGEN_BIN lets a phase swap in the open-loop driver (loadgen2) without
# touching the binary a running sweep is mid-way through using — rebuilding
# loadgen under a live sweep would make its later runs incomparable with its
# earlier ones, silently.
LOADGEN_BIN="${LOADGEN_BIN:-./loadgen/loadgen}"
[[ -x "$LOADGEN_BIN" ]] || die "load generator not built at $LOADGEN_BIN"

# Fresh table for every run — see header. KEEP_TABLE=1 suppresses the reset so a
# run can measure an AGED table: kine's compaction is OFF by default, so a real
# deployment's table grows without bound, and a benchmark that always starts
# empty measures a state production never occupies.
if [[ -n "${KEEP_TABLE:-}" ]]; then
  log "KEEPING existing table (aged-table run)"
else
  log "resetting kine table"
  pg -qc 'DROP TABLE IF EXISTS kine CASCADE' >/dev/null
fi

log "starting kine (label=$LABEL)"
# --metrics-bind-address 0 disables the metrics listener: it defaults to
# :8080, which collides with anything else on the box. kine creates its
# schema BEFORE binding that port, so a collision leaves a table behind and
# then exits — which is why the liveness check below tests the SERVING port
# and not just the table.
# KINE_MAX_OPEN: kine defaults --datastore-max-open-connections to 0, which
# means UNLIMITED (generic.go:139 SetMaxOpenConns). Against a Postgres with
# max_connections=100 that produces "sorry, too many clients already" under
# concurrency — which looks exactly like write-contention errors but is a
# pool misconfiguration. Left at 0 unless the caller sets it, so the
# baseline reflects kine's real defaults.
"$KINE_BIN" --endpoint "$DSN" --listen-address 127.0.0.1:2379 \
  --metrics-bind-address 0 \
  ${KINE_MAX_OPEN:+--datastore-max-open-connections "$KINE_MAX_OPEN"} \
  ${KINE_MAX_IDLE:+--datastore-max-idle-connections "$KINE_MAX_IDLE"} \
  ${KINE_EXTRA_ARGS:-} \
  >"$RESULTS/$LABEL.kine.log" 2>&1 &
KINE_PID=$!
# shellcheck disable=SC2064
trap "kill $KINE_PID 2>/dev/null || true" EXIT

for _ in $(seq 1 40); do
  pg -tAc "select to_regclass('public.kine')" 2>/dev/null | grep -q '^kine$' && break
  sleep 0.5
done
pg -tAc "select to_regclass('public.kine')" \
  | grep -q '^kine$' || die "kine never created its schema — see $RESULTS/$LABEL.kine.log"

# The table existing does NOT prove kine is serving — it creates the schema
# before it binds. Verify the process is alive AND the port answers, or a
# dead kine silently becomes a hung loadgen.
kill -0 "$KINE_PID" 2>/dev/null || die "kine exited after creating its schema — see $RESULTS/$LABEL.kine.log"
for _ in $(seq 1 40); do
  (exec 3<>/dev/tcp/127.0.0.1/2379) 2>/dev/null && { exec 3<&- 3>&-; break; }
  sleep 0.25
done
(exec 3<>/dev/tcp/127.0.0.1/2379) 2>/dev/null || die "kine is not accepting connections on 2379 — see $RESULTS/$LABEL.kine.log"
exec 3<&- 3>&- 2>/dev/null || true

# RSS WATCHDOG. Samples kine's resident set once a second, records the peak
# alongside the result (peak memory is a measurement worth having, not just a
# safety limit), and kills the run if kine exceeds the cap. Without this the
# only backstop is the kernel OOM killer, which takes whatever it likes on a
# shared machine.
KINE_RSS_CAP_GB="${KINE_RSS_CAP_GB:-12}"
CAP_KB=$(( KINE_RSS_CAP_GB * 1024 * 1024 ))
rm -f "$RESULTS/$LABEL.rss-abort.txt"
(
  peak=0
  while kill -0 "$KINE_PID" 2>/dev/null; do
    rss=$(awk '/^VmRSS:/{print $2}' "/proc/$KINE_PID/status" 2>/dev/null || true)
    [[ -n "${rss:-}" ]] || rss=0
    if (( rss > peak )); then
      peak=$rss
      # Written EVERY time it grows, not once at the end. The first version
      # wrote the peak only after the loop exited, and the EXIT trap killed this
      # subshell first — so the file never appeared and the whole guard silently
      # measured nothing. Caught by deliberately setting the cap to 1 GB and
      # watching it fail to fire.
      printf '%d\n' "$peak" > "$RESULTS/$LABEL.rss-peak-kb.txt"
    fi
    if (( rss > CAP_KB )); then
      printf 'ABORTED: kine RSS %d kB exceeded cap %d kB (%d GB)\n' \
        "$rss" "$CAP_KB" "$KINE_RSS_CAP_GB" > "$RESULTS/$LABEL.rss-abort.txt"
      kill -9 "$KINE_PID" 2>/dev/null
      pkill -9 -f '[l]oadgen' 2>/dev/null
      break
    fi
    sleep 1
  done
) &
RSS_WATCHDOG=$!
# shellcheck disable=SC2064
trap "kill $KINE_PID $RSS_WATCHDOG 2>/dev/null || true" EXIT

if [[ -n "$VARIANT" ]]; then
  [[ -f "$VARIANT" ]] || die "variant sql not found: $VARIANT"
  log "applying variant $(basename "$VARIANT")"
  pgi -v ON_ERROR_STOP=1 -q < "$VARIANT" \
    || die "variant failed to apply"
fi

# Record what the table ACTUALLY looks like for this run, so a result can
# never be attributed to a variant that silently failed to apply.
pg -tAc "select indexname from pg_indexes where tablename='kine' order by 1" \
  > "$RESULTS/$LABEL.indexes.txt"
pg -tAc "select a.attname, a.attstorage, c.relpersistence from pg_attribute a
     join pg_class c on c.oid=a.attrelid
    where c.relname='kine' and a.attname in ('value','old_value')" \
  > "$RESULTS/$LABEL.storage.txt"

# The machine is shared: a kind cluster and other work run on it. Record load
# before and after, so a run taken under contention can be identified later
# rather than quietly averaged into a mean.
{
  echo "backend=$PG_CONTAINER port=$PG_PORT"
  echo "loadavg_before=$(cut -d' ' -f1-3 /proc/loadavg)"
} > "$RESULTS/$LABEL.env.txt"

log "running load: $DURATION, $WRITERS writers, $WATCHERS watchers"
# WRITE_RATE caps each writer's puts/sec. An earlier run set this as an env
# var while loadgen only read the -write-rate FLAG, so a run labelled
# "100 tenants @ 20 puts/s" was in fact unthrottled. Pass it through.
"$LOADGEN_BIN" -endpoint 127.0.0.1:2379 -duration "$DURATION" \
  -writers "$WRITERS" -watchers "$WATCHERS" -label "$LABEL" \
  ${WRITE_RATE:+-write-rate "$WRITE_RATE"} \
  ${KEYSPACE:+-keyspace "$KEYSPACE"} \
  ${VALUE_BYTES:+-value-bytes "$VALUE_BYTES"} \
  ${ZIPF:+-zipf "$ZIPF"} \
  ${WARMUP:+-warmup "$WARMUP"} \
  -out "$RESULTS/$LABEL.json"

TBL=$(pg -tAc \
  "select pg_size_pretty(pg_table_size('kine'))||' table, '||pg_size_pretty(pg_indexes_size('kine'))||' indexes'")
log "sizes: $TBL"
echo "$TBL" > "$RESULTS/$LABEL.sizes.txt"
echo "loadavg_after=$(cut -d' ' -f1-3 /proc/loadavg)" >> "$RESULTS/$LABEL.env.txt"
if [[ -f "$RESULTS/$LABEL.rss-peak-kb.txt" ]]; then
  log "kine peak RSS: $(awk '{printf "%.1f GB", $1/1048576}' "$RESULTS/$LABEL.rss-peak-kb.txt")"
fi
[[ -f "$RESULTS/$LABEL.rss-abort.txt" ]] && log "!! $(cat "$RESULTS/$LABEL.rss-abort.txt")"
log "done -> $RESULTS/$LABEL.json"
