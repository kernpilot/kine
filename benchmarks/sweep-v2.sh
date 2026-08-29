#!/usr/bin/env bash
# Full optimization sweep, v2 — split by what each test is actually studying.
#
# WHY V2 EXISTS. The first sweep was abandoned after three identical baseline
# runs produced 4946 / 3146 / 3062 writes per second — a 61 % spread inside one
# config. The cause was the harness, not kine: every run writes and drops a
# ~1.5 GB table, the Postgres volume sits on btrfs over dm-crypt, and
# copy-on-write plus encryption made consecutive runs non-independent. Load
# average read 218 on a 24-core box, dominated by btrfs I/O completion workers.
#
# THE SPLIT. Two backends, chosen per experiment by what the experiment studies:
#
#   LOGIC arm  (tmpfs, port 55442) — index shape, poll batching, watcher
#     fan-out, payload size, table growth. Here the disk is a confounder, not
#     the subject. Measured noise floor: 5.1 %.
#
#   DURABILITY arm (btrfs disk, port 55432) — synchronous_commit, WAL sizing,
#     shared_buffers, streaming replication. Here the disk IS the subject, so
#     the realistic path is the only honest one, and the cost is a noisier
#     floor that needs more repetitions and interleaved baselines.
#
# Running the logic tests on tmpfs is not cheating: it isolates the variable
# under study. Reporting a tmpfs number as if it were production throughput
# would be, so every result records which backend produced it.
#
# STANDING RESULT, established while fixing the above: kine reached 11 043
# writes/s on tmpfs against 3 100-5 100 on the encrypted-btrfs volume. Storage
# dominates every kine-level knob measured in this suite.
set -uo pipefail

cd "$(dirname "$0")"
KINE_BIN="${KINE_BIN:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/kine-patched}"
DUR="${DUR:-60s}"; W="${W:-100}"; WATCH="${WATCH:-256}"; REPS="${REPS:-3}"
RESULTS="results"; LOG="$RESULTS/sweep-v2.log"; mkdir -p "$RESULTS"
export KINE_MAX_OPEN="${KINE_MAX_OPEN:-80}" KINE_MAX_IDLE="${KINE_MAX_IDLE:-80}"

MEM_C=kine-bench-mem; MEM_P=55442
DSK_C=kine-bench-pg;  DSK_P=55432

say() { printf '%s | %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$LOG"; }

# mem <label> <reps> [kine args] [variant] [extra env assignments]
mem() {
  local label="$1" reps="$2" extra="${3:-}" variant="${4:-}"
  for r in $(seq 1 "$reps"); do
    say "  [mem] ${label}-r${r} extra='${extra}' variant='${variant##*/}'"
    env PG_CONTAINER="$MEM_C" PG_PORT="$MEM_P" KINE_BIN="$KINE_BIN" \
        KINE_EXTRA_ARGS="$extra" VARIANT="$variant" \
        ${VALUE_BYTES:+VALUE_BYTES="$VALUE_BYTES"} \
        ${KEEP_TABLE:+KEEP_TABLE="$KEEP_TABLE"} \
      timeout 300 ./run.sh "${label}-r${r}" "$DUR" "${RUN_W:-$W}" "${RUN_WATCH:-$WATCH}" \
      >>"$LOG" 2>&1 || say "    !! ${label}-r${r} FAILED"
  done
}

dsk() {
  local label="$1" reps="$2" extra="${3:-}" variant="${4:-}"
  for r in $(seq 1 "$reps"); do
    say "  [disk] ${label}-r${r} extra='${extra}' variant='${variant##*/}'"
    env PG_CONTAINER="$DSK_C" PG_PORT="$DSK_P" PG_EXEC_PORT=5432 KINE_BIN="$KINE_BIN" \
        KINE_EXTRA_ARGS="$extra" VARIANT="$variant" \
      timeout 300 ./run.sh "${label}-r${r}" "$DUR" "$W" "$WATCH" \
      >>"$LOG" 2>&1 || say "    !! ${label}-r${r} FAILED"
  done
}

say "================ SWEEP V2 START ================"

############################ LOGIC ARM (tmpfs) ################################
say "=== LOGIC ARM (tmpfs) ==="
mem v2mem-base-a "$REPS"

say "-- poll batch size (default 500) --"
mem v2mem-poll100  2 "--poll-batch-size 100"
mem v2mem-poll2000 2 "--poll-batch-size 2000"

say "-- index shape --"
mem v2mem-idx-covering  2 "" "variants/e2-covering.sql"
mem v2mem-idx-noprevrev 2 "" "variants/e5-drop-prevrev-index.sql"
mem v2mem-idx-fillfac   2 "" "variants/e6-fillfactor.sql"

mem v2mem-base-b "$REPS"     # interleaved baseline: exposes drift

say "-- connection recycling (expected to COST; included as a signed prediction) --"
mem v2mem-connlife30 2 "--datastore-connection-max-lifetime 30s"

say "-- payload size x TOAST storage --"
for SZ in 512 2048 8192 65536; do
  VALUE_BYTES="$SZ" mem "v2mem-pay${SZ}-default"  1
  VALUE_BYTES="$SZ" mem "v2mem-pay${SZ}-external" 1 "" "variants/e3-toast-external.sql"
done

say "-- watcher fan-out --"
for N in 64 256 1024 2048; do
  RUN_W=50 RUN_WATCH="$N" mem "v2mem-watch${N}" 1
done

mem v2mem-base-c "$REPS"     # interleaved baseline

say "-- aged table: compaction off (kine default) vs on --"
for g in 0 1 2 3 4; do
  [[ "$g" == "0" ]] && unset KEEP_TABLE || export KEEP_TABLE=1
  mem "v2mem-aged-off-g${g}" 1
  docker exec "$MEM_C" psql -U kine -d kine -p "$MEM_P" -tAc "select count(*) from kine" \
    > "$RESULTS/v2mem-aged-off-g${g}.rows.txt" 2>/dev/null
  say "    rows=$(cat "$RESULTS/v2mem-aged-off-g${g}.rows.txt" 2>/dev/null)"
done
unset KEEP_TABLE
for g in 0 1 2 3 4; do
  [[ "$g" == "0" ]] && unset KEEP_TABLE || export KEEP_TABLE=1
  mem "v2mem-aged-on-g${g}" 1 "--compact-interval 30s"
  docker exec "$MEM_C" psql -U kine -d kine -p "$MEM_P" -tAc "select count(*) from kine" \
    > "$RESULTS/v2mem-aged-on-g${g}.rows.txt" 2>/dev/null
  say "    rows=$(cat "$RESULTS/v2mem-aged-on-g${g}.rows.txt" 2>/dev/null)"
done
unset KEEP_TABLE

######################### DURABILITY ARM (real disk) ##########################
say "=== DURABILITY ARM (btrfs/dm-crypt disk) ==="
dsk v2dsk-base-a "$REPS"
dsk v2dsk-syncoff "$REPS" "" "variants/e7-sync-commit-off.sql"
dsk v2dsk-syncrevert 1 "" "variants/e7-revert.sql"
dsk v2dsk-base-b "$REPS"

say "================ SWEEP V2 COMPLETE ================"
