#!/usr/bin/env bash
# PostgreSQL server tuning — the side of this system nothing here has touched.
#
# Every kine number in this suite was measured against a stock `postgres:18.3`
# container. Its defaults are deliberately conservative so the image starts
# anywhere: shared_buffers 128 MB, max_wal_size 1 GB, and a checkpoint policy
# sized for a small database. The kine table reached 1.5 GB in a single 60-second
# run, so the working set has been an order of magnitude larger than the cache
# the whole time. A datastore tuned like that is not a fair reading of what
# Postgres can do for kine — and CNPG in production is not configured this way.
#
# These settings need a server restart, which is why they are a separate phase
# rather than a VARIANT file. Each config restarts the container, waits for
# readiness, verifies the setting is actually in force, runs the profile, then
# resets. `ALTER SYSTEM` writes postgresql.auto.conf, which survives the
# restart; the RESET ALL at the end removes it.
set -uo pipefail

cd "$(dirname "$0")"
KINE_BIN="${KINE_BIN:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/kine-patched}"
PG_CONTAINER="${PG_CONTAINER:-kine-bench-pg}"
DUR="${DUR:-60s}"
W="${W:-100}"
WATCH="${WATCH:-256}"
REPS="${REPS:-2}"
RESULTS="results"
LOG="$RESULTS/pgtune.log"
mkdir -p "$RESULTS"

export KINE_MAX_OPEN="${KINE_MAX_OPEN:-80}"
export KINE_MAX_IDLE="${KINE_MAX_IDLE:-80}"

say() { printf '%s | %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$LOG"; }

wait_pg() {
  for _ in $(seq 1 90); do
    if docker exec "$PG_CONTAINER" pg_isready -U kine -d kine >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  return 1
}

apply_and_run() { # <label> <reps> <setting=value>...
  local label="$1" reps="$2"; shift 2
  say "--- ${label}: $* ---"
  docker exec "$PG_CONTAINER" psql -U kine -d kine -qc "ALTER SYSTEM RESET ALL" >/dev/null 2>&1
  for kv in "$@"; do
    docker exec "$PG_CONTAINER" psql -U kine -d kine -qc \
      "ALTER SYSTEM SET ${kv%%=*} = '${kv#*=}'" >/dev/null 2>&1 \
      || say "  !! could not set $kv"
  done
  docker restart "$PG_CONTAINER" >/dev/null
  wait_pg || { say "  !! postgres did not come back"; return 1; }

  # Verify the settings are IN FORCE, not merely requested. shared_buffers in
  # particular is silently clamped if the container cannot get the shared
  # memory, and a clamped setting would otherwise be reported as a measured
  # non-effect.
  : > "$RESULTS/$label.pgsettings.txt"
  for kv in "$@"; do
    local name="${kv%%=*}"
    printf '%s = %s\n' "$name" \
      "$(docker exec "$PG_CONTAINER" psql -U kine -d kine -tAc "show $name" 2>/dev/null)" \
      >> "$RESULTS/$label.pgsettings.txt"
  done
  say "  in force: $(tr '\n' '; ' < "$RESULTS/$label.pgsettings.txt")"

  for r in $(seq 1 "$reps"); do
    say "  run ${label}-r${r}"
    env PG_CONTAINER="$PG_CONTAINER" PG_PORT=55432 PG_EXEC_PORT=5432 \
        KINE_BIN="$KINE_BIN" timeout 300 ./run.sh "${label}-r${r}" "$DUR" "$W" "$WATCH" \
      >>"$LOG" 2>&1 || say "    !! ${label}-r${r} FAILED"
  done
}

say "=== pg tuning sweep ==="

# Stock defaults, re-measured after a restart so the baseline shares the
# restart's cache-cold starting point with every candidate.
apply_and_run pg-stock "$REPS"

# The working set has been ~10x shared_buffers all along.
apply_and_run pg-buffers "$REPS" shared_buffers=2GB effective_cache_size=6GB

# Checkpoint pressure: at ~5000 writes/s of 2 KB rows, a 1 GB max_wal_size
# forces frequent checkpoints, each one a write storm of its own.
apply_and_run pg-wal "$REPS" max_wal_size=8GB min_wal_size=2GB \
  checkpoint_timeout=30min checkpoint_completion_target=0.9 wal_buffers=64MB

# Both together, which is roughly how a real CNPG instance would be sized.
apply_and_run pg-both "$REPS" shared_buffers=2GB effective_cache_size=6GB \
  max_wal_size=8GB min_wal_size=2GB checkpoint_timeout=30min \
  checkpoint_completion_target=0.9 wal_buffers=64MB

say "resetting postgres to stock"
docker exec "$PG_CONTAINER" psql -U kine -d kine -qc "ALTER SYSTEM RESET ALL" >/dev/null 2>&1
docker restart "$PG_CONTAINER" >/dev/null
wait_pg && say "=== pg tuning sweep complete, server reset to defaults ==="
