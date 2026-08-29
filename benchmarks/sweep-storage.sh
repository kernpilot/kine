#!/usr/bin/env bash
# Additional storage and commit-path experiments.
#
# THE SPLIT-STORAGE QUESTION. Full-PGDATA tmpfs measured 2-3.5x faster than the
# encrypted-btrfs volume, but that swap changes two things at once: table I/O
# AND WAL fsync. Putting ONLY pg_wal on tmpfs separates them. If the split gets
# most of the tmpfs win, the bottleneck is commit fsync and the fix is a fast
# WAL device — which is a real, durable, deployable answer. If it gets little,
# the bottleneck is table/index I/O and no WAL trick will help.
#
# That distinction is worth more than the raw tmpfs number, because a production
# cluster can be given a fast WAL volume and cannot be given a volatile database.
#
# Backends:
#   kine-bench-pg     55432  data + WAL on encrypted btrfs   (baseline)
#   kine-bench-walmem 55452  data on btrfs, pg_wal on tmpfs  (the split)
#   kine-bench-mem    55442  everything on tmpfs             (upper bound)
set -uo pipefail
cd "$(dirname "$0")"
KINE_BIN="${KINE_BIN:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/kine-patched}"
DUR="${DUR:-60s}"; W="${W:-100}"; WATCH="${WATCH:-256}"; REPS="${REPS:-3}"
RESULTS="results"; LOG="$RESULTS/storage.log"; mkdir -p "$RESULTS"
export KINE_MAX_OPEN="${KINE_MAX_OPEN:-80}" KINE_MAX_IDLE="${KINE_MAX_IDLE:-80}"
say() { printf '%s | %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$LOG"; }

on() { # <container> <hostport> <execport> <label> <reps> [variant]
  local c="$1" hp="$2" ep="$3" label="$4" reps="$5" variant="${6:-}"
  for r in $(seq 1 "$reps"); do
    say "  [$c] ${label}-r${r} variant='${variant##*/}'"
    env PG_CONTAINER="$c" PG_PORT="$hp" PG_EXEC_PORT="$ep" \
        KINE_BIN="$KINE_BIN" VARIANT="$variant" \
      timeout 300 ./run.sh "${label}-r${r}" "$DUR" "$W" "$WATCH" \
      >>"$LOG" 2>&1 || say "    !! ${label}-r${r} FAILED"
  done
}

say "=== storage + commit-path sweep ==="

say "-- three storage layouts, same kine, same profile --"
on kine-bench-pg     55432 5432  v3-store-disk   "$REPS"
on kine-bench-walmem 55452 55452 v3-store-walmem "$REPS"
on kine-bench-mem    55442 55442 v3-store-tmpfs  "$REPS"

say "-- group commit (disk only; a flush on tmpfs is a memcpy) --"
on kine-bench-pg 55432 5432 v3-groupcommit "$REPS" variants/e8-group-commit.sql
on kine-bench-pg 55432 5432 v3-groupcommit-revert 1 variants/e8-revert.sql

say "-- aggressive autovacuum, on the realistic volume --"
on kine-bench-pg 55432 5432 v3-autovac "$REPS" variants/e9-autovacuum.sql

say "-- synchronous_commit=off ON THE SPLIT LAYOUT (do the two stack?) --"
on kine-bench-walmem 55452 55452 v3-walmem-syncoff "$REPS" variants/e7-sync-commit-off.sql
on kine-bench-walmem 55452 55452 v3-walmem-revert 1 variants/e7-revert.sql

say "=== storage sweep complete ==="
