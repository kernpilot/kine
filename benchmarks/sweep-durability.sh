#!/usr/bin/env bash
# Durability arm, run standalone against the real (btrfs/dm-crypt) volume.
#
# Split out because these are the only experiments where the disk is the
# SUBJECT rather than a confounder: synchronous_commit, WAL sizing,
# shared_buffers and streaming replication are all meaningless on tmpfs, where
# every flush is a memcpy. The cost of using the realistic path is a noisier
# floor, which is why the baseline is re-measured between every candidate
# rather than once.
set -uo pipefail
cd "$(dirname "$0")"
KINE_BIN="${KINE_BIN:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/kine-patched}"
DUR="${DUR:-60s}"; W="${W:-100}"; WATCH="${WATCH:-256}"; REPS="${REPS:-3}"
RESULTS="results"; LOG="$RESULTS/durability.log"; mkdir -p "$RESULTS"
export KINE_MAX_OPEN="${KINE_MAX_OPEN:-80}" KINE_MAX_IDLE="${KINE_MAX_IDLE:-80}"
say() { printf '%s | %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$LOG"; }

dsk() {
  local label="$1" reps="$2" variant="${3:-}"
  for r in $(seq 1 "$reps"); do
    say "  [disk] ${label}-r${r} variant='${variant##*/}'"
    env PG_CONTAINER=kine-bench-pg PG_PORT=55432 PG_EXEC_PORT=5432 \
        KINE_BIN="$KINE_BIN" VARIANT="$variant" \
      timeout 300 ./run.sh "${label}-r${r}" "$DUR" "$W" "$WATCH" \
      >>"$LOG" 2>&1 || say "    !! ${label}-r${r} FAILED"
  done
}

say "=== durability arm start ==="
dsk v2dsk-base-a "$REPS"
dsk v2dsk-syncoff "$REPS" "variants/e7-sync-commit-off.sql"
dsk v2dsk-syncrevert 1 "variants/e7-revert.sql"
dsk v2dsk-base-b "$REPS"
say "=== durability arm complete ==="
