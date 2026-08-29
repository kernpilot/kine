#!/usr/bin/env bash
# Copy-on-write vs nodatacow, same disk, same encryption.
#
# WHY. The disk arm's noise floor is 72.9 % — two baseline passes came in at
# 5242 and 3143 writes/s — which makes every effect smaller than ~70 %
# unmeasurable there. shared_buffers, WAL sizing and checkpoint tuning all
# vanished into it. The cause is btrfs copy-on-write: the benchmark rewrites a
# large table continuously, and COW turns each overwrite into a new extent plus
# metadata churn, so the filesystem degrades as the run proceeds and never
# returns to the same starting state.
#
# `chattr +C` on an empty directory disables COW for files created inside it.
# It is the standard recommendation for databases on btrfs, and it is a
# CONTROLLED comparison here: same physical volume, same dm-crypt layer, same
# PostgreSQL, only the COW flag differs.
#
# Two questions, both worth answering:
#   1. Does nodatacow raise throughput on this storage?
#   2. Does it lower the VARIANCE enough to make the disk arm usable — that is,
#      to make the settings that vanished into the noise measurable at all?
#
# Question 2 is the more valuable one. A tighter floor would let the pgtune
# results be re-measured instead of written off as unmeasured.
set -uo pipefail
cd "$(dirname "$0")"
RESULTS="results"; LOG="$RESULTS/cow.log"; mkdir -p "$RESULTS"
export KINE_MAX_OPEN=80 KINE_MAX_IDLE=80
say() { printf '%s | %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$LOG"; }

while pgrep -f '[l]oadgen|[k]ine-(e1|bench) --endpoint' >/dev/null; do sleep 15; done
say "machine idle; starting"

run() { # container hostport execport label
  say "  [$1] $4"
  env PG_CONTAINER="$1" PG_PORT="$2" PG_EXEC_PORT="$3" \
      KINE_BIN=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/kine-patched \
    timeout 300 ./run.sh "$4" 60s 100 256 >>"$LOG" 2>&1 || say "    !! $4 FAILED"
}

say "=== COW vs NOCOW ==="
# Interleaved, not blocked: alternating the two arms means a machine that drifts
# during the sweep drifts through BOTH, instead of favouring whichever ran first.
for r in 1 2 3 4; do
  run kine-bench-pg    55432 5432  "cow-yes-r${r}"
  run kine-bench-nocow 55462 55462 "cow-no-r${r}"
done
say "=== COW sweep complete ==="
