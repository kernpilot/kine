#!/usr/bin/env bash
# Resolve the skew question properly: 3 reps per point instead of 1.
#
# Two single runs disagreed. At 15 000/s offered, Zipfian looked worse than
# uniform (scheduled p99 18 034 ms vs 13 688 ms). At 11 000/s it looked much
# better (566 ms vs 2 715 ms). Both were n=1, and a claim was published off the
# first of them — that skew costs "10 % throughput and a 32 % worse tail" —
# which the second contradicts.
#
# A plausible mechanism exists in each direction: concentrating writes on few
# keys keeps those rows and index pages hot in cache, while also concentrating
# contention on the unique index. Which one dominates could genuinely depend on
# how far past the knee the system is. That is a hypothesis, and it is not
# testable with one run per point.
#
# Three reps at each of two rates, one below the knee and one above it.
set -uo pipefail
cd "$(dirname "$0")"
RESULTS="results"; LOG="$RESULTS/zipf.log"; mkdir -p "$RESULTS"
export KINE_MAX_OPEN=80 KINE_MAX_IDLE=80 LOADGEN_BIN=./loadgen2/loadgen2
say() { printf '%s | %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$LOG"; }
while pgrep -f '[l]oadgen|[k]ine-(e1|bench) --endpoint' >/dev/null; do sleep 15; done
say "=== skew resolution: 3 reps per point ==="
for R in 90 120; do
  for Z in 0 1.2; do
    for rep in 1 2 3; do
      say "  rate=$((R*100))/s zipf=$Z rep=$rep"
      env PG_CONTAINER=kine-bench-mem PG_PORT=55442 PG_EXEC_PORT=55442 \
          KINE_BIN=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/kine-patched \
          WRITE_RATE="$R" ZIPF="$Z" WARMUP=10s KEYSPACE=500 \
        timeout 300 ./run.sh "zipf3-r${R}-z${Z}-${rep}" 60s 100 256 >>"$LOG" 2>&1 \
        || say "    !! failed"
    done
  done
done
say "=== skew resolution complete ==="
