#!/usr/bin/env bash
# Find the USABLE ceiling, not the saturation point.
#
# The open-loop phase showed that at 15 000 offered writes/s the closed-loop
# harness reported p99 40 ms while a client actually waited 13.7 SECONDS. Below
# the knee the two agree within a millisecond. So "11 000 writes/s" is not a
# capacity number — it is the throughput observed while latency was already
# collapsing, which is precisely the reading the article this came from warns
# benchmarks produce.
#
# This walks the region between the last healthy point (8 000/s, scheduled p99
# 22 ms) and the collapsed one, reporting SCHEDULED latency. The usable ceiling
# is the highest rate whose scheduled p99 stays in the same order of magnitude
# as its service p99 — past that the system is accepting work it cannot retire.
set -uo pipefail
cd "$(dirname "$0")"
RESULTS="results"; LOG="$RESULTS/knee.log"; mkdir -p "$RESULTS"
export KINE_MAX_OPEN=80 KINE_MAX_IDLE=80 LOADGEN_BIN=./loadgen2/loadgen2
say() { printf '%s | %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$LOG"; }

run() { # per-writer rate, zipf
  say "  rate=${1}/writer (=$((1 * $1 * 100))/s offered) zipf=${2}"
  env PG_CONTAINER=kine-bench-mem PG_PORT=55442 PG_EXEC_PORT=55442 \
      KINE_BIN=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/kine-patched \
      WRITE_RATE="$1" ZIPF="$2" WARMUP=10s KEYSPACE=500 \
    timeout 300 ./run.sh "knee-z${2}-r${1}" 60s 100 256 >>"$LOG" 2>&1 \
    || say "    !! failed"
}

say "=== knee ladder ==="
for R in 90 100 110 120; do
  run "$R" 0
  run "$R" 1.2
done
say "=== knee ladder complete ==="
