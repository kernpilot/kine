#!/usr/bin/env bash
# Open-loop and skewed-key measurement, using loadgen2.
#
# Answers two questions the closed-loop driver structurally cannot:
#
#  1. HOW OPTIMISTIC WERE THE PERCENTILES? A closed-loop driver waits for each
#     reply before sending again, so when the server slows the offered load
#     slows with it and the queueing delay never appears in any number. This
#     phase reports latency from each operation's SCHEDULED time, so the two can
#     be compared directly. If they diverge, every p99 reported earlier in this
#     suite was flattering.
#
#  2. DOES SKEW CHANGE THE PICTURE? Kubernetes write access is violently
#     non-uniform — leader-election Leases, node status and endpoints are
#     rewritten every few seconds while most objects sit idle. Uniform keys
#     understate row and page contention on exactly those hot objects.
#
# Runs on tmpfs so that storage, already established as the dominant variable,
# does not drown the effect being measured.
set -uo pipefail
cd "$(dirname "$0")"
RESULTS="results"; LOG="$RESULTS/openloop.log"; mkdir -p "$RESULTS"
export KINE_MAX_OPEN=80 KINE_MAX_IDLE=80
export LOADGEN_BIN=./loadgen2/loadgen2
say() { printf '%s | %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$LOG"; }

[[ -x "$LOADGEN_BIN" ]] || { say "!! loadgen2 not built"; exit 1; }

# Self-check first: prove the new driver runs at all before spending the phase
# on it. Deliberately here rather than earlier — validating it mid-sweep would
# have added load to whichever run was in flight.
say "self-check: 15s smoke with the open-loop driver"
env PG_CONTAINER=kine-bench-mem PG_PORT=55442 PG_EXEC_PORT=55442 \
    KINE_BIN=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/kine-patched WRITE_RATE=200 \
  timeout 200 ./run.sh ol-smoke 15s 4 4 >>"$LOG" 2>&1 || { say "!! smoke FAILED"; exit 1; }
python3 -c "
import json,sys
d=json.load(open('results/ol-smoke.json'))
assert d['puts_ok']>0, 'no writes'
print('smoke ok: puts=%d service_p99=%.1fms scheduled_p99=%.1fms'%(d['puts_ok'],d['put_p99_ms'],d.get('write_scheduled_p99_ms',0)))
" | tee -a "$LOG" || { say "!! smoke produced no usable result"; exit 1; }

run() { # label rate zipf
  say "  ${1}: rate=${2}/writer zipf=${3}"
  env PG_CONTAINER=kine-bench-mem PG_PORT=55442 PG_EXEC_PORT=55442 \
      KINE_BIN=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/kine-patched \
      WRITE_RATE="$2" ZIPF="$3" WARMUP=10s KEYSPACE=500 \
    timeout 300 ./run.sh "$1" 60s 100 256 >>"$LOG" 2>&1 || say "    !! $1 FAILED"
}

say "=== open-loop sweep ==="
# A rate ladder: below, near, and above the ~11k/s ceiling measured on tmpfs.
# Coordinated omission only becomes visible once arrivals outpace service.
for R in 40 80 150; do
  run "ol-uniform-r${R}" "$R" 0
  run "ol-zipf-r${R}"    "$R" 1.2
done
say "=== open-loop sweep complete ==="
