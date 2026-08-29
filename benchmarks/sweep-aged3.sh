#!/usr/bin/env bash
# Aged-table arm, third attempt — now instrumented for MEMORY as well as speed.
#
# The previous attempts died twice: once when the harness killed the sweep and
# my resume raced its own generation, once when I ran a probe concurrently with
# a live measurement. The third failure was more interesting: the run drove the
# host to 120 GB of 124 GB used, kine alone at 61.7 GB.
#
# A fresh-table run with the same 256 watchers peaks at 0.84 GB. So the buffer
# arithmetic (200 batches x 500 events x value size per watcher) is the WORST
# case, and what makes it materialize is consumers or the poll loop falling
# behind — which is exactly what a growing table causes. That is the hypothesis
# this run tests, with peak RSS recorded per generation.
#
# The RSS watchdog in run.sh caps kine at KINE_RSS_CAP_GB and aborts the run
# rather than letting the host OOM. Hitting the cap is a RESULT here, not a
# failure: it locates the table size at which kine's watch buffers run away.
set -uo pipefail
cd "$(dirname "$0")"
RESULTS="results"; LOG="$RESULTS/aged3.log"; mkdir -p "$RESULTS"
export KINE_MAX_OPEN=80 KINE_MAX_IDLE=80
export KINE_RSS_CAP_GB="${KINE_RSS_CAP_GB:-12}"
export MIN_FREE_GB="${MIN_FREE_GB:-30}"
say() { printf '%s | %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$LOG"; }

while pgrep -f '[l]oadgen|[k]ine-(e1|bench) --endpoint' >/dev/null; do sleep 15; done
say "=== aged-table arm, memory-instrumented (cap ${KINE_RSS_CAP_GB} GB) ==="

arm() { # name  extra-kine-args
  say "--- arm $1 ---"
  for g in 0 1 2 3 4 5; do
    local keep=""; [[ "$g" != "0" ]] && keep=1
    say "  $1 g${g}"
    env PG_CONTAINER=kine-bench-mem PG_PORT=55442 PG_EXEC_PORT=55442 \
        KINE_BIN=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/kine-patched \
        KINE_EXTRA_ARGS="$2" ${keep:+KEEP_TABLE=1} \
      timeout 300 ./run.sh "aged3-$1-g${g}" 60s 100 256 >>"$LOG" 2>&1 || true
    local rows rate peak
    rows=$(docker exec kine-bench-mem psql -U kine -d kine -p 55442 -tAc \
      "select count(*) from kine" 2>/dev/null || echo '?')
    rate=$(python3 -c "import json;print('%.0f'%json.load(open('$RESULTS/aged3-$1-g${g}.json'))['put_rate_per_sec'])" 2>/dev/null || echo '-')
    peak=$(awk '{printf "%.2f", $1/1048576}' "$RESULTS/aged3-$1-g${g}.rss-peak-kb.txt" 2>/dev/null || echo '-')
    say "    rows=$rows rate=$rate peakRSS=${peak}GB $(test -f "$RESULTS/aged3-$1-g${g}.rss-abort.txt" && echo '<< HIT CAP')"
  done
}

arm nocompact ""
arm compact "--compact-interval 30s"
say "=== aged3 complete ==="
