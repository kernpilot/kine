#!/usr/bin/env bash
# Watcher scaling — does kine hold up at the fan-out a dense multi-tenant plane
# implies?
#
# WHY. The density model this whole evaluation exists to test is 250-500 tenant
# control planes. Each one runs controllers holding watches. Every run so far
# used 256 watchers against a single kine, which is a plausible number for ONE
# busy cluster and a low number for a shared plane. kine fans a single poll loop
# out to every subscriber through one broadcaster under a mutex
# (broadcaster.go:64), so watcher count is a specific and unexamined scaling
# axis — and the drop-and-unsubscribe path on a full subscriber buffer lives
# right there.
set -uo pipefail
cd "$(dirname "$0")"
KINE_BIN="${KINE_BIN:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/kine-patched}"
RESULTS="results"; LOG="$RESULTS/watchers.log"; mkdir -p "$RESULTS"
export KINE_MAX_OPEN="${KINE_MAX_OPEN:-80}" KINE_MAX_IDLE="${KINE_MAX_IDLE:-80}"
say() { printf '%s | %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$LOG"; }

say "=== watcher-scaling sweep ==="
for N in 64 256 1024 2048; do
  say "watchers=${N}"
  KINE_BIN="$KINE_BIN" timeout 300 ./run.sh "watch-${N}" 60s 50 "$N" >>"$LOG" 2>&1 \
    || say "  !! watch-${N} FAILED"
done
say "=== watcher sweep complete ==="
