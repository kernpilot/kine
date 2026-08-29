#!/usr/bin/env bash
# Payload-size sensitivity, and whether E3's null result survives it.
#
# WHY. Every run in this suite used a 2048-byte value. Real Kubernetes objects
# are not one size: a small ConfigMap is a few hundred bytes, a Deployment with
# managedFields is a few KB, and a bloated CRD or a Helm release secret runs to
# tens or hundreds of KB. PostgreSQL's behaviour is not linear across that
# range — it stores a value inline until roughly 2 KB and moves it to TOAST
# beyond that. 2048 bytes sits directly ON that boundary, which is the single
# least representative point that could have been chosen.
#
# E3 (SET STORAGE EXTERNAL) measured +1.1% at 2 KB and was written off as noise.
# EXTERNAL only changes behaviour for values that actually go out of line, so
# that result says nothing about the sizes where TOAST dominates. This sweep
# crosses the boundary in both directions and re-tests E3 at each size.
set -uo pipefail
cd "$(dirname "$0")"
KINE_BIN="${KINE_BIN:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/kine-patched}"
RESULTS="results"; LOG="$RESULTS/payload.log"; mkdir -p "$RESULTS"
export KINE_MAX_OPEN="${KINE_MAX_OPEN:-80}" KINE_MAX_IDLE="${KINE_MAX_IDLE:-80}"
say() { printf '%s | %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$LOG"; }

say "=== payload-size sweep ==="
for SZ in 512 2048 8192 65536; do
  for arm in default external; do
    variant=""
    [[ "$arm" == "external" ]] && variant="variants/e3-toast-external.sql"
    say "size=${SZ} storage=${arm}"
    VALUE_BYTES="$SZ" KINE_BIN="$KINE_BIN" VARIANT="$variant" \
      timeout 300 ./run.sh "pay-${SZ}-${arm}" 60s 100 256 >>"$LOG" 2>&1 \
      || say "  !! pay-${SZ}-${arm} FAILED"
  done
done
say "=== payload sweep complete ==="
