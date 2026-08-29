#!/usr/bin/env bash
# Re-measure every claim in kubehz/CHANGELOG.md, upstream versus patched.
#
# THIS IS THE POINT OF KEEPING THE SUITE IN THE FORK. Each patch is justified by
# a number, and every one of those numbers depends on kine's version,
# PostgreSQL's version and the storage underneath. All three move. After a
# rebase onto new upstream, or a PostgreSQL major upgrade, run this: it rebuilds
# both binaries from this repository and re-runs the experiment behind each
# claim, so "is this patch still worth carrying?" is answered by measurement.
#
# How to read the output. Each check prints the recorded figure next to the one
# just measured. They will not match exactly — they were taken on different
# hardware, and this suite's own noise floor ranged from 0.7 % to 72.9 %
# depending on the backend. What matters is whether the EFFECT still holds in
# direction and rough magnitude. A patch whose effect has vanished should be
# dropped and the removal recorded in the changelog.
#
# Usage:  ./setup.sh && ./build.sh && ./verify-patches.sh
set -uo pipefail
cd "$(dirname "$0")"
ROOT="$(cd .. && pwd)"
FAIL=0

say()  { printf '\n\033[1m%s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }
bad()  { printf '\033[31m   !! %s\033[0m\n' "$*"; FAIL=1; }

[[ -x "$ROOT/kine-patched"  ]] || { bad "kine-patched not built — run ./build.sh";  exit 1; }
[[ -x "$ROOT/kine-upstream" ]] || { bad "kine-upstream not built — run ./build.sh"; exit 1; }
docker exec kine-bench-mem pg_isready -U kine -d kine -p 55442 >/dev/null 2>&1 \
  || { bad "backends not up — run ./setup.sh"; exit 1; }

rate() { python3 -c "
import json,sys
try: print('%.1f'%json.load(open(sys.argv[1]))[sys.argv[2]])
except Exception: print('NaN')" "$1" "$2" 2>/dev/null; }

say "P1 — cross-instance watch wake-up"
note "recorded: stock p50 715.7ms / p99 716.6ms  ->  patched p50 20.0ms / p99 21.3ms"
note "measuring (2 kine instances, one database, 4 writers @ 1/s, 60s)..."

KINE_BIN="$ROOT/kine-upstream" ./run-cross.sh verify-p1-upstream 60s 4 16 1 >/dev/null 2>&1
KINE_BIN="$ROOT/kine-patched"  ./run-cross.sh verify-p1-patched  60s 4 16 1 >/dev/null 2>&1

U50=$(rate results/verify-p1-upstream.json watch_delivery_p50_ms)
U99=$(rate results/verify-p1-upstream.json watch_delivery_p99_ms)
P50=$(rate results/verify-p1-patched.json  watch_delivery_p50_ms)
P99=$(rate results/verify-p1-patched.json  watch_delivery_p99_ms)
note "measured : stock p50 ${U50}ms / p99 ${U99}ms  ->  patched p50 ${P50}ms / p99 ${P99}ms"

python3 - "$U99" "$P99" <<'PY'
import sys
try: u, p = float(sys.argv[1]), float(sys.argv[2])
except ValueError: print("   !! could not parse a result — inspect results/verify-p1-*.json"); sys.exit(1)
if p <= 0: print("   !! patched p99 is zero — the measurement did not sample; inspect it"); sys.exit(1)
factor = u / p
print(f"   improvement: {factor:.1f}x at p99")
if factor >= 5:
    print("   VERDICT keep P1 — the effect still holds")
else:
    print("   !! VERDICT P1's benefit no longer reproduces (<5x).")
    print("      Upstream may have fixed cross-instance wake-up. Check whether the")
    print("      poll loop now learns of other instances' writes, and if so drop P1")
    print("      and record the removal in kubehz/CHANGELOG.md.")
    sys.exit(1)
PY
[[ $? -ne 0 ]] && FAIL=1

say "P1 durability invariant — the poll fallback must still cover a dead listener"
note "recorded: killing the LISTEN backend mid-run loses 0 events (p99 degrades, then recovers)"
note "this check is manual — see MERGE-GUIDE.md#p1. Automating it needs a mid-run"
note "pg_terminate_backend, which is timing-dependent and would fail noisily for the"
note "wrong reasons in CI. Run it by hand after any change to notify.go."

say "Configuration findings (no patch, but easy to lose)"
note "these are deployment settings, not code — re-checked by sweep-durability.sh"
note "and sweep-storage.sh rather than here:"
note "  1. set BOTH pool flags; max-idle defaults to 20 against unlimited max-open"
note "  2. give PostgreSQL a fast durable WAL device (97% of a RAM-disk speedup)"
note "  3. synchronous_commit=off is then unnecessary (+3.5% once the WAL is fast)"

if [[ $FAIL -eq 0 ]]; then
  say "ALL CHECKS PASSED — every carried patch still earns its place"
else
  say "SOME CHECKS FAILED — read the notes above before rebasing further"
fi
exit $FAIL
