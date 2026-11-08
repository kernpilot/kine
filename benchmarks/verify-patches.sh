#!/usr/bin/env bash
# Re-measure every claim in kubehz/CHANGELOG.md against a real PostgreSQL.
#
# THIS IS THE POINT OF KEEPING THE SUITE IN THE FORK. Each patch is justified by
# a number, and every one of those numbers depends on kine's version,
# PostgreSQL's version and the storage underneath. All three move. After an
# upstream sync, or a PostgreSQL major upgrade, run this: it re-runs the
# experiment behind each claim, so "is this patch still worth carrying?" is
# answered by measurement.
#
# How to read the output. Each check prints the recorded figure next to the one
# just measured. They will not match exactly: they were taken on different
# hardware and PostgreSQL versions. What matters is whether the effect still
# holds. A patch whose effect has vanished should be dropped and the removal
# recorded in the changelog.
#
# What it checks:
#   P2 + P3  TestKubehzP2P3Postgres against kine-bench-pg (disk, :55432, from
#            setup.sh). The test opens the driver with New(), as production
#            does. It checks P3's storage parameters on the kine table, then
#            P2's live-size estimate against the real size of the rows.
# P1 (the cross-instance watch wake-up) was dropped on 2026-11-08, and its
# measurement (run-cross.sh, stock against patched) went with it. See
# kubehz/CHANGELOG.md; the last P1 code is on branch archive/p1-port-v0.17.5.
#
# Usage:  ./setup.sh && ./verify-patches.sh
set -uo pipefail
cd "$(dirname "$0")"
ROOT="$(cd .. && pwd)"
FAIL=0

say()  { printf '\n\033[1m%s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }
bad()  { printf '\033[31m   !! %s\033[0m\n' "$*"; FAIL=1; }

# -h 127.0.0.1: the socket answers during the init restart of a new
# container, TCP only after it.
docker exec kine-bench-pg pg_isready -U kine -d kine -h 127.0.0.1 >/dev/null 2>&1 \
  || { bad "kine-bench-pg not up — run ./setup.sh"; exit 1; }

say "P2 + P3 — the live-size estimate and the kine table's storage parameters"
note "recorded: P3 both parameters set after one start"
note "recorded: P2 200 TOAST rows, estimate 13187400 / actual 13107400 = 1.006 (test bound 0.9 to 1.3)"
note "measuring (TestKubehzP2P3Postgres on kine-bench-pg, database kine_kubehz_verify)..."

# The driver creates the database when it is missing. The test truncates the
# table first, so an earlier run does not change the figures.
out=$(cd "$ROOT" && KINE_TEST_PGSQL_DSN="postgres://kine:kine@127.0.0.1:55432/kine_kubehz_verify?sslmode=disable" \
  go test -tags=test -count=1 -v -run '^TestKubehzP2P3Postgres$' ./pkg/drivers/pgsql/ 2>&1)
rc=$?
server=$(docker exec kine-bench-pg psql -U kine -d kine -tAc 'show server_version' 2>/dev/null)
note "PostgreSQL ${server:-unknown}"
ratio=$(grep -m1 'live-size ratio, 200 TOAST rows:' <<<"$out" | sed 's/.*live-size ratio, //')
if grep -q -- '--- SKIP: TestKubehzP2P3Postgres' <<<"$out"; then
  bad "the test skipped: it did not get the database"
elif [[ $rc -ne 0 || -z "$ratio" ]]; then
  bad "TestKubehzP2P3Postgres failed (exit $rc) — the last lines:"
  tail -15 <<<"$out" | sed 's/^/      /'
else
  note "measured: P3 both parameters set (the test checks pg_class.reloptions)"
  note "measured: P2 $ratio"
  note "VERDICT keep P2 and P3 — the estimate is in its bound and the parameters are set"
fi

say "Configuration findings (no patch, but easy to lose)"
note "these are deployment settings, not code — re-checked by sweep-durability.sh"
note "and sweep-storage.sh rather than here:"
note "  1. set BOTH pool flags; max-idle defaults to 20 against unlimited max-open"
note "  2. give PostgreSQL a fast durable WAL device (97% of a RAM-disk speedup)"
note "  3. synchronous_commit=off is then unnecessary (+3.5% once the WAL is fast)"

if [[ $FAIL -eq 0 ]]; then
  say "ALL CHECKS PASSED — every carried patch still earns its place"
else
  say "SOME CHECKS FAILED — read the notes above before syncing further"
fi
exit $FAIL
