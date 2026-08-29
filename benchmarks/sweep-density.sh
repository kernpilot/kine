#!/usr/bin/env bash
# D1/D2 — the density constraint: how many tenant control planes fit on one
# PostgreSQL, and what does each actually cost in CONNECTIONS?
#
# WHY. Kamaji and k0smotron give every tenant its own kine against a shared
# PostgreSQL (own database or schema per TenantControlPlane). This suite
# established that kine needs `max-idle == max-open` or it churns connections
# and fails writes — which means every tenant holds a STANDING pool. At 250-500
# tenants even a small pool implies thousands of connections, and PostgreSQL
# does not serve thousands comfortably. That makes CONNECTIONS, not throughput,
# the plausible density ceiling: one kine sustains ~3 000 writes/s against a
# tenant's single-digit need, a 60-300x margin, so throughput was never going to
# bind first.
#
# That reasoning was recorded as reasoning. This measures it.
#
# D1 — the connection floor for a QUIET tenant. Every pool figure in this suite
#      came from 100-writer saturation runs. A tenant control plane at rest does
#      single-digit writes/s with a handful of watchers. The floor has never
#      been measured, and the whole density arithmetic rests on it.
#
# D2 — N kine instances, one PostgreSQL, one database each (Kamaji's model),
#      each carrying a tenant-shaped load. Find where it actually breaks.
#
# Deliberately run against a backend with max_connections=200 so the breaking
# point is reached on purpose rather than discovered by accident on a box with
# a larger default.
set -uo pipefail
cd "$(dirname "$0")"

K_ROOT="$(cd .. && pwd)"
KINE_BIN="${KINE_BIN:-$K_ROOT/kine-patched}"
PG_C="${PG_C:-kine-dens-pg}"
PG_PORT="${PG_PORT:-55472}"
RESULTS="results"; LOG="$RESULTS/density.log"; mkdir -p "$RESULTS"

say() { printf '%s | %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$LOG"; }
q() { docker exec "$PG_C" psql -U kine -d "${2:-kine}" -p "$PG_PORT" -tAqc "$1" 2>/dev/null; }

# Connections attributable to kine, excluding our own measuring session.
conns() { q "SELECT count(*) FROM pg_stat_activity WHERE usename='kine' AND application_name <> 'psql'"; }

[[ -x "$KINE_BIN" ]] || { say "!! kine binary missing at $KINE_BIN"; exit 1; }
[[ -x ./loadgen/loadgen ]] || { say "!! loadgen not built"; exit 1; }
docker exec "$PG_C" pg_isready -U kine -d kine -p "$PG_PORT" >/dev/null 2>&1 \
  || { say "!! $PG_C not ready"; exit 1; }

MAXC=$(q "show max_connections")
say "=== density sweep · backend $PG_C, max_connections=$MAXC ==="

############################ D1 — the connection floor ########################
# A tenant control plane at rest: a few writes/s, a handful of watchers.
D1_RATE="${D1_RATE:-10}"      # writes/s for the whole tenant
D1_WRITERS="${D1_WRITERS:-4}"
D1_WATCH="${D1_WATCH:-8}"

say "--- D1: connection floor for a quiet tenant (${D1_WRITERS} writers @ $((D1_RATE)) /s each, ${D1_WATCH} watchers) ---"
for POOL in 2 5 10 20; do
  q "DROP TABLE IF EXISTS kine CASCADE" >/dev/null
  "$KINE_BIN" --endpoint "postgres://kine:kine@localhost:${PG_PORT}/kine?sslmode=disable" \
    --listen-address 127.0.0.1:2379 --metrics-bind-address 0 \
    --datastore-max-open-connections "$POOL" --datastore-max-idle-connections "$POOL" \
    >"$RESULTS/d1-pool${POOL}.kine.log" 2>&1 &
  KP=$!
  for _ in $(seq 1 60); do (exec 3<>/dev/tcp/127.0.0.1/2379) 2>/dev/null && { exec 3<&- 3>&-; break; }; sleep 0.25; done
  if ! kill -0 "$KP" 2>/dev/null; then say "  pool=$POOL: kine failed to start"; continue; fi

  ./loadgen/loadgen -endpoint 127.0.0.1:2379 -duration 45s -writers "$D1_WRITERS" \
    -watchers "$D1_WATCH" -write-rate "$D1_RATE" -keyspace 200 -value-bytes 2048 \
    -label "d1-pool${POOL}" -out "$RESULTS/d1-pool${POOL}.json" >/dev/null 2>&1 &
  LG=$!
  sleep 20
  PEAK=$(conns)                      # measured mid-run, under load
  RSS=$(awk '/^VmRSS/{printf "%.0f", $2/1024}' "/proc/$KP/status" 2>/dev/null)
  wait $LG 2>/dev/null
  kill "$KP" 2>/dev/null; sleep 2

  ERR=$(python3 -c "import json;print('%.2f'%json.load(open('$RESULTS/d1-pool${POOL}.json'))['put_error_rate_pct'])" 2>/dev/null || echo '-')
  RATE=$(python3 -c "import json;print('%.0f'%json.load(open('$RESULTS/d1-pool${POOL}.json'))['put_rate_per_sec'])" 2>/dev/null || echo '-')
  printf 'pool=%-3s conns_in_use=%-4s achieved=%-6s err=%-6s kine_rss=%sMB\n' \
    "$POOL" "${PEAK:-?}" "$RATE" "${ERR}%" "${RSS:-?}" | tee -a "$LOG"
  printf '%s %s %s %s\n' "$POOL" "${PEAK:-0}" "${RATE:-0}" "${ERR:-0}" >> "$RESULTS/d1-floor.txt"
done

say "=== D1 complete — see $RESULTS/d1-floor.txt ==="
