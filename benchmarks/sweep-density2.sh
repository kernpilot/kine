#!/usr/bin/env bash
# D2 — N kine instances on one PostgreSQL, one database each: Kamaji's model.
#
# Each TenantControlPlane gets its own kine and its own database on a shared
# PostgreSQL. D1 measured the per-tenant cost at 6-8 connections under load
# (3-6 for upstream kine; P1 adds exactly 2 for its LISTEN and notifier).
# Against max_connections=200 that predicts trouble around N=25-33.
#
# The prediction is the point. If it breaks near there, connections are the
# density ceiling and the arithmetic generalises to a production max_connections.
# If it breaks earlier or degrades some other way, the model is wrong and the
# real limit needs finding.
set -uo pipefail
cd "$(dirname "$0")"
K_ROOT="$(cd .. && pwd)"
KINE_BIN="${KINE_BIN:-$K_ROOT/kine-patched}"
PG_C=kine-dens-pg; PG_PORT=55472
RESULTS=results; LOG="$RESULTS/density2.log"; mkdir -p "$RESULTS"
say() { printf '%s | %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$LOG"; }
q() { docker exec "$PG_C" psql -U kine -d postgres -p "$PG_PORT" -tAqc "$1" 2>/dev/null; }
conns() { docker exec "$PG_C" psql -U kine -d postgres -p "$PG_PORT" -tAqc \
  "SELECT count(*) FROM pg_stat_activity WHERE usename='kine' AND application_name <> 'psql'" 2>/dev/null; }

MAXC=$(docker exec "$PG_C" psql -U kine -d postgres -p "$PG_PORT" -tAqc 'show max_connections' 2>/dev/null)
say "=== D2: N tenants on one PostgreSQL (max_connections=$MAXC) ==="
printf 'N conns rate_total err_pct failed\n' > "$RESULTS/d2-ladder.txt"

for N in 4 8 16 24 32; do
  say "--- N=$N tenant control planes ---"
  PIDS=(); LGS=(); FAILED=0
  for i in $(seq 1 "$N"); do
    q "DROP DATABASE IF EXISTS tenant$i" >/dev/null
    q "CREATE DATABASE tenant$i" >/dev/null
    PORT=$((23000 + i))
    "$KINE_BIN" --endpoint "postgres://kine:kine@localhost:${PG_PORT}/tenant${i}?sslmode=disable" \
      --listen-address "127.0.0.1:${PORT}" --metrics-bind-address 0 \
      --datastore-max-open-connections 5 --datastore-max-idle-connections 5 \
      >"$RESULTS/d2-n${N}-t${i}.kine.log" 2>&1 &
    PIDS+=($!)
  done
  # Wait for all to listen, counting the ones that never do.
  sleep 8
  for i in $(seq 1 "$N"); do
    PORT=$((23000 + i))
    if ! (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; then FAILED=$((FAILED+1)); else exec 3<&- 3>&-; fi
  done
  # Tenant-shaped load against each that came up.
  for i in $(seq 1 "$N"); do
    PORT=$((23000 + i))
    (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null || continue
    exec 3<&- 3>&-
    ./loadgen/loadgen -endpoint "127.0.0.1:${PORT}" -duration 40s -writers 4 -watchers 8 \
      -write-rate 10 -keyspace 200 -label "d2-n${N}-t${i}" -out "$RESULTS/d2-n${N}-t${i}.json" \
      >/dev/null 2>&1 &
    LGS+=($!)
  done
  sleep 20
  C=$(conns)
  for p in "${LGS[@]}"; do wait "$p" 2>/dev/null; done
  for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null; done
  sleep 3
  TOT=$(python3 -c "
import json,glob
d=[json.load(open(f)) for f in glob.glob('$RESULTS/d2-n${N}-t*.json')]
print('%.0f %.2f %d'%(sum(x['put_rate_per_sec'] for x in d), (sum(x['puts_err'] for x in d)/max(sum(x['puts_ok']+x['puts_err'] for x in d),1))*100, len(d)))" 2>/dev/null || echo "0 0 0")
  set -- $TOT
  say "  N=$N conns=${C:-?} aggregate=${1}/s err=${2}% instances_reporting=${3}/$N failed_to_start=$FAILED"
  printf '%s %s %s %s %s\n' "$N" "${C:-0}" "$1" "$2" "$FAILED" >> "$RESULTS/d2-ladder.txt"
  for i in $(seq 1 "$N"); do q "DROP DATABASE IF EXISTS tenant$i" >/dev/null; done
done
say "=== D2 complete ==="
