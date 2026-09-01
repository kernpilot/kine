#!/usr/bin/env bash
# D5 — the density WALL: how far does ONE PostgreSQL take extCP tenants when
# max_connections stops being the pilot default?
#
# The owner question (2026-09-01): the shard tenant caps are 250 soft / 500
# hard — can one datastore carry them, and where is the hard wall? D1/D2
# established ~7 connections per tenant and reproduced the FATAL wall at
# max_connections/7. This sweep LADDERS the two levers those runs pinned:
# the server's max_connections and the per-tenant pool.
#
# PROD-SHAPE FIDELITY (extcp-datastore/cluster.yaml + space_extcp_pg.go):
#   - postgres 18 (live extcp-ds runs 18.4);
#   - reserved_connections=5, authentication_timeout=5s;
#   - each tenant is its OWN role with CONNECTION LIMIT 12 owning its OWN
#     database — the limit changes the failure mode (a hungry tenant fails
#     alone instead of draining the shared pool), so the sweep must carry it;
#   - kine pool 8/8 (extCPKinePoolConns) for the prod-shape configs; a
#     reduced-pool config measures the cheapest density lever.
# KNOWN DIVERGENCE, on purpose: shared_buffers is pinned to 4GB across all
# configs (a trio-share number) so the CONNECTION axis is the only variable;
# the bench host is not the trio — the wall SHAPE transfers, the absolute
# latency numbers do not. The chosen setting gets a confirm-run on prod
# hardware (task #106's pgtune sweep).
#
# Load per tenant: writers 2 @ 5/s, 4 watchers — the quiet-CP shape D1
# measured 7 connections under. At N=500 that is ~2 500-5 000 writes/s
# aggregate; the D-series measured one kine alone sustains ~3 000/s, so the
# aggregate is postgres-bound, which is the point.
#
# GUARDRAIL: the host also runs the dev stack. Every rung checks
# MemAvailable first and SKIPS (recorded, never silent) under 10 GB.
set -uo pipefail
cd "$(dirname "$0")"
K_ROOT="$(cd .. && pwd)"
KINE_BIN="${KINE_BIN:-$K_ROOT/kine-patched}"
PG_IMAGE="${PG_IMAGE:-postgres:18.4}"
PG_C="${PG_C:-kine-wall-pg}"
PG_PORT="${PG_PORT:-55473}"
DUR="${DUR:-60s}"
RESULTS=results; LOG="$RESULTS/density-wall.log"; mkdir -p "$RESULTS"
TABLE="$RESULTS/d5-wall.txt"

say() { printf '%s | %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$LOG"; }
qsu() { docker exec "$PG_C" psql -U postgres -p "$PG_PORT" -tAqc "$1" 2>/dev/null; }
mem_avail_gb() { awk '/MemAvailable/{printf "%.0f", $2/1048576}' /proc/meminfo; }

[[ -x "$KINE_BIN" ]] || { say "!! kine binary missing"; exit 1; }
[[ -x ./loadgen/loadgen ]] || { say "!! loadgen not built"; exit 1; }

pg_up() { # $1 = max_connections
  docker rm -f "$PG_C" >/dev/null 2>&1
  docker run -d --name "$PG_C" --shm-size=2g -p "${PG_PORT}:${PG_PORT}" \
    -e POSTGRES_PASSWORD=postgres "$PG_IMAGE" \
    -p "$PG_PORT" -c "max_connections=$1" -c reserved_connections=5 \
    -c authentication_timeout=5s -c shared_buffers=4GB >/dev/null || return 1
  for _ in $(seq 1 60); do
    docker exec "$PG_C" pg_isready -U postgres -p "$PG_PORT" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}

teardown_tenants() {
  pkill -f "[k]ine-patched.*:${PG_PORT}/" 2>/dev/null
  pkill -f "[l]oadgen -endpoint 127.0.0.1:23" 2>/dev/null
  sleep 2
}

printf 'config maxc pool N started conns pg_rss_gb kine_rss_gb err_pct rate_total p99_ms mem_avail_gb note\n' > "$TABLE"

run_rung() { # $1=config $2=maxc $3=pool $4=N
  local CFG=$1 MAXC=$2 POOL=$3 N=$4 NOTE=""
  local MA; MA=$(mem_avail_gb)
  if (( MA < 10 )); then
    say "-- $CFG N=$N SKIPPED: MemAvailable ${MA}GB < 10GB guard"
    printf '%s %s %s %s - - - - - - - %s skipped-mem\n' "$CFG" "$MAXC" "$POOL" "$N" "$MA" >> "$TABLE"
    return
  fi
  say "--- $CFG: maxc=$MAXC pool=$POOL N=$N (MemAvailable ${MA}GB) ---"
  pg_up "$MAXC" || { say "!! postgres failed to start at maxc=$MAXC"; printf '%s %s %s %s - - - - - - - - pg-start-failed\n' "$CFG" "$MAXC" "$POOL" "$N" >> "$TABLE"; return; }

  # Tenants: role (CONNECTION LIMIT 12, the prod shape) + owned database.
  # Batched into single psql calls — 500 round-trips of docker exec is slow.
  {
    for i in $(seq 1 "$N"); do
      printf "CREATE ROLE t%d LOGIN PASSWORD 'x' CONNECTION LIMIT 12;\nCREATE DATABASE t%d OWNER t%d;\n" "$i" "$i" "$i"
    done
  } | docker exec -i "$PG_C" psql -U postgres -p "$PG_PORT" -q >/dev/null 2>&1

  # Spawn every tenant's kine, staggered so 500 pools do not race the auth path.
  for i in $(seq 1 "$N"); do
    "$KINE_BIN" --endpoint "postgres://t${i}:x@localhost:${PG_PORT}/t${i}?sslmode=disable" \
      --listen-address "127.0.0.1:$((23000 + i))" --metrics-bind-address 0 \
      --datastore-max-open-connections "$POOL" --datastore-max-idle-connections "$POOL" \
      >"$RESULTS/d5-${CFG}-n${N}-t${i}.kine.log" 2>&1 &
    (( i % 50 == 0 )) && sleep 1
  done
  sleep 10
  local STARTED=0
  for i in $(seq 1 "$N"); do
    if (exec 3<>/dev/tcp/127.0.0.1/$((23000 + i))) 2>/dev/null; then exec 3<&- 3>&-; STARTED=$((STARTED+1)); fi
  done
  say "  $STARTED/$N kine listening"

  # Quiet-CP load on every started tenant.
  for i in $(seq 1 "$N"); do
    (exec 3<>/dev/tcp/127.0.0.1/$((23000 + i))) 2>/dev/null || continue
    exec 3<&- 3>&-
    ./loadgen/loadgen -endpoint "127.0.0.1:$((23000 + i))" -duration "$DUR" -writers 2 \
      -watchers 4 -write-rate 5 -keyspace 200 -value-bytes 2048 \
      -label "d5-${CFG}-n${N}-t${i}" -out "$RESULTS/d5-${CFG}-n${N}-t${i}.json" >/dev/null 2>&1 &
    (( i % 50 == 0 )) && sleep 0.5
  done

  # Mid-run sample.
  sleep 35
  local CONNS PGRSS KRSS MA2
  CONNS=$(qsu "SELECT count(*) FROM pg_stat_activity WHERE usename LIKE 't%'")
  PGRSS=$(docker stats --no-stream --format '{{.MemUsage}}' "$PG_C" 2>/dev/null | awk -F/ '{print $1}' | tr -d ' ')
  KRSS=$(ps -o rss= -C kine-patched 2>/dev/null | awk '{s+=$1} END {printf "%.1f", s/1048576}')
  MA2=$(mem_avail_gb)

  # Wait out the loadgens (they exit at -duration; cap the wait).
  local T0=$SECONDS
  while pgrep -f "[l]oadgen -endpoint 127.0.0.1:23" >/dev/null 2>&1; do
    (( SECONDS - T0 > 150 )) && { NOTE="${NOTE}loadgen-timeout;"; pkill -f "[l]oadgen -endpoint 127.0.0.1:23"; break; }
    sleep 3
  done

  # Aggregate.
  local AGG
  AGG=$(python3 - "$RESULTS" "d5-${CFG}-n${N}-t" "$N" <<'PY'
import json, sys, glob
d, prefix, n = sys.argv[1], sys.argv[2], int(sys.argv[3])
errs, rates, p99s, ok = [], [], [], 0
for i in range(1, n + 1):
    try:
        j = json.load(open(f"{d}/{prefix}{i}.json"))
        errs.append(j.get("put_error_rate_pct", 100.0))
        rates.append(j.get("put_rate_per_sec", 0.0))
        p99s.append(j.get("put_p99_ms", 0.0))
        ok += 1
    except Exception:
        errs.append(100.0)
if not ok:
    print("100 0 0 0"); raise SystemExit
# The reported p99 is the WORST tenant's put_p99 at the 99th percentile of
# tenants — a fleet-shaped number: one slow tenant among 500 must show.
p99 = sorted(p99s)[max(0, int(len(p99s) * 0.99) - 1)] if p99s else 0
print(f"{sum(errs)/len(errs):.2f} {sum(rates):.0f} {p99:.0f} {ok}")
PY
)
  local ERR RATE P99 OKN
  read -r ERR RATE P99 OKN <<<"$AGG"
  say "  conns=$CONNS pg_rss=$PGRSS kine_rss=${KRSS}GB err=${ERR}% rate=${RATE}/s p99=${P99}ms (json from $OKN tenants)"
  printf '%s %s %s %s %s %s %s %s %s %s %s %s %s\n' \
    "$CFG" "$MAXC" "$POOL" "$N" "$STARTED" "${CONNS:-?}" "${PGRSS:-?}" "${KRSS:-?}" "$ERR" "$RATE" "$P99" "$MA2" "${NOTE:--}" >> "$TABLE"

  teardown_tenants
  docker rm -f "$PG_C" >/dev/null 2>&1
}

say "=== D5 density wall (image $PG_IMAGE, shared_buffers=4GB pinned) ==="

# A — the instrument check: reproduce the known wall at the prod default.
run_rung A300  300  8 24
run_rung A300  300  8 40
# B — one raise: does the /7 arithmetic hold at 100 tenants?
run_rung B800  800  8 100
# C — the soft target: 250 tenants (predict ~1750 conns).
run_rung C2000 2000 8 250
# D — the hard target: 500 tenants direct (predict ~3500 conns).
run_rung D4000 4000 8 500
# E — the pool lever: 500 tenants at pool 4 (predict ~2000-2500 conns).
run_rung E2600 2600 4 500

say "=== D5 complete ==="
column -t "$TABLE" | tee -a "$LOG"
