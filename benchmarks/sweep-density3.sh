#!/usr/bin/env bash
# D4 — resource cost per tenant control plane: memory and CPU.
#
# The connection model (sweep-density.sh, sweep-density2.sh) answers
# tenants-per-DATABASE. This answers tenants-per-NODE: every tenant runs its own
# kine process and those have to live somewhere.
#
# Runs through PgBouncer on 6432 so connections are not the limiter and the
# resource cost is measured cleanly.
#
# MEASURES BY PROCESS NAME, not by tracked PIDs. An earlier version summed
# /proc/<pid> over an array it maintained, hung on a bare `wait` (the kine
# children never exit), and reported zeros. Counting `kine-patched` processes
# also gives a free correctness check: if the instance count does not equal N,
# something from a previous phase is still running and the sample is
# contaminated — which is exactly how one contaminated row was caught.
set -uo pipefail
cd "$(dirname "$0")"
K_ROOT="$(cd .. && pwd)"
KINE_BIN="${KINE_BIN:-$K_ROOT/kine-patched}"
PG_C="${PG_C:-kine-dens-pg}"; PG_PORT="${PG_PORT:-55472}"; POOL_PORT="${POOL_PORT:-6432}"
RESULTS=results; mkdir -p "$RESULTS"
q(){ docker exec "$PG_C" psql -U kine -d postgres -p "$PG_PORT" -tAqc "$1" 2>/dev/null; }

printf 'N instances kine_rss_mb per_tenant_mb kine_cpu_pct pg_mem\n' > "$RESULTS/d4-resources.txt"
for N in ${TENANTS:-4 16 32}; do
  for i in $(seq 1 "$N"); do q "DROP DATABASE IF EXISTS rt$i" >/dev/null; q "CREATE DATABASE rt$i" >/dev/null; done
  for i in $(seq 1 "$N"); do
    "$KINE_BIN" --endpoint "postgres://kine:kine@localhost:${POOL_PORT}/rt${i}?sslmode=disable" \
      --listen-address "127.0.0.1:$((25000+i))" --metrics-bind-address 0 \
      --datastore-max-open-connections 5 --datastore-max-idle-connections 5 \
      >"$RESULTS/d4-n${N}-t${i}.kine.log" 2>&1 &
  done
  sleep 18
  for i in $(seq 1 "$N"); do
    ./loadgen/loadgen -endpoint "127.0.0.1:$((25000+i))" -duration 40s -writers 4 \
      -watchers 8 -write-rate 10 -keyspace 200 -label "d4-n${N}-t${i}" \
      -out "$RESULTS/d4-n${N}-t${i}.json" >/dev/null 2>&1 &
  done
  sleep 25
  read RSSKB CPU CNT < <(ps -eo rss=,pcpu=,comm= | awk '$3=="kine-patched"{r+=$1;c+=$2;n++} END{print r+0, c+0, n+0}')
  PG=$(docker stats --no-stream --format '{{.MemUsage}}' "$PG_C" 2>/dev/null | awk '{print $1}')
  PER=$(python3 -c "print('%.1f'%($RSSKB/1024/max($CNT,1)))")
  [[ "$CNT" == "$N" ]] || echo "  !! instances=$CNT but N=$N — sample contaminated, discard this row"
  echo "N=$N instances=$CNT kine_rss=$((RSSKB/1024))MB per_tenant=${PER}MB cpu=${CPU}% pg=$PG"
  printf '%s %s %s %s %s %s\n' "$N" "$CNT" "$((RSSKB/1024))" "$PER" "$CPU" "$PG" >> "$RESULTS/d4-resources.txt"
  sleep 22
  pkill -f '[k]ine-patched --endp'; pkill -f '[l]oadgen -endpoint'; sleep 4
  for i in $(seq 1 "$N"); do q "DROP DATABASE IF EXISTS rt$i" >/dev/null; done
done
