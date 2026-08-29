#!/usr/bin/env bash
# Throughput vs BATCH SIZE, at realistic concurrency.
#
# The first probe compared kine's per-transaction insert against a 2000-row
# batch and reported 96x. That number is real but misleading as a design input:
# the baseline was a SEQUENTIAL single connection, while kine runs ~100
# concurrent writers whose commits PostgreSQL already group-commits for free.
# kine measured 3189 writes/s on this same disk, not the 171/s that probe showed.
#
# A batching implementation cannot wait for 2000 writes either — at a few
# thousand writes/s a 10 ms window collects a few dozen. So the decision-relevant
# question is the shape of the curve at small batch sizes, run at concurrency.
set -uo pipefail
CT="${1:-kine-bench-pg}"; PORT="${2:-5432}"; CONC="${3:-8}"; TOTAL="${4:-8000}"
q() { docker exec "$CT" psql -U kine -d kine -p "$PORT" -tAqc "$1"; }
PAY="decode(md5(random()::text)||md5(random()::text)||md5(random()::text)||md5(random()::text),'hex')"
BIG="($PAY||$PAY||$PAY||$PAY||$PAY||$PAY||$PAY||$PAY||$PAY||$PAY||$PAY||$PAY||$PAY||$PAY||$PAY||$PAY)"

q "DROP TABLE IF EXISTS bc; CREATE TABLE bc (
  id BIGSERIAL PRIMARY KEY, name text COLLATE \"C\",
  created INTEGER, deleted INTEGER, create_revision BIGINT,
  prev_revision BIGINT, lease INTEGER, value bytea, old_value bytea);
CREATE INDEX bc_name_id ON bc(name,id);
CREATE UNIQUE INDEX bc_name_prev ON bc(name, prev_revision);" >/dev/null

printf '%-10s %10s %12s %8s\n' 'batch' 'ms' 'rows/s' 'vs b=1'
BASE=0
for K in 1 5 10 30 100 500; do
  q "TRUNCATE bc" >/dev/null
  PER=$(( TOTAL / CONC ))          # rows per worker
  ITERS=$(( PER / K )); (( ITERS < 1 )) && ITERS=1
  S=$(date +%s%N)
  for w in $(seq 1 "$CONC"); do
    docker exec "$CT" psql -U kine -d kine -p "$PORT" -tAqc "
      DO \$\$ DECLARE i int; BEGIN FOR i IN 1..$ITERS LOOP
        INSERT INTO bc(name,created,deleted,create_revision,prev_revision,lease,value)
        SELECT '/k/'||$w||'-'||i||'-'||g,1,0,g,0,0,$BIG FROM generate_series(1,$K) g;
        COMMIT; END LOOP; END \$\$;" >/dev/null 2>&1 &
  done
  wait
  E=$(date +%s%N); T=$(( (E-S)/1000000 )); (( T < 1 )) && T=1
  ROWS=$(( ITERS * K * CONC ))
  RS=$(echo "$ROWS $T" | awk '{printf "%.0f", $1/($2/1000)}')
  [[ "$K" == "1" ]] && BASE=$RS
  printf '%-10s %10s %12s %7.1fx\n' "$K" "$T" "$RS" "$(echo "$RS $BASE" | awk '{print $1/$2}')"
done
q "DROP TABLE IF EXISTS bc" >/dev/null
