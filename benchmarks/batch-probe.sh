#!/usr/bin/env bash
# What could write batching buy kine?
#
# kine issues ONE INSERT per etcd transaction and commits it. Every commit is an
# fsync, and fsync has measured as the dominant cost in this suite (moving pg_wal
# to tmpfs was worth 3.5x; synchronous_commit=off was worth +124%). If N writes
# can share one commit, that cost is divided by N.
#
# This measures the ceiling at the SQL layer — no kine change required — using
# kine's real column shape and a realistic 2 KB incompressible payload:
#
#   A  one INSERT per transaction        (what kine does today)
#   B  N INSERTs inside one transaction  (batching, same statements)
#   C  one multi-row INSERT              (batching + one round trip)
#   D  COPY                              (the article's suggestion)
#
# B and C are the interesting ones: both are reachable from kine's architecture,
# because a multi-row INSERT ... RETURNING id still hands every caller its own
# revision. D is included to show whether COPY beats a multi-row INSERT enough
# to matter, given COPY cannot RETURNING and so cannot return revisions at all.
set -uo pipefail
CT="${1:-kine-bench-pg}"; PORT="${2:-5432}"; N="${3:-2000}"
q() { docker exec "$CT" psql -U kine -d kine -p "$PORT" -tAqc "$1"; }

q "DROP TABLE IF EXISTS bp; CREATE TABLE bp (
  id BIGSERIAL PRIMARY KEY, name text COLLATE \"C\",
  created INTEGER, deleted INTEGER, create_revision BIGINT,
  prev_revision BIGINT, lease INTEGER, value bytea, old_value bytea);
CREATE INDEX bp_name_id ON bp(name,id);
CREATE UNIQUE INDEX bp_name_prev ON bp(name, prev_revision);" >/dev/null

# 2 KB incompressible payload, as used throughout this suite.
PAY="decode(md5(random()::text)||md5(random()::text)||md5(random()::text)||md5(random()::text), 'hex')"
BIG="($PAY||$PAY||$PAY||$PAY||$PAY||$PAY||$PAY||$PAY||$PAY||$PAY||$PAY||$PAY||$PAY||$PAY||$PAY||$PAY)"

echo "backend=$CT rows=$N payload=~2KB"
echo

# A — one insert per transaction (kine's behaviour today)
q "TRUNCATE bp" >/dev/null
A=$(q "DO \$\$ DECLARE i int; BEGIN FOR i IN 1..$N LOOP
  INSERT INTO bp(name,created,deleted,create_revision,prev_revision,lease,value)
  VALUES ('/k/'||i,1,0,i,0,0,$BIG); COMMIT; END LOOP; END \$\$;
" >/dev/null 2>&1; date +%s%N)
q "TRUNCATE bp" >/dev/null
S=$(date +%s%N)
q "DO \$\$ DECLARE i int; BEGIN FOR i IN 1..$N LOOP
  INSERT INTO bp(name,created,deleted,create_revision,prev_revision,lease,value)
  VALUES ('/k/'||i,1,0,i,0,0,$BIG); COMMIT; END LOOP; END \$\$;" >/dev/null
E=$(date +%s%N); TA=$(( (E-S)/1000000 ))
printf 'A  insert-per-transaction   %6d ms  %8.0f rows/s\n' "$TA" "$(echo "$N $TA" | awk '{print $1/($2/1000)}')"

# B — N inserts, one transaction
q "TRUNCATE bp" >/dev/null
S=$(date +%s%N)
q "DO \$\$ DECLARE i int; BEGIN FOR i IN 1..$N LOOP
  INSERT INTO bp(name,created,deleted,create_revision,prev_revision,lease,value)
  VALUES ('/k/'||i,1,0,i,0,0,$BIG); END LOOP; END \$\$;" >/dev/null
E=$(date +%s%N); TB=$(( (E-S)/1000000 ))
printf 'B  one transaction          %6d ms  %8.0f rows/s   %.1fx\n' "$TB" "$(echo "$N $TB" | awk '{print $1/($2/1000)}')" "$(echo "$TA $TB" | awk '{print $1/$2}')"

# C — single multi-row INSERT ... RETURNING (reachable from kine)
q "TRUNCATE bp" >/dev/null
S=$(date +%s%N)
q "INSERT INTO bp(name,created,deleted,create_revision,prev_revision,lease,value)
   SELECT '/k/'||g,1,0,g,0,0,$BIG FROM generate_series(1,$N) g RETURNING id;" >/dev/null
E=$(date +%s%N); TC=$(( (E-S)/1000000 ))
printf 'C  multi-row INSERT RETURNING %4d ms  %8.0f rows/s   %.1fx\n' "$TC" "$(echo "$N $TC" | awk '{print $1/($2/1000)}')" "$(echo "$TA $TC" | awk '{print $1/$2}')"

# D — COPY (cannot RETURNING; shown for comparison only)
q "TRUNCATE bp" >/dev/null
S=$(date +%s%N)
docker exec "$CT" bash -c "psql -U kine -d kine -p $PORT -qc \"COPY bp(name,created,deleted,create_revision,prev_revision,lease,value) FROM PROGRAM 'head -c 0 /dev/null' CSV\"" >/dev/null 2>&1 || true
q "INSERT INTO bp(name,created,deleted,create_revision,prev_revision,lease,value)
   SELECT '/k/'||g,1,0,g,0,0,$BIG FROM generate_series(1,$N) g;" >/dev/null
E=$(date +%s%N)
echo "D  COPY: not measurable as a kine path — COPY has no RETURNING, so it cannot"
echo "   hand each caller its revision. Excluded on semantics, not speed."
q "DROP TABLE IF EXISTS bp" >/dev/null
