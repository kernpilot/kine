#!/usr/bin/env bash
# Replication overhead — the largest gap between this benchmark and production.
#
# WHY THIS MATTERS MOST. Every number in this suite was measured against a
# single standalone PostgreSQL with no replica. Production is CNPG with
# replicas and automatic failover. Streaming replication is not free, and
# SYNCHRONOUS replication puts a network round trip to the standby inside every
# commit — which is exactly the path kine's write throughput depends on. A
# density model built on standalone numbers could be wrong by a wide margin in
# the one direction that matters.
#
# THREE ARMS:
#   none  — standalone, the configuration every earlier run used
#   async — a streaming standby, replication off the commit path
#   sync  — synchronous_standby_names set, standby ack inside every commit
#
# The sync arm is the one worth knowing. It is also the one most likely to
# change the recommendation, because it converts a local disk flush into a
# network round trip per write.
#
# The replica is VERIFIED to be streaming before anything is measured. A
# standby that silently failed to connect would make the async and sync arms
# quietly measure the standalone case and report "replication is free".
set -uo pipefail

cd "$(dirname "$0")"
KINE_BIN="${KINE_BIN:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/kine-patched}"
# Parameterised so the same sweep can run against a primary whose WAL is on
# fast storage. Synchronous replication and a slow WAL are both commit-path
# costs, and such costs have SUBSTITUTED rather than added throughout this
# suite — a fast WAL made synchronous_commit=off worth +3.5% instead of +124%.
# If that pattern holds, a fast WAL should buy little once synchronous
# replication is on, and provisioning one for a replicated deployment would be
# wasted. That is a prediction; this measures it.
PRIMARY="${PG_CONTAINER:-kine-bench-pg}"
PRIMARY_PORT="${PRIMARY_PORT:-55432}"      # host-side port kine dials
PRIMARY_EXEC_PORT="${PRIMARY_EXEC_PORT:-5432}"  # port inside the container
LABEL_PREFIX="${LABEL_PREFIX:-repl}"
REPLICA="kine-bench-pg-replica"
DUR="${DUR:-60s}"; W="${W:-100}"; WATCH="${WATCH:-256}"; REPS="${REPS:-2}"
RESULTS="results"; LOG="$RESULTS/replication.log"; mkdir -p "$RESULTS"
export KINE_MAX_OPEN="${KINE_MAX_OPEN:-80}" KINE_MAX_IDLE="${KINE_MAX_IDLE:-80}"

say() { printf '%s | %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$LOG"; }
pexec() { docker exec "$PRIMARY" psql -U kine -d kine -p "$PRIMARY_EXEC_PORT" -tAqc "$1" 2>/dev/null; }

wait_pg() { for _ in $(seq 1 90); do
    docker exec "$1" pg_isready -U kine -d kine -p "${2:-5432}" >/dev/null 2>&1 && return 0; sleep 1; done; return 1; }
die_repl() { say "!! $*"; exit 1; }

run_arm() { # label reps
  for r in $(seq 1 "$2"); do
    say "  run $1-r${r}"
    env PG_CONTAINER="$PRIMARY" PG_PORT="$PRIMARY_PORT" PG_EXEC_PORT="$PRIMARY_EXEC_PORT" \
        KINE_BIN="$KINE_BIN" timeout 300 ./run.sh "$1-r${r}" "$DUR" "$W" "$WATCH" \
      >>"$LOG" 2>&1 || say "    !! $1-r${r} FAILED"
  done
}

teardown_replica() {
  docker rm -f "$REPLICA" >/dev/null 2>&1 || true
  docker volume rm kine-bench-replica-data >/dev/null 2>&1 || true
  pexec "ALTER SYSTEM RESET synchronous_standby_names" >/dev/null
  pexec "SELECT pg_reload_conf()" >/dev/null
}

say "=== replication sweep ==="

# ---- Arm 1: standalone -------------------------------------------------------
teardown_replica
say "--- arm: none (standalone) ---"
run_arm "${LABEL_PREFIX}-none" "$REPS"

# ---- Build a real streaming standby ------------------------------------------
say "preparing replication on the primary"
pexec "SELECT 1 FROM pg_roles WHERE rolname='repl'" | grep -q 1 \
  || pexec "CREATE ROLE repl WITH REPLICATION LOGIN PASSWORD 'repl'" >/dev/null

# ASK the server where its config lives; do not assume. PostgreSQL 18's data
# directory in this image is /var/lib/postgresql/18/docker, not the
# /var/lib/postgresql/data that earlier majors used. Appending to the assumed
# path failed silently inside `bash -c` (the directory does not exist), the
# replication rule was never added, and pg_basebackup died with "no pg_hba.conf
# entry for replication connection" one second in.
HBA=$(pexec "SHOW hba_file")
[[ -n "$HBA" ]] || die_repl "could not locate pg_hba.conf on the primary"
say "primary hba_file: $HBA"
docker exec "$PRIMARY" bash -c \
  "grep -q '^host replication repl' '$HBA' || echo 'host replication repl all scram-sha-256' >> '$HBA'"
# Verify the rule is really there before relying on it.
docker exec "$PRIMARY" grep -q '^host replication repl' "$HBA" \
  || die_repl "failed to add the replication rule to $HBA"
pexec "SELECT pg_reload_conf()" >/dev/null
say "replication rule present and config reloaded"

say "taking a base backup into the replica volume"
docker volume create kine-bench-replica-data >/dev/null
docker run --rm --network host -e PGPASSWORD=repl \
  -v kine-bench-replica-data:/var/lib/postgresql/data \
  postgres:18.3 bash -c \
  "rm -rf /var/lib/postgresql/data/* && \
   pg_basebackup -h 127.0.0.1 -p '"$PRIMARY_PORT"' -U repl -D /var/lib/postgresql/data -Fp -Xs -R -c fast && \
   chown -R postgres:postgres /var/lib/postgresql/data && \
   chmod 0700 /var/lib/postgresql/data" >>"$LOG" 2>&1 \
  || { say "!! base backup failed — see $LOG"; exit 1; }

say "starting the replica on 55433"
docker run -d --name "$REPLICA" --network host \
  -v kine-bench-replica-data:/var/lib/postgresql/data \
  -e PGDATA=/var/lib/postgresql/data \
  postgres:18.3 -c port=55433 >/dev/null
wait_pg "$REPLICA" 55433 || die_repl "replica never became ready"

# VERIFY it is actually streaming. Without this the next two arms could silently
# measure the standalone case again.
for _ in $(seq 1 30); do
  STATE=$(pexec "SELECT state FROM pg_stat_replication WHERE usename='repl' LIMIT 1")
  [[ "$STATE" == "streaming" ]] && break
  sleep 2
done
[[ "$STATE" == "streaming" ]] || { say "!! replica is not streaming (state='$STATE') — aborting"; exit 1; }
say "replica confirmed streaming"

# ---- Arm 2: asynchronous replication ----------------------------------------
say "--- arm: async replication ---"
pexec "SHOW synchronous_standby_names" > "$RESULTS/${LABEL_PREFIX}-async.replconf.txt"
run_arm "${LABEL_PREFIX}-async" "$REPS"

# ---- Arm 3: synchronous replication -----------------------------------------
say "--- arm: SYNCHRONOUS replication ---"
pexec "ALTER SYSTEM SET synchronous_standby_names = '*'" >/dev/null
pexec "SELECT pg_reload_conf()" >/dev/null
for _ in $(seq 1 30); do
  SYNCSTATE=$(pexec "SELECT sync_state FROM pg_stat_replication WHERE usename='repl' LIMIT 1")
  [[ "$SYNCSTATE" == "sync" ]] && break
  sleep 2
done
say "sync_state = ${SYNCSTATE:-unknown}"
printf 'sync_state=%s\n' "${SYNCSTATE:-unknown}" > "$RESULTS/${LABEL_PREFIX}-sync.replconf.txt"
[[ "$SYNCSTATE" == "sync" ]] || say "!! standby did not reach sync — the arm below is NOT synchronous"
run_arm "${LABEL_PREFIX}-sync" "$REPS"

say "tearing the replica down and restoring the primary"
teardown_replica
say "=== replication sweep complete ==="
