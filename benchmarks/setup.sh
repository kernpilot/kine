#!/usr/bin/env bash
# One-time setup: the PostgreSQL backends the suite measures against.
#
# THREE BACKENDS, because storage turned out to dominate every kine-level knob
# by more than an order of magnitude. Comparing a patch measured on one against
# a baseline measured on another produces confident nonsense, so each result
# records which backend produced it.
#
#   pg      real disk       the realistic path; the only place durability
#                           settings (synchronous_commit, WAL, replication)
#                           mean anything
#   mem     tmpfs           for experiments about kine's LOGIC (index shape,
#                           poll batching, watcher fan-out, payload size),
#                           where disk is a confounder rather than the subject
#   walmem  split           table data on disk, pg_wal on tmpfs — isolates
#                           commit fsync from table I/O
set -uo pipefail
PG_IMAGE="${PG_IMAGE:-postgres:18.3}"   # match your production major version
say() { printf '» %s\n' "$*"; }

start() { # name port extra-args...
  docker rm -f "$1" >/dev/null 2>&1 || true
  docker run -d --name "$1" --network host \
    -e POSTGRES_USER=kine -e POSTGRES_PASSWORD=kine -e POSTGRES_DB=kine \
    "${@:3}" "$PG_IMAGE" -c port="$2" >/dev/null
  for _ in $(seq 1 90); do
    docker exec "$1" pg_isready -U kine -d kine -p "$2" >/dev/null 2>&1 && { say "$1 ready on $2"; return 0; }
    sleep 1
  done
  say "!! $1 did not become ready"; return 1
}

say "kine-bench-pg      (disk)   :55432"
docker rm -f kine-bench-pg >/dev/null 2>&1 || true
docker run -d --name kine-bench-pg -p 55432:5432 \
  -e POSTGRES_USER=kine -e POSTGRES_PASSWORD=kine -e POSTGRES_DB=kine "$PG_IMAGE" >/dev/null
for _ in $(seq 1 90); do docker exec kine-bench-pg pg_isready -U kine -d kine >/dev/null 2>&1 && break; sleep 1; done

say "kine-bench-mem     (tmpfs)  :55442"
start kine-bench-mem 55442 --tmpfs /var/lib/postgresql:rw,size=32g -e PGDATA=/var/lib/postgresql/data

say "kine-bench-walmem  (split)  :55452"
docker volume create kine-walmem-data >/dev/null
start kine-bench-walmem 55452 -v kine-walmem-data:/var/lib/postgresql \
  --tmpfs /pgwal:rw,size=8g,mode=0700 \
  -e PGDATA=/var/lib/postgresql/data -e POSTGRES_INITDB_WALDIR=/pgwal

say "done. build the binaries with ./build.sh, then ./verify-patches.sh"
