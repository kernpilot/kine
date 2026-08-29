#!/usr/bin/env bash
# Tier 2 — a REAL kube-apiserver backed by kine, driven through the Kubernetes
# REST API.
#
# Tier 1 measures kine through a synthetic etcd client. That is fast to iterate
# on and it is where every optimization in this suite was found, but it can be
# wrong about the workload in ways internal consistency never reveals — the
# largest defect in this whole suite was a synthetic writer using a write shape
# the apiserver never emits. Tier 2 removes that class of doubt: the apiserver
# generates the etcd transactions itself.
#
# Usage: ./run-apiserver.sh <label> [duration] [writers] [watchers] [objects]
#   KINE_BIN       which kine to run
#   KINE_MAX_OPEN  connection-pool cap; UNSET means kine's default of unlimited
set -euo pipefail

cd "$(dirname "$0")"
LABEL="${1:?usage: run-apiserver.sh <label> [duration] [writers] [watchers] [objects]}"
DURATION="${2:-60s}"
WRITERS="${3:-16}"
WATCHERS="${4:-8}"
OBJECTS="${5:-25}"

KINE_BIN="${KINE_BIN:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/kine-patched}"
PG_CONTAINER="${PG_CONTAINER:-kine-bench-pg}"
APISERVER_IMAGE="${APISERVER_IMAGE:-registry.k8s.io/kube-apiserver:v1.31.0}"
DSN="postgres://kine:kine@localhost:55432/kine?sslmode=disable"
CERTS="$PWD/apiserver-certs"
RESULTS="results"
mkdir -p "$RESULTS"

log() { printf '\033[36m»\033[0m %s\n' "$*"; }
die() { printf '\033[31m✗\033[0m %s\n' "$*" >&2; exit 1; }

[[ -x "$KINE_BIN" ]] || die "kine binary not found at $KINE_BIN"
[[ -x ./apiload/apiload ]] || die "apiload not built"
[[ -f "$CERTS/tokens.csv" ]] || die "missing $CERTS/tokens.csv"
docker exec "$PG_CONTAINER" pg_isready -U kine -d kine >/dev/null 2>&1 \
  || die "postgres container '$PG_CONTAINER' is not ready"

cleanup() {
  docker rm -f t2-apiserver >/dev/null 2>&1 || true
  [[ -n "${KINE_PID:-}" ]] && kill "$KINE_PID" 2>/dev/null || true
}
trap cleanup EXIT

cleanup
docker exec "$PG_CONTAINER" psql -U kine -d kine -qc 'DROP TABLE IF EXISTS kine CASCADE' >/dev/null

# Deliberately quoted so an unset KINE_MAX_OPEN leaves kine on its DEFAULT of
# unlimited connections. That default is the finding of E0 and this run must be
# able to reproduce it, not silently paper over it.
log "starting kine (pool cap: ${KINE_MAX_OPEN:-DEFAULT/unlimited})"
"$KINE_BIN" --endpoint "$DSN" --listen-address 127.0.0.1:2379 --metrics-bind-address 0 \
  ${KINE_MAX_OPEN:+--datastore-max-open-connections "$KINE_MAX_OPEN"} \
  ${KINE_MAX_IDLE:+--datastore-max-idle-connections "$KINE_MAX_IDLE"} \
  >"$RESULTS/$LABEL.kine.log" 2>&1 &
KINE_PID=$!
# A port check alone is not proof OUR kine is up: a kine left over from an
# earlier run still holds 2379, so the new one fails to bind, exits, and the
# check passes against the corpse of the old one. Verify the process we started
# is alive as well as the port being open.
for _ in $(seq 1 60); do
  if ! kill -0 "$KINE_PID" 2>/dev/null; then
    die "kine exited on startup (port 2379 already in use?) — see $RESULTS/$LABEL.kine.log"
  fi
  if (exec 3<>/dev/tcp/127.0.0.1/2379) 2>/dev/null; then
    exec 3<&- 3>&-
    break
  fi
  sleep 0.25
done
kill -0 "$KINE_PID" 2>/dev/null || die "kine is not running — see $RESULTS/$LABEL.kine.log"

log "starting kube-apiserver"
docker run -d --name t2-apiserver --network host -v "$CERTS":/certs "$APISERVER_IMAGE" \
  kube-apiserver --etcd-servers=http://127.0.0.1:2379 \
    --service-cluster-ip-range=10.0.0.0/24 --authorization-mode=AlwaysAllow \
    --token-auth-file=/certs/tokens.csv \
    --service-account-key-file=/certs/sa.pub --service-account-signing-key-file=/certs/sa.key \
    --service-account-issuer=https://kubernetes.default.svc \
    --cert-dir=/certs --secure-port=6443 >/dev/null

# healthz is the real readiness signal. A container that is "Up" proves nothing:
# the apiserver binds late and a kine it cannot reach leaves it running and
# useless, which would show up as a load generator that hangs rather than fails.
# The trailing `sleep 1` is load-bearing under `set -e`: a bare
# `[[ ... ]] && break` as the last statement in the body returns non-zero on
# every non-matching iteration, which exits the script silently before the
# check below can report anything useful.
for _ in $(seq 1 90); do
  if [[ "$(curl -sk -H 'Authorization: Bearer benchtoken123' https://127.0.0.1:6443/healthz -m 3 2>/dev/null)" == "ok" ]]; then
    break
  fi
  sleep 1
done
[[ "$(curl -sk -H 'Authorization: Bearer benchtoken123' https://127.0.0.1:6443/healthz -m 5 2>/dev/null)" == "ok" ]] \
  || die "apiserver never became healthy — docker logs t2-apiserver"
log "apiserver healthy"

log "load: $DURATION, $WRITERS writers, $WATCHERS watchers, $OBJECTS objects/writer"
./apiload/apiload -duration "$DURATION" -writers "$WRITERS" -watchers "$WATCHERS" \
  -objects "$OBJECTS" -label "$LABEL" -out "$RESULTS/$LABEL.json"

docker exec "$PG_CONTAINER" psql -U kine -d kine -tAc \
  "select pg_size_pretty(pg_table_size('kine'))||' table, '||pg_size_pretty(pg_indexes_size('kine'))||' indexes'" \
  > "$RESULTS/$LABEL.sizes.txt"
log "sizes: $(cat "$RESULTS/$LABEL.sizes.txt")"
log "done -> $RESULTS/$LABEL.json"
