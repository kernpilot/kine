#!/usr/bin/env bash
# Tier 2, HA topology — TWO apiservers, each on its own kine, sharing one
# Postgres. Writes go to apiserver A; watches are held on apiserver B.
#
# WHY THIS EXISTS. E1's justification is a tier-1 measurement: with two kine
# processes on one database, a watcher on the instance that did not receive the
# write waits for that instance's 1 s fallback ticker. That was measured with a
# synthetic etcd client. Tier 2 has already shown once that a tier-1 result can
# be right about the mechanism and wrong about the magnitude — capping
# max-open looked sufficient there and left 6 % of writes failing here. So E1's
# 34x claim does not become a real-world number until a real apiserver, with
# its own watch cache in the path, reproduces it.
#
# This is the topology Kamaji and k0smotron deploy: replicated control planes
# over a shared datastore.
#
# Usage: ./run-apiserver-ha.sh <label> [duration] [writers] [watchers] [rate]
#   KINE_BIN  stock kine, or the e1-listen-notify build
set -euo pipefail

cd "$(dirname "$0")"
LABEL="${1:?usage: run-apiserver-ha.sh <label> [duration] [writers] [watchers] [rate]}"
DURATION="${2:-60s}"
WRITERS="${3:-4}"
WATCHERS="${4:-8}"
RATE="${5:-2}"

KINE_BIN="${KINE_BIN:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/kine-patched}"
PG_CONTAINER="${PG_CONTAINER:-kine-bench-pg}"
APISERVER_IMAGE="${APISERVER_IMAGE:-registry.k8s.io/kube-apiserver:v1.31.0}"
MAX_OPEN="${KINE_MAX_OPEN:-40}"
MAX_IDLE="${KINE_MAX_IDLE:-40}"
DSN="postgres://kine:kine@localhost:55432/kine?sslmode=disable"
CERTS="$PWD/apiserver-certs"
RESULTS="results"
mkdir -p "$RESULTS"

log() { printf '\033[36m»\033[0m %s\n' "$*"; }
die() { printf '\033[31m✗\033[0m %s\n' "$*" >&2; exit 1; }

[[ -x "$KINE_BIN" ]] || die "kine binary not found at $KINE_BIN"
[[ -x ./apiload/apiload ]] || die "apiload not built"
[[ -f "$CERTS/tokens.csv" ]] || die "missing $CERTS/tokens.csv"

cleanup() {
  docker rm -f t2ha-apiserver-a t2ha-apiserver-b >/dev/null 2>&1 || true
  [[ -n "${PID_A:-}" ]] && kill "$PID_A" 2>/dev/null || true
  [[ -n "${PID_B:-}" ]] && kill "$PID_B" 2>/dev/null || true
  return 0
}
trap cleanup EXIT
cleanup
docker exec "$PG_CONTAINER" psql -U kine -d kine -qc 'DROP TABLE IF EXISTS kine CASCADE' >/dev/null

start_kine() { # port suffix -> echoes pid
  "$KINE_BIN" --endpoint "$DSN" --listen-address "127.0.0.1:$1" --metrics-bind-address 0 \
    --datastore-max-open-connections "$MAX_OPEN" \
    --datastore-max-idle-connections "$MAX_IDLE" \
    >"$RESULTS/$LABEL-$2.kine.log" 2>&1 &
  echo $!
}

wait_port() { for _ in $(seq 1 80); do
    if (exec 3<>/dev/tcp/127.0.0.1/"$1") 2>/dev/null; then exec 3<&- 3>&-; return 0; fi
    sleep 0.25
  done; return 1; }

log "starting kine A (2379) and kine B (2380) on one database"
PID_A=$(start_kine 2379 a); wait_port 2379 || die "kine A never listened"
kill -0 "$PID_A" 2>/dev/null || die "kine A exited — see $RESULTS/$LABEL-a.kine.log"
PID_B=$(start_kine 2380 b); wait_port 2380 || die "kine B never listened"
kill -0 "$PID_B" 2>/dev/null || die "kine B exited — see $RESULTS/$LABEL-b.kine.log"

start_apiserver() { # name etcd-port secure-port
  docker run -d --name "$1" --network host -v "$CERTS":/certs "$APISERVER_IMAGE" \
    kube-apiserver --etcd-servers="http://127.0.0.1:$2" \
      --service-cluster-ip-range=10.0.0.0/24 --authorization-mode=AlwaysAllow \
      --token-auth-file=/certs/tokens.csv \
      --service-account-key-file=/certs/sa.pub --service-account-signing-key-file=/certs/sa.key \
      --service-account-issuer=https://kubernetes.default.svc \
      --cert-dir="/certs/$1" --secure-port="$3" >/dev/null
}

wait_healthz() { # port
  for _ in $(seq 1 120); do
    if [[ "$(curl -sk -H 'Authorization: Bearer benchtoken123' "https://127.0.0.1:$1/healthz" -m 3 2>/dev/null)" == "ok" ]]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

log "starting apiserver A (6443 -> kine A) and apiserver B (6444 -> kine B)"
start_apiserver t2ha-apiserver-a 2379 6443
wait_healthz 6443 || die "apiserver A never became healthy — docker logs t2ha-apiserver-a"
start_apiserver t2ha-apiserver-b 2380 6444
wait_healthz 6444 || die "apiserver B never became healthy — docker logs t2ha-apiserver-b"
log "both apiservers healthy"

# -latency-sample-every 1: this is a low-rate profile by design. Sampling
# 1-in-50 here yields a handful of points, and percentiles from a handful come
# out identical to each other and read as a clean result.
log "load: $DURATION, $WRITERS writers @ ${RATE}/s -> A(6443), $WATCHERS watchers -> B(6444)"
./apiload/apiload -server https://127.0.0.1:6443 -watch-server https://127.0.0.1:6444 \
  -duration "$DURATION" -writers "$WRITERS" -watchers "$WATCHERS" -objects 25 \
  -write-rate "$RATE" -latency-sample-every 1 \
  -label "$LABEL" -out "$RESULTS/$LABEL.json"

log "done -> $RESULTS/$LABEL.json"
