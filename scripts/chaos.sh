#!/usr/bin/env bash
# Injects faults into the running compose stack while one recorded marketsim
# session streams through it, then checks that the pipeline lost and
# duplicated no alert. `make chaos` runs it after `make up`.
#
# Faults, mid-stream: SIGKILL of a detector, re-published copies of recent
# raw, market_events and alert records, a graceful restart and a SIGKILL of
# ingest and the alert sink, a SIGKILL of both detectors at once, and
# rescaling the detectors up and back down. The fault log and the verdict
# go to docs/results/chaos (CHAOS_OUT).
set -euo pipefail

out="${CHAOS_OUT:-docs/results/chaos}"
work="tmp/chaos"
mkdir -p "$work"
faults="$work/faults.txt"
: >"$faults"
run="$(date -u +%Y%m%dT%H%M%SZ)"
session="chaos-$run-001"
compose=(docker compose)
toolbox=(docker compose run --rm --no-deps -v tradewatch_recordings:/recordings:ro toolbox)

fault() {
	echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*" | tee -a "$faults"
}

restore() {
	echo "restoring marketsim and two detectors"
	"${compose[@]}" up -d --no-deps --scale detector=2 --wait detector >/dev/null 2>&1 || true
	"${compose[@]}" up -d --no-deps --force-recreate --wait marketsim >/dev/null 2>&1 || true
}
trap restore EXIT

echo "building the chaos tool"
"${toolbox[@]}" go build -o "$work/chaos" ./cmd/chaos

echo "starting session $session"
TW_CHAOS_RUN="$run" "${compose[@]}" -f docker-compose.yml -f deploy/compose/chaos.yml \
	up -d --no-deps --force-recreate --wait marketsim
sleep 30

fault "SIGKILL detector-1 while events are streaming"
docker kill -s KILL tradewatch-detector-1 >/dev/null
sleep 20
fault "detector-1 started again"
docker start tradewatch-detector-1 >/dev/null
sleep 20

summary="$("${toolbox[@]}" "$work/chaos" duplicate -session "$session" -tail 5000)"
fault "replayed duplicates: $summary"
sleep 10

fault "graceful restart of ingest and the alert sink"
"${compose[@]}" restart ingest alertsink >/dev/null
sleep 20
fault "SIGKILL ingest and the alert sink"
docker kill -s KILL tradewatch-ingest-1 tradewatch-alertsink-1 >/dev/null
sleep 10
fault "ingest and the alert sink started again"
docker start tradewatch-ingest-1 tradewatch-alertsink-1 >/dev/null
sleep 20

fault "SIGKILL both detectors at once"
docker kill -s KILL tradewatch-detector-1 tradewatch-detector-2 >/dev/null
sleep 10
fault "both detectors started again"
docker start tradewatch-detector-1 tradewatch-detector-2 >/dev/null
sleep 20

fault "detectors scaled from 2 to 3: partitions rebalance"
"${compose[@]}" up -d --no-deps --no-recreate --scale detector=3 detector >/dev/null
sleep 20
fault "detectors scaled back to 2: the third leaves gracefully"
"${compose[@]}" up -d --no-deps --no-recreate --scale detector=2 detector >/dev/null

echo "waiting for session $session to finish"
docker wait tradewatch-marketsim-1 >/dev/null
echo "session finished; verifying"
"${toolbox[@]}" "$work/chaos" verify -session "$session" -recording "/recordings/$session.twrec.gz" \
	-faults "$faults" -out "$out" -commit "$(git -c safe.directory="$PWD" rev-parse --short HEAD 2>/dev/null || true)"
