#!/usr/bin/env bash
# Captures CPU profiles of every TradeWatch service while marketsim publishes
# at a fixed rate, and summarizes each with `go tool pprof -top`. `make
# profile` runs it in the toolbox container against the compose stack, with
# marketsim configured by deploy/compose/bench.yml.
#
# Usage: scripts/profile.sh <events per second> <seconds> <output directory>
set -euo pipefail

rate="$1"
seconds="$2"
out="$3"
mkdir -p "$out"

curl -fsS -X PUT -H 'Content-Type: application/json' \
	-d "{\"mode\":\"rate\",\"rate\":$rate}" http://marketsim:8090/api/v1/sim/pace >/dev/null
echo "marketsim is publishing $rate events/s; warming up for 20 s"
sleep 20

published() {
	curl -fsS http://marketsim:8090/metrics | awk '/^tradewatch_marketsim_events_published_total/ {s += $2} END {printf "%d", s}'
}

profile() { # name base-url
	curl -fsS "$2/debug/pprof/profile?seconds=$seconds" -o "$out/$1.pb.gz"
}

before=$(published)
pids=()
profile marketsim http://marketsim:8090 &
pids+=($!)
profile ingest http://ingest:8091 &
pids+=($!)
profile alertsink http://alertsink:8093 &
pids+=($!)
n=0
for ip in $(getent ahostsv4 detector | awk '{print $1}' | sort -u); do
	n=$((n + 1))
	profile "detector-$n" "http://$ip:8092" &
	pids+=($!)
done
for p in "${pids[@]}"; do
	wait "$p"
done
after=$(published)

{
	echo "CPU profiles of every service, $seconds s each, taken together"
	echo "target rate:     $rate events/s"
	echo "published:       $(((after - before) / seconds)) events/s during the profiles"
	echo "date:            $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "commit:          $(git -c safe.directory="$PWD" rev-parse --short HEAD 2>/dev/null || echo unknown)"
	echo "cpu:             $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ *//') ($(nproc) logical CPUs visible)"
	echo "detectors:       $n"
} >"$out/README.txt"
for f in "$out"/*.pb.gz; do
	go tool pprof -top -nodecount=40 "$f" >"${f%.pb.gz}.top.txt" 2>/dev/null
	go tool pprof -top -cum -nodecount=200 "$f" >"${f%.pb.gz}.cum.txt" 2>/dev/null
done
cat "$out/README.txt"
echo "profiles written to $out"
