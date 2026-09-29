#!/usr/bin/env bash
# Starts the demo backends from examples/lb.conf, then the load balancer.
#   scripts/demo.sh [build]
# Ctrl-C stops everything.
set -euo pipefail
BUILD="${1:-build}"
pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT

for i in 1 2 3; do
  "$BUILD/demo_backend" --port "900$i" --id "web$i" & pids+=($!)
done
for i in 1 2; do
  "$BUILD/demo_backend" --port "910$i" --id "echo$i" --mode tcp & pids+=($!)
done
sleep 0.5
echo "try: curl localhost:8080/echo   curl -H 'X-Canary: 1' localhost:8080/   curl localhost:9901/stats"
"$BUILD/hybridlb" --config examples/lb.conf
