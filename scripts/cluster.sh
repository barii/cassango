#!/usr/bin/env bash
# Run a local cassango cluster.
#
#   scripts/cluster.sh start [N]     start N nodes (default 3) on ports 7001..
#   scripts/cluster.sh stop          stop all nodes
#   scripts/cluster.sh kill <i>      hard-kill node i (1-based)
#   scripts/cluster.sh restart <i>   start node i again
#   scripts/cluster.sh status        show cluster view from node 1
#   scripts/cluster.sh repair        run anti-entropy repair on every running node
#   scripts/cluster.sh join <i>      start a NEW node i that bootstraps into the cluster
#   scripts/cluster.sh decommission <i>   stream node i's data away, then stop it
#   scripts/cluster.sh removenode <id>    remove a dead node (run via node 1)
#   scripts/cluster.sh cleanup       drop data nodes no longer own (after joins)
#   scripts/cluster.sh clean         stop and delete data/ and logs/
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=target/release/cassango
BASE_PORT=7000
SEEDS="127.0.0.1:7001,127.0.0.1:7002"
RF=${RF:-3}
MEMTABLE_MB=${MEMTABLE_MB:-16}
RING_DELAY_MS=${RING_DELAY_MS:-1500}

build() { cargo build --release --quiet; }

start_node() {
  local i=$1 port=$((BASE_PORT + $1))
  mkdir -p logs
  "$BIN" --node-id "node$i" --listen "127.0.0.1:$port" --seeds "$SEEDS" \
         --replication-factor "$RF" --data-dir "data/node$i" --memtable-mb "$MEMTABLE_MB" \
         --gossip-interval-ms 500 --failure-timeout-ms 3000 --ring-delay-ms "$RING_DELAY_MS" ${EXTRA_ARGS:-} \
         >>"logs/node$i.log" 2>&1 &
  echo $! >"logs/node$i.pid"
  echo "started node$i on 127.0.0.1:$port (pid $!)"
}

stop_node() {
  local i=$1 sig=${2:-INT}
  if [[ -f logs/node$i.pid ]]; then
    kill "-$sig" "$(cat logs/node$i.pid)" 2>/dev/null || true
    rm -f "logs/node$i.pid"
    echo "stopped node$i ($sig)"
  fi
}

case "${1:-}" in
  start)
    build
    for i in $(seq 1 "${2:-3}"); do start_node "$i"; done ;;
  stop)
    pids=()
    for f in logs/node*.pid; do [[ -e $f ]] || continue; pids+=("$(cat "$f")"); i=${f#logs/node}; stop_node "${i%.pid}"; done
    # wait for graceful shutdown (memtable flush) to finish
    for p in ${pids[@]+"${pids[@]}"}; do while kill -0 "$p" 2>/dev/null; do sleep 0.1; done; done ;;
  kill)    stop_node "$2" KILL ;;
  restart|join) start_node "$2" ;;
  decommission)
    curl -s -XPOST "127.0.0.1:$((BASE_PORT + $2))/admin/decommission" | python3 -m json.tool
    stop_node "$2" ;;
  removenode)
    curl -s -XPOST "127.0.0.1:$((BASE_PORT + 1))/admin/removenode/$2" | python3 -m json.tool ;;
  cleanup)
    for f in logs/node*.pid; do
      [[ -e $f ]] || continue; i=${f#logs/node}; i=${i%.pid}
      echo "node$i: $(curl -s -XPOST "127.0.0.1:$((BASE_PORT + i))/admin/cleanup" | python3 -c 'import json,sys; d=json.load(sys.stdin); print("dropped", d.get("dropped_entries", d))')"
    done ;;
  repair)
    for f in logs/node*.pid; do
      [[ -e $f ]] || continue; i=${f#logs/node}; i=${i%.pid}
      echo "node$i: $(curl -s -XPOST "127.0.0.1:$((BASE_PORT + i))/admin/repair" | python3 -c 'import json,sys; d=json.load(sys.stdin); print("ranges={ranges} in_sync={ranges_in_sync} repaired={ranges_repaired} skipped={ranges_skipped} streamed={total} {duration_ms}ms errors={errors}".format(total=sum(d["streamed"].values()), **d))')"
    done ;;
  status)  curl -s "127.0.0.1:$((BASE_PORT + ${2:-1}))/cluster" | python3 -m json.tool ;;
  clean)   "$0" stop; rm -rf data logs ;;
  *) sed -n '2,15p' "$0"; exit 1 ;;
esac
