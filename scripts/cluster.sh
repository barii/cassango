#!/usr/bin/env bash
# Run a local cassa cluster.
#
#   scripts/cluster.sh start [N]     start N nodes (default 3) on ports 7001..
#   scripts/cluster.sh stop          stop all nodes
#   scripts/cluster.sh kill <i>      hard-kill node i (1-based)
#   scripts/cluster.sh restart <i>   start node i again
#   scripts/cluster.sh status        show cluster view from node 1
#   scripts/cluster.sh clean         stop and delete data/ and logs/
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=target/release/cassa
BASE_PORT=7000
SEEDS="127.0.0.1:7001,127.0.0.1:7002"
RF=${RF:-3}

build() { cargo build --release --quiet; }

start_node() {
  local i=$1 port=$((BASE_PORT + $1))
  mkdir -p logs
  "$BIN" --node-id "node$i" --listen "127.0.0.1:$port" --seeds "$SEEDS" \
         --replication-factor "$RF" --data-dir "data/node$i" \
         --gossip-interval-ms 500 --failure-timeout-ms 3000 \
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
    for f in logs/node*.pid; do [[ -e $f ]] || continue; i=${f#logs/node}; stop_node "${i%.pid}"; done ;;
  kill)    stop_node "$2" KILL ;;
  restart) start_node "$2" ;;
  status)  curl -s "127.0.0.1:$((BASE_PORT + ${2:-1}))/cluster" | python3 -m json.tool ;;
  clean)   "$0" stop; rm -rf data logs ;;
  *) sed -n '2,10p' "$0"; exit 1 ;;
esac
