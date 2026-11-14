#!/usr/bin/env bash
# End-to-end test against a fresh local 4-node cluster (RF=3).
set -euo pipefail
cd "$(dirname "$0")/.."

C=scripts/cluster.sh
export MEMTABLE_MB=1   # small memtables so the test exercises SSTable flushes
N1=127.0.0.1:7001; N2=127.0.0.1:7002; N3=127.0.0.1:7003; N4=127.0.0.1:7004
pass=0
ok()   { pass=$((pass+1)); echo "  ✓ $*"; }
fail() { echo "  ✗ $*"; $C stop >/dev/null; exit 1; }
j()    { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }

trap '$C stop >/dev/null 2>&1 || true' EXIT
$C clean >/dev/null 2>&1 || true
$C start 4
echo "waiting for gossip to converge..."
for _ in $(seq 1 40); do
  up=$(curl -s $N4/cluster 2>/dev/null | j "sum(m['status']=='UP' for m in d['members'])" 2>/dev/null || echo 0)
  [[ $up == 4 ]] && break; sleep 0.25
done
[[ $up == 4 ]] && ok "all 4 nodes see each other via gossip" || fail "gossip did not converge ($up/4)"

echo "documents"
id=$(curl -s -XPOST $N1/collections/users/docs -d '{"name":"ann","age":31,"tags":["admin"],"addr":{"city":"Leeds"}}' -H 'content-type: application/json' | j "d['_id']")
[[ -n $id ]] && ok "insert via node1 -> _id=$id"
name=$(curl -s $N3/collections/users/docs/$id | j "d['name']")
[[ $name == ann ]] && ok "read via node3 (any node can coordinate)" || fail "read: $name"
for p in bob:25:London cat:42:Leeds dan:19:York; do
  IFS=: read n a city <<<"$p"
  curl -s -XPUT $N2/collections/users/docs/$n -H 'content-type: application/json' \
       -d "{\"name\":\"$n\",\"age\":$a,\"addr\":{\"city\":\"$city\"}}" >/dev/null
done
r=$(curl -s -XPOST $N4/collections/users/find -H 'content-type: application/json' \
     -d '{"filter":{"addr.city":"Leeds","age":{"$gte":30}},"sort":{"age":-1}}' | j "[x['name'] for x in d['docs']]")
[[ $r == "['cat', 'ann']" ]] && ok "find with nested filter + sort: $r" || fail "find: $r"
r=$(curl -s -XPOST $N1/collections/users/find -H 'content-type: application/json' \
     -d '{"filter":{"$or":[{"age":{"$lt":20}},{"tags":"admin"}]},"limit":10}' | j "sorted(x['name'] for x in d['docs'])")
[[ $r == "['ann', 'dan']" ]] && ok "find with \$or / array match: $r" || fail "find \$or: $r"
r=$(curl -s -XPATCH $N2/collections/users/docs/bob -H 'content-type: application/json' \
     -d '{"$inc":{"age":1},"$set":{"addr.zip":"E1"},"$push":{"tags":"new"}}' | j "(d['age'], d['addr']['zip'], d['tags'])")
[[ $r == "(26, 'E1', ['new'])" ]] && ok "patch with \$inc/\$set/\$push: $r" || fail "patch: $r"
code=$(curl -s -o /dev/null -w '%{http_code}' -XPOST $N1/collections/users/find -d '{"filter":{"a":{"$bad":1}}}' -H 'content-type: application/json')
[[ $code == 400 ]] && ok "invalid operator rejected (400)" || fail "expected 400, got $code"

echo "replication & failure handling"
reps=$(curl -s $N1/cluster/owners/users/dan | j "' '.join(r['node_id'] for r in d['replicas'])")
echo "    replicas of users/dan: $reps"
victim=${reps%% *}; vi=${victim#node}
other=$(for i in 1 2 3 4; do [[ " $reps " == *" node$i "* ]] || echo $i; done | head -1)
coord=127.0.0.1:700$other
$C kill $vi >/dev/null
echo "    killed $victim (primary replica); waiting for failure detection..."
for _ in $(seq 1 40); do
  st=$(curl -s $coord/cluster | j "[m['status'] for m in d['members'] if m['node_id']=='$victim'][0]")
  [[ $st == DOWN ]] && break; sleep 0.25
done
[[ $st == DOWN ]] && ok "gossip marked $victim DOWN" || fail "$victim not detected down"
r=$(curl -s -XPATCH "$coord/collections/users/docs/dan?consistency=quorum" -H 'content-type: application/json' -d '{"$set":{"age":20}}' | j "d['age']")
[[ $r == 20 ]] && ok "QUORUM write succeeds with 1 of 3 replicas down" || fail "quorum write: $r"
code=$(curl -s -o /dev/null -w '%{http_code}' "$coord/collections/users/docs/dan?consistency=all")
[[ $code == 503 ]] && ok "ALL read fails fast with 503 Unavailable" || fail "expected 503, got $code"
hints=$(curl -s $coord/cluster | j "d['pending_hints'].get('$victim',0)")
[[ $hints -ge 1 ]] && ok "hint stored for $victim ($hints pending)" || fail "no hint stored"

$C restart $vi >/dev/null
echo "    restarted $victim; waiting for hinted handoff..."
for _ in $(seq 1 60); do
  hints=$(curl -s $coord/cluster | j "d['pending_hints'].get('$victim',0)")
  [[ $hints == 0 ]] && break; sleep 0.25
done
[[ $hints == 0 ]] && ok "hints replayed to $victim" || fail "hints still pending: $hints"
r=$(curl -s "127.0.0.1:700$vi/collections/users/docs/dan?consistency=one" >/dev/null; \
    curl -s -XPOST 127.0.0.1:700$vi/internal/get -H 'content-type: application/json' -d '{"c":"users","k":"dan"}' | j "d['v']['age']")
[[ $r == 20 ]] && ok "$victim has the write it missed (age=$r) and survived restart" || fail "victim has age=$r"

echo "deletes"
code=$(curl -s -o /dev/null -w '%{http_code}' -XDELETE $N1/collections/users/docs/bob)
[[ $code == 204 ]] || fail "delete returned $code"
code=$(curl -s -o /dev/null -w '%{http_code}' $N4/collections/users/docs/bob)
[[ $code == 404 ]] && ok "deleted document is gone (tombstone)" || fail "expected 404, got $code"
r=$(curl -s -XPOST $N2/collections/users/find -d '{}' -H 'content-type: application/json' | j "d['matched']")
[[ $r == 3 ]] && ok "find sees 3 remaining docs" || fail "find count $r"

echo "on-disk storage"
start=$(date +%s.%N)
python3 - <<'PY'
import json, http.client, concurrent.futures as cf
def load(worker):
    conn = http.client.HTTPConnection("127.0.0.1", 7001 + worker % 4)
    for i in range(worker, 5000, 8):
        body = json.dumps({"n": i, "bucket": i % 10, "blob": "x" * 800})
        conn.request("PUT", f"/collections/bulk/docs/d{i:05}?consistency=quorum", body, {"content-type": "application/json"})
        r = conn.getresponse(); r.read()
        assert r.status == 200, r.status
with cf.ThreadPoolExecutor(8) as ex: list(ex.map(load, range(8)))
PY
secs=$(python3 -c "import time; print(round($(date +%s.%N) - $start, 1))")
ok "bulk-loaded 5000 x ~1KB docs at QUORUM in ${secs}s"
for _ in $(seq 1 40); do   # flushes happen in the background
  ssts=$( (ls data/node*/sst-*.db 2>/dev/null || true) | wc -l | tr -d ' ')
  [[ $ssts -ge 4 ]] && break; sleep 0.25
done
[[ $ssts -ge 4 ]] && ok "memtables flushed to $ssts SSTables across the cluster" || fail "expected SSTables on disk, found $ssts"
q='{"filter":{"bucket":3,"n":{"$gte":4000}},"sort":{"n":-1},"limit":3}'
r=$(curl -s -XPOST $N3/collections/bulk/find -H 'content-type: application/json' -d "$q" | j "(d['matched'], [x['n'] for x in d['docs']])")
[[ $r == "(100, [4993, 4983, 4973])" ]] && ok "find over SSTables + memtables: $r" || fail "find: $r"

$C stop >/dev/null
[[ $(ls data/node*/commitlog-*.log | xargs cat | wc -c | tr -d ' ') == 0 ]] && ok "graceful stop flushed memtables (commit logs empty)" || fail "commit logs not empty after shutdown"
$C start 4 >/dev/null
for _ in $(seq 1 40); do
  up=$(curl -s $N1/cluster 2>/dev/null | j "sum(m['status']=='UP' for m in d['members'])" 2>/dev/null || echo 0)
  [[ $up == 4 ]] && break; sleep 0.25
done
r=$(curl -s -XPOST $N2/collections/bulk/find -H 'content-type: application/json' -d '{"filter":{},"limit":0}' | j "d['matched']")
[[ $r == 5000 ]] && ok "full cluster restart: all 5000 docs read back from disk" || fail "after restart matched=$r"
r=$(curl -s $N4/collections/users/docs/dan | j "d['age']")
[[ $r == 20 ]] && ok "earlier documents intact (dan.age=$r)" || fail "dan: $r"

for i in 1 2 3 4; do curl -s -XPOST "127.0.0.1:700$i/admin/compact?major=true" >/dev/null; done
per_node=$(for i in 1 2 3 4; do ls data/node$i/sst-*.db | wc -l | tr -d ' '; done | sort -u | tr '\n' ' ')
[[ $per_node == "1 " ]] && ok "major compaction: exactly 1 SSTable per node" || fail "sstables per node after major compaction: $per_node"
r=$(curl -s -XPOST $N1/collections/bulk/find -H 'content-type: application/json' -d '{"filter":{"bucket":7}}' | j "d['matched']")
[[ $r == 500 ]] && ok "data intact after compaction (bucket 7: $r docs)" || fail "after compaction: $r"

echo
echo "all $pass checks passed"
