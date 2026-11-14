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
converged() {  # every node sees all 4 nodes UP and NORMAL
  for n in $N1 $N2 $N3 $N4; do
    up=$(curl -s $n/cluster 2>/dev/null | j "sum(m['status']=='UP' and m['state']=='NORMAL' for m in d['members'])" 2>/dev/null || echo 0)
    [[ $up == 4 ]] || return 1
  done
}
for _ in $(seq 1 60); do converged && break; sleep 0.25; done
converged && ok "all 4 nodes see each other via gossip" || fail "gossip did not converge ($up/4)"

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
for _ in $(seq 1 60); do converged && break; sleep 0.25; done
r=$(curl -s -XPOST $N2/collections/bulk/find -H 'content-type: application/json' -d '{"filter":{},"limit":0}' | j "d['matched']")
[[ $r == 5000 ]] && ok "full cluster restart: all 5000 docs read back from disk" || fail "after restart matched=$r"
r=$(curl -s $N4/collections/users/docs/dan | j "d['age']")
[[ $r == 20 ]] && ok "earlier documents intact (dan.age=$r)" || fail "dan: $r"

for i in 1 2 3 4; do curl -s -XPOST "127.0.0.1:700$i/admin/compact?major=true" >/dev/null; done
per_node=$(for i in 1 2 3 4; do ls data/node$i/sst-*.db | wc -l | tr -d ' '; done | sort -u | tr '\n' ' ')
[[ $per_node == "1 " ]] && ok "major compaction: exactly 1 SSTable per node" || fail "sstables per node after major compaction: $per_node"
r=$(curl -s -XPOST $N1/collections/bulk/find -H 'content-type: application/json' -d '{"filter":{"bucket":7}}' | j "d['matched']")
[[ $r == 500 ]] && ok "data intact after compaction (bucket 7: $r docs)" || fail "after compaction: $r"

echo "anti-entropy repair"
replicas_of() { curl -s $N1/cluster/owners/$1/$2 | j "' '.join(r['node_id'][4:] for r in d['replicas'])"; }
state_on() {  # live | tomb | none
  curl -s -XPOST 127.0.0.1:700$1/internal/get -H 'content-type: application/json' -d "{\"c\":\"$2\",\"k\":\"$3\"}" \
    | j "'none' if d is None else ('tomb' if d['v'] is None else 'live')"
}
states() { for i in $(replicas_of $1 $2); do state_on $i $1 $2; done | sort | uniq -c | awk '{printf "%s:%s ", $2, $1}'; }
now() { python3 -c "import time; print(int(time.time()*1e6))"; }

# (a) Writes that reached only one replica, as if their hints were lost.
# (never inject onto node3: it's about to lose its disk, which would erase the evidence)
r1=$(replicas_of users eve | tr ' ' '\n' | grep -v '^3$' | head -1)
curl -s -XPOST 127.0.0.1:700$r1/internal/apply -H 'content-type: application/json' \
     -d "[{\"c\":\"users\",\"k\":\"eve\",\"r\":{\"ts\":$(now),\"v\":{\"_id\":\"eve\",\"name\":\"eve\"}}}]" >/dev/null
r2=$(replicas_of users $id | tr ' ' '\n' | grep -v '^3$' | tail -1)
curl -s -XPOST 127.0.0.1:700$r2/internal/apply -H 'content-type: application/json' \
     -d "[{\"c\":\"users\",\"k\":\"$id\",\"r\":{\"ts\":$(now),\"v\":null}}]" >/dev/null
s1=$(states users eve); s2=$(states users $id)
[[ $s1 == "live:1 none:2 " && $s2 == "live:2 tomb:1 " ]] && ok "injected divergence: eve [$s1] ann [$s2]" || fail "setup: eve [$s1] ann [$s2]"

# (b) A node loses its disk.
before=$(curl -s $N3/collections | j "d['local_document_counts'].get('bulk',0)")
# --no-bootstrap: rejoin in place with an empty disk (otherwise it would re-stream on join)
$C kill 3 >/dev/null; rm -rf data/node3; EXTRA_ARGS=--no-bootstrap $C restart 3 >/dev/null
for _ in $(seq 1 60); do
  up=$(curl -s $N3/cluster 2>/dev/null | j "sum(m['status']=='UP' for m in d['members'])" 2>/dev/null || echo 0)
  seen=$(curl -s $N1/cluster | j "[m['status'] for m in d['members'] if m['node_id']=='node3'][0]")
  [[ $up == 4 && $seen == UP ]] && break; sleep 0.25
done
after=$(curl -s $N3/collections | j "d['local_document_counts'].get('bulk',0)")
[[ $after == 0 && $before -gt 0 ]] && ok "node3 lost its disk: $before bulk docs -> $after" || fail "wipe: $before -> $after"

$C repair | sed 's/^/    /'
fixed=$(curl -s $N3/collections | j "d['local_document_counts'].get('bulk',0)")
[[ $fixed == $before ]] && ok "repair rebuilt node3 from its peers ($fixed/$before docs)" || fail "node3 has $fixed/$before after repair"
s1=$(states users eve); s2=$(states users $id)
[[ $s1 == "live:3 " ]] && ok "lost write propagated to all replicas [$s1]" || fail "eve after repair: [$s1]"
[[ $s2 == "tomb:3 " ]] && ok "lost delete propagated to all replicas [$s2]" || fail "ann after repair: [$s2]"
streamed=$(for i in 1 2 3 4; do curl -s -XPOST 127.0.0.1:700$i/admin/repair | j "sum(d['streamed'].values())"; done | paste -sd+ - | bc)
[[ $streamed == 0 ]] && ok "second repair: every range in sync, nothing streamed" || fail "second repair streamed $streamed"
code=$(curl -s -o /dev/null -w '%{http_code}' $N1/collections/users/docs/eve)
[[ $code == 200 ]] && ok "eve readable through the normal API" || fail "eve: $code"

$C kill 4 >/dev/null
for _ in $(seq 1 40); do
  st=$(curl -s $N1/cluster | j "[m['status'] for m in d['members'] if m['node_id']=='node4'][0]")
  [[ $st == DOWN ]] && break; sleep 0.25
done
r=$(curl -s -XPOST $N1/admin/repair | j "(d['ranges_skipped'] > 0, d['ranges_skipped'] + d['ranges_in_sync'] == d['ranges'], len(d['errors']) > 0)")
[[ $r == "(True, True, True)" ]] && ok "repair with a replica down skips its ranges and reports why" || fail "repair with node down: $r"

echo "range movements (join / decommission / removenode)"
$C restart 4 >/dev/null
for _ in $(seq 1 60); do converged && break; sleep 0.25; done
converged && ok "node4 back; cluster of 4 NORMAL nodes" || fail "node4 did not rejoin"

# Expect each bulk key on exactly RF=3 replicas, and every replica to have it.
placement() {  # usage: placement <coordinator port> [collection count] -> "ok" or the problem
  python3 - "$1" "${2:-bulk}" "${3:-5000}" <<'PY'
import json, sys, http.client, random
port, coll, count = int(sys.argv[1]), sys.argv[2], int(sys.argv[3])
def call(p, method, path, body=None):
    c = http.client.HTTPConnection("127.0.0.1", p, timeout=10)
    c.request(method, path, json.dumps(body) if body is not None else None, {"content-type": "application/json"})
    r = c.getresponse(); return json.loads(r.read())
random.seed(7)
bad = []
for i in random.sample(range(count), min(150, count)):
    k = f"d{i:05}"
    reps = [r["node_id"] for r in call(port, "GET", f"/cluster/owners/{coll}/{k}")["replicas"]]
    for r in reps:
        rec = call(7000 + int(r[4:]), "POST", "/internal/get", {"c": coll, "k": k})
        if not rec or rec["v"] is None: bad.append(f"{k}@{r}")
print("ok" if not bad else f"{len(bad)} missing, e.g. {bad[:3]}")
PY
}
local_total() { for i in "$@"; do curl -s 127.0.0.1:700$i/collections | j "d['local_document_counts'].get('bulk',0)"; done | paste -sd+ - | bc; }
wait_state() {  # wait_state <port> <node> <STATE>
  for _ in $(seq 1 120); do
    st=$(curl -s 127.0.0.1:$1/cluster | j "[m['state'] for m in d['members'] if m['node_id']=='$2'][0]" 2>/dev/null)
    [[ $st == $3 ]] && return 0; sleep 0.25
  done; return 1
}

# --- join: a brand-new node5 bootstraps and streams its ranges ---
# longer ring delay on the joiner leaves a clear window to write during the join
RING_DELAY_MS=6000 $C join 5 >/dev/null
wait_state 7005 node5 JOINING && ok "node5 starts as JOINING" || fail "node5 did not start JOINING"
for p in 7001 7002 7003 7004; do wait_state $p node5 JOINING || true; done
python3 - <<'PY'
import json, http.client
c = http.client.HTTPConnection("127.0.0.1", 7001)
for i in range(300):
    c.request("PUT", f"/collections/during_join/docs/d{i:05}", json.dumps({"i": i}), {"content-type": "application/json"})
    r = c.getresponse(); r.read(); assert r.status == 200
PY
st=$(curl -s $N1/cluster | j "[m['state'] for m in d['members'] if m['node_id']=='node5'][0]")
[[ $st == JOINING ]] && ok "wrote 300 docs while node5 was still JOINING" || fail "node5 already $st; write window missed"
wait_state 7001 node5 NORMAL && ok "node5 bootstrapped and became NORMAL" || fail "node5 never became NORMAL"
for p in 7002 7003 7004 7005; do wait_state $p node5 NORMAL || fail "port $p never saw node5 NORMAL"; done
streamed=$(grep -o 'bootstrap streaming complete.*entries[^ ]*' logs/node5.log | tail -1 | grep -oE '[0-9]+' | head -1)
n5=$(local_total 5)
[[ $n5 -gt 1000 ]] && ok "node5 streamed its share of the data ($n5 bulk docs)" || fail "node5 only has $n5 docs"
r=$(placement 7001); [[ $r == ok ]] && ok "every sampled key is on all 3 of its new replicas" || fail "placement after join: $r"
r=$(placement 7001 during_join 300); [[ $r == ok ]] && ok "writes made during the join reached node5 (pending replica)" || fail "during-join writes: $r"
before=$(local_total 1 2 3 4 5)
$C cleanup | sed 's/^/    /'
after=$(local_total 1 2 3 4 5)
[[ $before -gt 15000 && $after == 15000 ]] && ok "cleanup removed copies old owners no longer own ($before -> $after = 5000 x RF)" || fail "cleanup: $before -> $after"
r=$(curl -s -XPOST $N2/collections/bulk/find -H 'content-type: application/json' -d '{"limit":0}' | j "d['matched']")
[[ $r == 5000 ]] && ok "find still sees all 5000 docs" || fail "find after join: $r"

# --- decommission: node2 hands its ranges to the others and leaves ---
code=$(curl -s -o /dev/null -w '%{http_code}' -XPOST $N2/admin/decommission)
[[ $code == 200 ]] && ok "node2 decommissioned" || fail "decommission returned $code"
wait_state 7001 node2 LEFT && ok "cluster sees node2 as LEFT" || fail "node2 not LEFT"
$C kill 2 >/dev/null
for p in 7003 7004 7005; do wait_state $p node2 LEFT || fail "port $p never saw node2 LEFT"; done
r=$(placement 7001); [[ $r == ok ]] && ok "node2's ranges re-replicated to their new owners" || fail "placement after decommission: $r"
$C cleanup >/dev/null
r=$(local_total 1 3 4 5); [[ $r == 15000 ]] && ok "remaining 4 nodes hold exactly 5000 x RF copies" || fail "copies after decommission: $r"

# --- removenode: node5 dies for good; node1 re-replicates its data from survivors ---
$C kill 5 >/dev/null
for _ in $(seq 1 40); do
  st=$(curl -s $N1/cluster | j "[m['status'] for m in d['members'] if m['node_id']=='node5'][0]")
  [[ $st == DOWN ]] && break; sleep 0.25
done
code=$(curl -s -o /dev/null -w '%{http_code}' -XPOST $N1/admin/removenode/node3)
[[ $code == 409 ]] && ok "removenode refuses a live node (409)" || fail "expected 409, got $code"
r=$(curl -s -XPOST $N1/admin/removenode/node5 | j "(d['operation'], d['entries_streamed'] > 0)")
[[ $r == "('removenode', True)" ]] && ok "removenode node5 re-streamed its data from surviving replicas" || fail "removenode: $r"
wait_state 7003 node5 LEFT && ok "node5 is LEFT cluster-wide" || fail "node5 not LEFT"
r=$(placement 7003); [[ $r == ok ]] && ok "every key fully replicated on the 3 survivors" || fail "placement after removenode: $r"
r=$(for i in 1 3 4; do curl -s 127.0.0.1:700$i/collections | j "d['local_document_counts'].get('bulk',0)"; done | sort -u | tr '\n' ' ')
[[ $r == "5000 " ]] && ok "3 nodes, RF=3: each holds all 5000 docs" || fail "per-node counts: $r"
wait_state 7004 node5 LEFT || fail "node4 never saw node5 LEFT"
r=$(curl -s -XPOST $N4/collections/users/find -H 'content-type: application/json' -d '{}' | j "(d['matched'], d['partial'])")
[[ $r == "(3, False)" ]] && ok "queries complete and not partial with LEFT nodes gone ($r)" || fail "users find: $r"
code=$(curl -s -o /dev/null -w '%{http_code}' -XPUT "$N1/collections/users/docs/zed?consistency=all" -H 'content-type: application/json' -d '{"n":1}')
[[ $code == 200 ]] && ok "writes at ALL succeed on the shrunken cluster" || fail "ALL write: $code"

echo "secondary indexes"
q() {  # q <port> <collection> <find body> <python expr over d>
  curl -s -XPOST 127.0.0.1:$1/collections/$2/find -H 'content-type: application/json' -d "$3" | j "$4"
}
plan='(d["matched"], sorted(set((p["index"], p["examined"]) for p in d["explain"].values())))'
idx_state() {  # "down" while the node is still starting
  curl -s 127.0.0.1:$1/collections/$2/indexes 2>/dev/null | j "([i['state'] for i in d['indexes'] if i['field']=='$3'] or ['absent'])[0]" 2>/dev/null || echo down
}
wait_index() {  # wait_index <collection> <field> <state> <ports...>
  local c=$1 f=$2 want=$3; shift 3
  for p in "$@"; do
    for _ in $(seq 1 80); do [[ $(idx_state $p $c $f) == $want ]] && break; sleep 0.25; done
    [[ $(idx_state $p $c $f) == $want ]] || return 1
  done
}
ids_hash() { q $1 bulk "$2" '__import__("hashlib").md5(",".join(x["_id"] for x in d["docs"]).encode()).hexdigest()'; }

body='{"filter":{"bucket":7},"explain":true}'
before=$(ids_hash 7001 "$body")
r=$(q 7001 bulk "$body" "$plan")
[[ $r == "(500, [(None, 5000)])" ]] && ok "without an index every node scans all 5000 docs: $r" || fail "baseline: $r"
code=$(curl -s -o /dev/null -w '%{http_code}' -XPOST $N1/collections/bulk/indexes -H 'content-type: application/json' -d '{"field":"bucket"}')
[[ $code == 201 ]] && ok "created index bulk.bucket (201)" || fail "create index: $code"
wait_index bulk bucket ready 7001 7003 7004 && ok "index built on every node (schema spread by gossip)" || fail "index not ready everywhere"
r=$(q 7003 bulk "$body" "$plan")
[[ $r == "(500, [('bucket', 500)])" ]] && ok "same query now examines only the 500 matches per node: $r" || fail "indexed: $r"
[[ $(ids_hash 7003 "$body") == $before ]] && ok "indexed and full-scan results are identical" || fail "result sets differ"

curl -s -XPOST $N1/collections/bulk/indexes -H 'content-type: application/json' -d '{"field":"n"}' >/dev/null
wait_index bulk n ready 7001 7003 7004 || fail "n index not ready"
r=$(q 7004 bulk '{"filter":{"n":{"$gte":4990}},"explain":true}' "$plan")
[[ $r == "(10, [('n', 10)])" ]] && ok "range query via index: $r" || fail "range: $r"
r=$(q 7001 bulk '{"filter":{"n":{"$gt":4990,"$lt":4995}},"explain":true}' "$plan")
[[ $r == "(4, [('n', 4)])" ]] && ok "bounded range (4990, 4995): $r" || fail "bounded range: $r"
r=$(q 7001 bulk '{"filter":{"bucket":{"$in":[1,2]},"n":{"$lt":50}},"explain":true}' "$plan")
[[ $r == "(10, [('bucket', 1000)])" ]] && ok "two indexed conditions: planner prefers \$in over a range (rank-based, not cost-based): $r" || fail "multi-condition: $r"

curl -s -XPATCH $N1/collections/bulk/docs/d00007 -H 'content-type: application/json' -d '{"$set":{"bucket":3}}' >/dev/null
curl -s -XDELETE $N3/collections/bulk/docs/d00013 >/dev/null
r7=$(q 7004 bulk '{"filter":{"bucket":7},"explain":true}' "$plan"); r3=$(q 7004 bulk '{"filter":{"bucket":3},"explain":true}' "$plan")
[[ $r7 == "(499, [('bucket', 499)])" && $r3 == "(500, [('bucket', 500)])" ]] \
  && ok "index follows updates and deletes exactly, no stale entries (7: $r7, 3: $r3)" || fail "maintenance: 7=$r7 3=$r3"

# An index created while a node is down is built when it comes back.
$C kill 4 >/dev/null
for _ in $(seq 1 40); do
  st=$(curl -s $N1/cluster | j "[m['status'] for m in d['members'] if m['node_id']=='node4'][0]"); [[ $st == DOWN ]] && break; sleep 0.25
done
curl -s -XPOST $N1/collections/during_join/indexes -H 'content-type: application/json' -d '{"field":"i"}' >/dev/null
wait_index during_join i ready 7001 7003 || fail "during_join.i not ready on live nodes"
$C restart 4 >/dev/null
wait_index during_join i ready 7004 && ok "node4 was down at creation; built the index after rejoining" || fail "node4 never built during_join.i"
[[ $(grep -c 'building index.*field=.*bucket\|building index.*bucket' logs/node4.log) == 1 && $(idx_state 7004 bulk bucket) == ready ]] \
  && ok "existing indexes survived node4's restart without a rebuild" || fail "bucket index rebuilt or not ready on node4"
for _ in $(seq 1 60); do converged3=1; for p in 7001 7003 7004; do
  up=$(curl -s 127.0.0.1:$p/cluster | j "sum(m['status']=='UP' for m in d['members'] if m['state']=='NORMAL')"); [[ $up == 3 ]] || converged3=0; done
  [[ $converged3 == 1 ]] && break; sleep 0.25; done
r=$(q 7004 during_join '{"filter":{"i":{"$lt":10}},"explain":true}' "$plan")
[[ $r == "(10, [('i', 10)])" ]] && ok "query on the new index across all nodes: $r" || fail "during_join.i: $r"

code=$(curl -s -o /dev/null -w '%{http_code}' -XDELETE $N3/collections/bulk/indexes/bucket)
[[ $code == 204 ]] && ok "dropped index bulk.bucket" || fail "drop: $code"
wait_index bulk bucket absent 7001 7003 7004 || fail "drop did not spread"
r=$(q 7001 bulk '{"filter":{"bucket":7},"explain":true}' "$plan")
[[ $r == "(499, [(None, 4999)])" ]] && ok "after drop, queries fall back to scanning: $r" || fail "after drop: $r"
code=$(curl -s -o /dev/null -w '%{http_code}' -XDELETE $N3/collections/bulk/indexes/bucket)
[[ $code == 404 ]] && ok "dropping again is 404" || fail "second drop: $code"

echo "cassa-cli"
CLI="target/release/cassa-cli -H 127.0.0.1:7001"
r=$($CLI -o json -e 'db.cli.insert({_id: "a", n: 1, tags: ["x"]})' -e "db.cli.insert({_id: 'b', n: 2})" -e 'db.cli.count()' | tail -1)
[[ $r == 2 ]] && ok "cli: JSON5 inserts and count via -e" || fail "cli count: $r"
script=$(mktemp)
cat > "$script" <<'CASSA'
// multi-line statements and comments
db.cli.update("a", {
  $inc: {n: 10},
  $push: {tags: "y"},
})
db.cli.find({n: {$gt: 5}})
CASSA
r=$($CLI -o json -f "$script" | tail -1 | j "(d['_id'], d['n'], d['tags'])")
[[ $r == "('a', 11, ['x', 'y'])" ]] && ok "cli: script file with multi-line statements: $r" || fail "cli script: $r"
r=$(echo 'db.cli.find({}).sort({n: -1}).limit(1)' | $CLI -o json | j "d['_id']")
[[ $r == a ]] && ok "cli: statements from stdin" || fail "cli stdin: $r"
code=0; $CLI -e 'db.cli.find({n: {$bogus: 1}})' >/dev/null 2>&1 || code=$?
[[ $code == 1 ]] && ok "cli: server errors give exit code 1" || fail "cli exit code: $code"
r=$(target/release/cassa-cli -H 127.0.0.1:7999,127.0.0.1:7003 -o json -e 'db.cli.get("b")' 2>/dev/null | j "d['n']")
[[ $r == 2 ]] && ok "cli: fails over to the next host" || fail "cli failover: $r"
r=$($CLI -o table -e 'db.cli.find({}).sort({_id: 1})' | head -1 | tr -s ' ')
[[ $r == "_id n tags" ]] && ok "cli: table output" || fail "cli table header: $r"

echo
echo "all $pass checks passed"
