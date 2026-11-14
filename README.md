# cassa

A small distributed JSON document database in Rust: **Cassandra-style distribution**
with **MongoDB-style documents and queries**.

| From Cassandra | From MongoDB |
|---|---|
| Gossip membership + failure detection, seed nodes | Schemaless JSON documents with `_id` |
| Consistent-hash ring with vnodes, masterless (any node coordinates) | Query filters: `$eq $ne $gt $gte $lt $lte $in $nin $exists $size $not $and $or $nor`, dotted paths, array matching |
| Replication factor + tunable consistency `one` / `quorum` / `all` | Sort / skip / limit |
| Last-write-wins timestamps, tombstones, gc_grace | Update operators: `$set $unset $inc $push` |
| Hinted handoff, read repair | |
| Commit log + memtable + snapshot compaction | |

## Run a cluster

```bash
scripts/cluster.sh start 3      # nodes on 127.0.0.1:7001..7003, RF=3
scripts/cluster.sh status       # gossip view
scripts/cluster.sh kill 2       # hard-kill node2
scripts/cluster.sh restart 2
scripts/cluster.sh clean        # stop + wipe data/ logs/
scripts/e2e.sh                  # full end-to-end test (4 nodes, failure + recovery)
```

Or by hand:

```bash
cargo run --release -- --node-id n1 --listen 127.0.0.1:7001 --seeds 127.0.0.1:7001
cargo run --release -- --node-id n2 --listen 127.0.0.1:7002 --seeds 127.0.0.1:7001
```

See `cassa --help` for all flags (replication factor, vnodes, gossip interval, failure timeout, fsync, ...).

## API

Every endpoint accepts `?consistency=one|quorum|all` (default `quorum`).

```bash
# insert (generates _id if absent) -> {"_id": "..."}
curl -XPOST localhost:7001/collections/users/docs -H 'content-type: application/json' \
     -d '{"name":"ann","age":31,"addr":{"city":"Leeds"}}'

curl localhost:7002/collections/users/docs/<id>                     # get
curl -XPUT localhost:7001/collections/users/docs/bob -d '{...}'     # upsert/replace
curl -XPATCH localhost:7001/collections/users/docs/bob \
     -d '{"$inc":{"age":1},"$set":{"addr.zip":"E1"}}'               # update operators
curl -XDELETE localhost:7001/collections/users/docs/bob             # delete (tombstone)

curl -XPOST localhost:7003/collections/users/find -d '{
  "filter": {"addr.city": "Leeds", "age": {"$gte": 30}},
  "sort": {"age": -1}, "skip": 0, "limit": 10
}'   # -> {"docs": [...], "matched": N, "partial": false}

curl localhost:7001/cluster                     # members, UP/DOWN, ownership, pending hints
curl localhost:7001/cluster/owners/users/bob    # which nodes hold a key
curl localhost:7001/collections                 # local per-node doc counts
```

Errors: `400` bad request, `404` not found, `503` not enough live replicas
(Unavailable), `504` replicas didn't acknowledge in time.

## How it works

```
src/
  gossip.rs       membership: push/pull state exchange each interval, (generation, heartbeat)
                  versioning, timeout-based failure detector, ring rebuilt on membership change
  ring.rs         token ring (FNV-1a + splitmix64), vnodes, replica placement, ownership
  coordinator.rs  replicated write/read with consistency levels, hinted handoff,
                  read repair, two-phase scatter/gather find
  storage.rs      commitlog.jsonl + in-memory BTreeMaps + snapshot.json compaction, LWW apply
  query.rs        filter matching, validation, sorting, update operators
  api.rs          axum HTTP API (public + /internal/* node-to-node)
```

**Write path:** the coordinator timestamps the mutation, finds the `RF` replicas on the ring,
fails with `503` if fewer than the consistency level are alive, sends to all live replicas in
parallel, and returns once enough have acknowledged. Down or failed replicas get a *hint*, which
is replayed when gossip sees the node again.

**Read path:** asks the live replicas, waits for enough responses to satisfy the consistency
level, returns the newest version (LWW), then repairs stale replicas in the background.

**Find:** phase 1 asks every live node for ids whose local version matches. Phase 2 fetches
every node's version of those ids, resolves them by LWW, and runs the filter again. That way a
stale replica can't return a document that was since updated or deleted.

## Simplifications (vs. the real things)

- Fixed-timeout failure detector instead of phi-accrual. Gossip sends full state, not digests.
- No SSTables: the whole dataset lives in memory and is snapshotted to one JSON file.
- Hints are in memory only, so they're lost if the coordinator restarts. No anti-entropy
  (Merkle-tree) repair.
- No data streaming when nodes join or leave. A new node only gets data through new writes,
  hints, and read repair. Changing the cluster under live data needs a repair this doesn't
  implement.
- `find` is a full scatter/gather scan. There are no secondary indexes.
- `PATCH` is read-modify-write and not atomic under concurrent updaters (LWW).
- Inserts are upserts, as in Cassandra. A duplicate `_id` overwrites.
- Inter-node traffic is plain HTTP/JSON with no auth or TLS.
