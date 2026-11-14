# cassa

A small distributed JSON document database in Rust: **Cassandra-style distribution**
with **MongoDB-style documents and queries**.

| From Cassandra | From MongoDB |
|---|---|
| Gossip membership + failure detection, seed nodes | Schemaless JSON documents with `_id` |
| Consistent-hash ring with vnodes, masterless (any node coordinates) | Query filters: `$eq $ne $gt $gte $lt $lte $in $nin $exists $size $not $and $or $nor`, dotted paths, array matching |
| Replication factor + tunable consistency `one` / `quorum` / `all` | Sort / skip / limit |
| Last-write-wins timestamps, tombstones, gc_grace | Update operators: `$set $unset $inc $push` |
| Hinted handoff, read repair, Merkle-tree anti-entropy repair | |
| Node lifecycle: bootstrap, decommission, removenode, cleanup | |
| LSM storage: commit log → memtable → SSTables, bloom filters, size-tiered compaction | |

## Run a cluster

```bash
scripts/cluster.sh start 3      # nodes on 127.0.0.1:7001..7003, RF=3
scripts/cluster.sh status       # gossip view
scripts/cluster.sh kill 2       # hard-kill node2
scripts/cluster.sh restart 2
scripts/cluster.sh repair       # anti-entropy repair on every node
scripts/cluster.sh join 4       # add a new node4: it bootstraps and streams its ranges
scripts/cluster.sh cleanup      # drop data old owners no longer own (after a join)
scripts/cluster.sh decommission 2   # node2 streams its ranges away, then stops
scripts/cluster.sh removenode node3 # re-replicate a dead node's data, mark it LEFT
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
curl localhost:7001/collections                 # local doc counts + storage stats (memtable, SSTables)
curl -XPOST 'localhost:7001/admin/compact?major=true'   # flush + compact this node
curl -XPOST localhost:7001/admin/repair         # repair this node's primary ranges -> report
curl localhost:7001/admin/repair                # last repair report
curl -XPOST localhost:7002/admin/decommission   # leave the cluster, handing ranges over
curl -XPOST localhost:7001/admin/removenode/n3  # remove a dead node
curl -XPOST localhost:7001/admin/cleanup        # drop data for ranges this node no longer owns
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
  movement.rs     bootstrap / decommission / removenode / cleanup, range streaming
  repair.rs       Merkle-tree anti-entropy repair (validation, tree diff, streaming), scheduler
  storage/
    mod.rs        LSM engine: memtable, flush/compaction worker, reads & scans
    commitlog.rs  segmented write-ahead log with CRC-framed records
    sstable.rs    SSTable format: 4 KiB blocks, sparse index, bloom filter, CRCs
    merge.rs      k-way merge with last-write-wins reconciliation
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

## On-disk storage

Each node runs a log-structured merge tree, like Cassandra:

```
write ─► commit log segment ─► memtable ──(> --memtable-mb)──► frozen ─► SSTable (background flush)
                                                                        │
                       size-tiered compaction merges ≥ --compaction-threshold similar-sized SSTables
```

- **Commit log:** records are framed as `len | crc32 | json`. A torn tail after a crash is
  detected and skipped. Segments are deleted once their memtable is safely in an SSTable.
  Pass `--fsync` to fsync after every write batch.
- **SSTables** (`sst-<gen>.db`) are immutable and sorted. Data sits in ~4 KiB blocks, each with
  a CRC32. Each file has a sparse block index and a bloom filter (10 bits per key), both loaded
  into memory, and a footer holding the max timestamp. Files are written to a `.tmp` name,
  fsynced and renamed into place, so a crash never leaves a half-written table behind.
- **Writes never read.** The newest version of a key is chosen at read time by timestamp
  across the memtable, memtables being flushed, and SSTables. Point reads skip an SSTable if
  its bloom filter rules the key out, or if its max timestamp is older than the best version
  already found.
- **Scans** (`find`) stream a k-way merge over all sources, so a node's data no longer has to
  fit in RAM. Only memtables, indexes and bloom filters stay in memory.
- **Compaction** merges similar-sized SSTables, keeping the newest version of each key.
  Tombstones older than `--gc-grace-secs` are purged, but only if no SSTable outside the
  compaction and no memtable might still hold an older value for that key. Otherwise a
  deleted document could come back.
- **Graceful shutdown** (SIGINT) flushes the memtable. After a crash, the commit log is
  replayed on startup.

## Anti-entropy repair

Hinted handoff and read repair fix most divergence, but not all of it. Hints are lost when a
coordinator restarts, read repair only touches keys that get read, and a node can lose its
disk. `POST /admin/repair`, or `--repair-interval-secs N` for scheduled runs, repairs this
node's **primary** token ranges, like `nodetool repair -pr`. Run it on every node to cover the
whole ring:

1. **Validation.** Every replica of those ranges builds one Merkle tree per range in a single
   scan of its local data. Each range is split into 2^`depth` leaves (default 1024) by token.
   A leaf's hash is the XOR of its `(key, timestamp, value)` entry hashes, so it doesn't depend
   on order. Inner nodes hash their children.
2. **Comparison.** Each replica's trees are compared with this node's from the root down, so
   only differing leaves come out.
3. **Streaming.** All replicas' entries in those leaves (tombstones included) are fetched. The
   newest version of each key is chosen by last-write-wins, and each replica is sent only what
   it's missing or has stale.

Ranges with a replica down are skipped and listed in the report's `errors`. Only one repair
runs per node at a time (a second request gets `409`). Run a repair on every node more often
than `--gc-grace-secs` (10 days by default). Otherwise a replica that missed a delete can bring
the document back after the tombstone is purged.

The end-to-end test wipes a node's data directory, and repair rebuilds all of it from its peers.
It also checks that a write and a delete that each reached only one replica get propagated.

## Adding and removing nodes

Nodes move through `JOINING → NORMAL → LEAVING → LEFT`, and the state travels in gossip.
Every node keeps two rings. The **current** ring (NORMAL + LEAVING nodes) serves reads and
counts write acknowledgements. The **future** ring (NORMAL + JOINING nodes) is what the ring
will look like once the move finishes. A node that's in the future ring for a key but not the
current one is a **pending replica**. It gets every write but doesn't count toward the
consistency level, so writes made while data is moving aren't lost.

Every move is built from one step, **pull**: a node copies token ranges from the live
replicas that currently hold them, in pages over `/internal/stream`. It pulls from all live
replicas and keeps the newest version of each key, so one stale replica can't leave it with
old data.

- **Join.** A node with an empty data directory starts as JOINING. If it finds an existing
  cluster, it waits `--ring-delay-ms` for every node to start sending it writes. Then it
  pulls the ranges it will own and switches to NORMAL. A `bootstrapped` marker file in its
  data directory stops it from doing this again. Pass `--no-bootstrap` for the first nodes of
  a brand-new cluster, or to rejoin in place after losing a disk (then run repair).
- **Decommission** (`POST /admin/decommission` on the node that should leave). It goes
  LEAVING, each new owner pulls the ranges it gains, then it goes LEFT and can be shut down.
  If a pull fails, it goes back to NORMAL.
- **Removenode** (`POST /admin/removenode/<id>`, for a node that is DOWN and not coming back).
  The dead node is marked LEAVING on its behalf, so writes go to the new owners. The new
  owners pull from the surviving replicas, then the dead node is marked LEFT.
- **Cleanup** (`POST /admin/cleanup`). After a join, the old owners still have copies of data
  they gave away. Cleanup rewrites their SSTables without it. It refuses while any node is
  joining or leaving.

Decommission and removenode refuse to run while another node is joining or leaving (`409`).
Writes to a LEFT node's queue are dropped, and LEFT nodes don't count toward `find`'s
`partial` flag.

## Simplifications (vs. the real things)

- Fixed-timeout failure detector instead of phi-accrual. Gossip sends full state, not digests.
- Size-tiered compaction only, with no leveled compaction. There's no block cache (the OS page
  cache does that job), no compression, and no write throttling if flushes fall behind.
- Values are stored as JSON text inside SSTables, not a binary format like BSON.
- Hints are in memory only, so they're lost if the coordinator restarts (repair covers that).
- Repair rescans a replica's whole dataset each run. There are no incremental or
  per-SSTable repaired markers, and streamed data isn't paged, so one repair response holds
  all entries in the differing leaves.
- Range movements copy from every live replica rather than picking one source. That's
  simpler and keeps the newest data, but it moves up to RF times as much data. Tokens come
  from the node id, so you can't choose or move tokens, and there's no "replace this dead
  node" shortcut: use removenode, then join a fresh node.
- A removed node that comes back with its old data dir rejoins as NORMAL with stale data.
  Wipe its data dir first.
- `find` is a full scatter/gather scan. There are no secondary indexes.
- `PATCH` is read-modify-write and not atomic under concurrent updaters (LWW).
- Inserts are upserts, as in Cassandra. A duplicate `_id` overwrites.
- Inter-node traffic is plain HTTP/JSON with no auth or TLS.
