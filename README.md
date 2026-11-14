# cassa

A small distributed JSON document database in Rust: **Cassandra-style distribution**
with **MongoDB-style documents and queries**, plus a shell to talk to it.

| From Cassandra | From MongoDB |
|---|---|
| Gossip membership + failure detection, seed nodes | Schemaless JSON documents with `_id` |
| Consistent-hash ring with vnodes, masterless (any node coordinates) | Query filters: `$eq $ne $gt $gte $lt $lte $in $nin $exists $size $not $and $or $nor`, dotted paths, array matching |
| Replication factor + tunable consistency `one` / `quorum` / `all` | Sort / skip / limit |
| Last-write-wins timestamps, tombstones, gc_grace | Update operators: `$set $unset $inc $push` |
| Hinted handoff, read repair, Merkle-tree anti-entropy repair | Secondary indexes (equality, `$in`, ranges, multikey arrays) |
| Node lifecycle: bootstrap, decommission, removenode, cleanup | A `mongo`-style shell: `db.users.find({age: {$gt: 30}})` |
| | An async Rust driver with typed collections |
| LSM storage: commit log → memtable → SSTables, bloom filters, size-tiered compaction | |

**Contents:**
[Quick start](#quick-start) ·
[The shell](#the-shell-cassa-cli) ·
[Examples](#examples) ·
[Scripting](#scripting) ·
[Rust driver](#rust-driver) ·
[Operating a cluster](#operating-a-cluster) ·
[HTTP API](#http-api) ·
[Server flags](#running-the-server) ·
[How it works](#how-it-works) ·
[Simplifications](#simplifications-vs-the-real-things)

## Quick start

```bash
cargo build --release              # builds target/release/cassa and target/release/cassa-cli
scripts/cluster.sh start 3         # 3 nodes on 127.0.0.1:7001..7003, replication factor 3
target/release/cassa-cli           # open the shell (connects to 127.0.0.1:7001)
```

```text
cassa shell — connected to 127.0.0.1:7001 (consistency quorum). Type 'help' for examples, 'exit' to quit.
cassa 127.0.0.1:7001> db.users.insert({_id: "ann", name: "Ann Lee", age: 31, city: "Leeds"})
inserted 1 document, _id: ann (4 ms)
cassa 127.0.0.1:7001> db.users.find({age: {$gt: 30}})
{
  "_id": "ann",
  "name": "Ann Lee",
  "age": 31,
  "city": "Leeds"
}
1 of 1 matching document (3 ms)
cassa 127.0.0.1:7001> show cluster
node   address         status  state   ownership_%  last_seen_s
-----  --------------  ------  ------  -----------  -----------
node1  127.0.0.1:7001  UP      NORMAL  33.28        0.0
node2  127.0.0.1:7002  UP      NORMAL  28.99        0.5
node3  127.0.0.1:7003  UP      NORMAL  37.73        0.5
coordinator node1 · RF 3 · pending hints 0 · range movements in progress: false
```

To try every example in this README at once, run the tour script:

```bash
target/release/cassa-cli -o table -f examples/tour.cassa
```

When you're done, `scripts/cluster.sh stop` stops the nodes and `scripts/cluster.sh clean`
also deletes their data.

## The shell (cassa-cli)

```bash
cassa-cli                                         # interactive shell with history (~/.cassa_history)
cassa-cli -e 'db.users.count()'                   # run one statement and exit
cassa-cli -e 'db.a.insert({x: 1})' -e 'db.a.count()'   # several statements
cassa-cli -f examples/tour.cassa                  # run a script file
echo 'db.users.find({city: "York"})' | cassa-cli  # read statements from stdin
cassa-cli -H 10.0.0.5:7001,10.0.0.6:7001          # fall back to the second host if the first is down
CASSA_HOST=10.0.0.5:7001 cassa-cli                # host from the environment
```

| Flag | Meaning |
|---|---|
| `-H, --host <list>` | Nodes to talk to, comma-separated; later ones are used if earlier ones are unreachable. Default `127.0.0.1:7001`, or `$CASSA_HOST`. |
| `-e, --execute <stmt>` | Run a statement (repeatable), then exit. |
| `-f, --file <path>` | Run a script file, then exit. |
| `-c, --consistency <cl>` | `one`, `quorum` (default) or `all`. |
| `-o, --format <fmt>` | `pretty` (default), `table`, or `json` (JSON Lines, nothing else printed). |
| `--continue-on-error` | Keep running a script after a failed statement. |

**Syntax notes:**

- Arguments are [JSON5](https://json5.org): keys can be unquoted (`{age: 31}`), strings can use
  `'single quotes'`, and trailing commas are fine. **Dotted keys need quotes:** `{"addr.city": "Leeds"}`.
- A statement can span several lines; the shell keeps reading (`...>`) until brackets and
  quotes balance. Ctrl-C cancels a half-typed statement.
- `//` starts a comment anywhere outside a string; `#` and `--` start a comment at the beginning of a line.
- A trailing `;` is optional.
- In the interactive shell, `find()` without `.limit()` shows the first 20 documents and tells
  you how many more matched. Scripts and `-e` return everything.

**Shell commands:**

| Command | What it does |
|---|---|
| `help`, `help query`, `help update`, `help admin` | Built-in examples |
| `show cluster` (or `status`) | Nodes, UP/DOWN, lifecycle state, token ownership |
| `show collections` | Document counts on the connected node (including replicas) |
| `consistency [one\|quorum\|all]` | Show or set the default consistency level |
| `format [pretty\|table\|json]` | Show or set the output format |
| `timing [on\|off]` | Show elapsed time after each statement (on by default interactively) |
| `connect host:port[,host:port]` | Switch to other node(s) |
| `repair` | Anti-entropy repair of the connected node's primary ranges |
| `compact` / `compact major` | Flush and compact the connected node's storage |
| `cleanup` | Drop data the connected node no longer owns (after a node joins) |
| `exit` / `quit` | Leave the shell (Ctrl-D also works) |

**Collection methods** (`db.<collection>.<method>(...)`):

| Method | Example |
|---|---|
| `insert(doc)` | `db.users.insert({name: "Ann"})` |
| `insertMany([docs])` | `db.users.insertMany([{name: "A"}, {name: "B"}])` |
| `get(id)` | `db.users.get("ann")` |
| `find(filter?)` + `.sort() .skip() .limit() .count() .explain()` | `db.users.find({age: {$gt: 30}}).sort({age: -1}).limit(5)` |
| `findOne(filter?)` | `db.users.findOne({city: "York"})` |
| `count(filter?)` | `db.users.count({city: "Leeds"})` |
| `update(id, update)` | `db.users.update("ann", {$inc: {visits: 1}})` |
| `updateMany(filter, update)` | `db.users.updateMany({city: "York"}, {$set: {region: "north"}})` |
| `replace(id, doc)` | `db.users.replace("ann", {name: "Ann", age: 32})` |
| `delete(id)` | `db.users.delete("ann")` |
| `deleteMany(filter)` | `db.users.deleteMany({inactive: true})` |
| `createIndex(field)` / `dropIndex(field)` / `indexes()` | `db.users.createIndex("addr.city")` |
| `owners(id)` | `db.users.owners("ann")`: which nodes store this document |
| `.consistency(cl)` (any method) | `db.users.get("ann").consistency("all")` |

## Examples

All of these come from [`examples/tour.cassa`](examples/tour.cassa); outputs are shown with
`format table`.

### Inserting

```js
// Documents are any JSON object. _id is optional; a UUID is generated if missing.
db.users.insert({_id: "ann", name: "Ann Lee", age: 31, city: "Leeds",
                 tags: ["admin", "beta"], addr: {street: "1 Park Row", zip: "LS1"}})

db.notes.insert({text: "remember the milk", pinned: true})
// inserted 1 document, _id: 260bd833-b433-426e-94ff-302f15760834

db.users.insertMany([
  {_id: "bob", name: "Bob Hart", age: 25, city: "York",  tags: ["beta"], visits: 3},
  {_id: "cat", name: "Cat Moss", age: 42, city: "Leeds", addr: {zip: "LS6"}, vip: true},
  {_id: "dan", name: "Dan Webb", age: 17, city: "Hull",  visits: 12},
  {_id: "eve", name: "Eve Shaw", age: 58, city: "York",  tags: [], vip: false},
])

db.products.insertMany([
  {_id: "p1", name: "Kettle",  price: 24.99, stock: 12,  category: "kitchen", ratings: [5, 4, 5]},
  {_id: "p2", name: "Toaster", price: 39.5,  stock: 0,   category: "kitchen", ratings: [3]},
  {_id: "p3", name: "Lamp",    price: 18,    stock: 40,  category: "living",  ratings: []},
  {_id: "p4", name: "Rug",     price: 120,   stock: 3,   category: "living"},
  {_id: "p5", name: "Mug",     price: 6.5,   stock: 200, category: "kitchen", ratings: [4, 4]},
])
```

`insert` is an upsert, as in Cassandra: inserting an existing `_id` replaces that document.

### Reading

```js
db.users.get("ann")                  // by _id: a single-partition read, the fastest lookup
db.users.findOne({city: "York"})     // first match
db.users.count()                     // 5
db.users.count({city: "Leeds"})      // 2
```

```text
_id  name     age  city   tags              addr
---  -------  ---  -----  ----------------  -----------------------------------
ann  Ann Lee  31   Leeds  ["admin","beta"]  {"street":"1 Park Row","zip":"LS1"}
```

### Filtering

| You want | Filter |
|---|---|
| equality | `{city: "Leeds"}` |
| a nested field | `{"addr.zip": "LS1"}` |
| an array containing a value | `{tags: "beta"}` |
| an array element by position | `{"tags.0": "admin"}` |
| a range | `{age: {$gte: 18, $lt: 40}}` |
| any of / none of | `{city: {$in: ["York", "Hull"]}}`, `{city: {$nin: ["York", "Hull"]}}` |
| not equal | `{age: {$ne: 31}}` |
| a field that exists / is missing | `{vip: {$exists: true}}`, `{addr: {$exists: false}}` |
| null or missing | `{nickname: null}` |
| an array of a given length | `{tags: {$size: 0}}` |
| negation | `{age: {$not: {$gte: 18}}}` |
| either / both / neither | `{$or: [...]}`, `{$and: [...]}`, `{$nor: [...]}` |

```js
db.users.find({age: {$gte: 18, $lt: 40}})
```
```text
_id  name      age  city   tags              addr                                 visits
---  --------  ---  -----  ----------------  -----------------------------------  ------
ann  Ann Lee   31   Leeds  ["admin","beta"]  {"street":"1 Park Row","zip":"LS1"}
bob  Bob Hart  25   York   ["beta"]                                               3
2 of 2 matching documents
```

```js
db.users.find({$or: [{age: {$lt: 18}}, {vip: true}]})
```
```text
_id  name      age  city   addr           vip   visits
---  --------  ---  -----  -------------  ----  ------
cat  Cat Moss  42   Leeds  {"zip":"LS6"}  true
dan  Dan Webb  17   Hull                        12
2 of 2 matching documents
```

```js
db.users.find({tags: "beta"})                         // ann and bob: matches inside arrays
db.users.find({tags: {$size: 0}})                     // eve: empty array
db.users.find({addr: {$exists: false}})               // bob, dan, eve
db.users.find({$nor: [{city: "Leeds"}, {city: "York"}]})   // dan
db.products.find({price: {$lt: 20}, stock: {$gt: 0}})      // Lamp and Mug: conditions are ANDed
db.products.find({ratings: {$gte: 5}})                     // Kettle: any element >= 5
```

Comparisons only match values of the same type: `{age: {$gt: "30"}}` matches nothing when
ages are numbers. Integers and floats compare as numbers (`31` equals `31.0`).

### Sorting and paging

```js
db.users.find({}).sort({age: -1})                                  // oldest first
db.users.find({}).sort({city: 1, age: -1})                         // by city, then oldest first
db.products.find({category: "kitchen"}).sort({price: 1}).limit(2)  // two cheapest
db.users.find({}).sort({_id: 1}).skip(2).limit(2)                  // page 2 with page size 2
db.products.find({stock: {$gt: 0}}).count()                        // 4
```

```text
_id  name    price  stock  category  ratings
---  ------  -----  -----  --------  -------
p5   Mug     6.5    200    kitchen   [4,4]
p1   Kettle  24.99  12     kitchen   [5,4,5]
2 of 3 matching documents
```

Without a `sort`, results come back ordered by `_id`. Missing fields sort first (like `null`).

### Updating

```js
db.users.update("bob", {$set: {city: "Leeds", "addr.zip": "LS2"}})  // set fields; creates addr
db.users.update("bob", {$inc: {visits: 1, age: 1}})                 // add to numbers
db.users.update("bob", {$push: {tags: "moved"}})                    // append to an array
db.users.update("bob", {$unset: {visits: ""}})                      // remove a field
db.products.update("p2", {$inc: {stock: 25, price: -5.5}})          // restock and discount
```

`update` returns the document as it is after the change:

```text
_id  name      age  city   tags              addr
---  --------  ---  -----  ----------------  -------------
bob  Bob Hart  26   Leeds  ["beta","moved"]  {"zip":"LS2"}
updated 1 document
```

```js
db.products.updateMany({category: "living"}, {$set: {sale: true}})  // updated 2 documents
db.users.replace("eve", {name: "Eve Shaw", age: 59, city: "Hull"})  // replace the whole document
```

`update` is read-modify-write: it reads the document, applies the operators and writes the
result. Two concurrent updates to the same document resolve by last write wins, as in Cassandra.
`updateMany` and `deleteMany` find the matching ids first, then update or delete them one by
one. They refuse to run if some nodes are unreachable, because the match list could be
incomplete.

### Deleting

```js
db.users.delete("dan")                     // deleted dan
db.notes.deleteMany({pinned: true})        // deleted 1 document
```

A delete writes a tombstone, which is replicated like any other write and purged after
`--gc-grace-secs` (10 days by default).

### Indexes and `explain()`

Without an index, every node scans the whole collection:

```js
db.people.find({city: "Ely", age: {$gt: 85}}).explain()
```
```text
27 of 27 matching documents
plan:
node   plan       examined  matched
-----  ---------  --------  -------
node1  full scan  2000      27
node2  full scan  2000      27
node3  full scan  2000      27
```

Create an index. It's defined for the whole cluster and built in the background on every node:

```js
db.people.createIndex("age")
db.people.indexes()
```
```text
field  created_ts        state
-----  ----------------  -----
age    1794680667212413  ready
```

The same query now reads only index entries with `age > 85`, then checks the remaining
condition (`city`) on those documents:

```text
plan:
node   plan       examined  matched
-----  ---------  --------  -------
node1  index age  158       27
node2  index age  158       27
node3  index age  158       27
```

Indexes serve equality (`{age: 31}`, `{age: {$eq: 31}}`), `$in`, and ranges
(`$gt $gte $lt $lte`, numbers or strings). If several conditions are indexed, the planner prefers
equality, then `$in`, then ranges. Index a field holding arrays and each element is indexed
(`{tags: "beta"}` uses an index on `tags`). Index nested fields with a dotted path:
`createIndex("addr.city")`. Queries using `$or`, `$ne`, `$exists` or `null` scan instead.

```js
db.people.dropIndex("age")
```

### Consistency levels

Each document is stored on 3 nodes (the replication factor). The consistency level is how many
of them must answer.

```js
consistency one                                   // default for the rest of the session
db.users.get("ann").consistency("all")            // just this statement
```

With one node down, `quorum` (2 of 3) and `one` still work; `all` fails fast:

```text
cassa 127.0.0.1:7001> show cluster
node   address         status  state   ownership_%  last_seen_s
-----  --------------  ------  ------  -----------  -----------
node1  127.0.0.1:7001  UP      NORMAL  33.28        0.0
node2  127.0.0.1:7002  DOWN    NORMAL  28.99        3.6
node3  127.0.0.1:7003  UP      NORMAL  37.73        0.1

cassa 127.0.0.1:7001> db.users.get("ann").consistency("all")
error: 503 unavailable: 3 replicas required but only 2 alive

cassa 127.0.0.1:7001> db.users.find({city: "Leeds"})
...
3 of 3 matching documents — PARTIAL: some nodes were unreachable
```

`PARTIAL` means `find` couldn't ask every node. Here each document is still on two live nodes,
so nothing is actually missing. Writes made while a node is down are queued (hinted handoff)
and delivered when it comes back.

### Where does a document live?

```js
db.users.owners("ann")
```
```text
node_id  alive
-------  -----
node2    true
node3    true
node1    true
```

## Scripting

Script files hold the same statements you'd type in the shell:

```js
// seed.cassa
db.users.insertMany([
  {_id: "ann", name: "Ann", city: "Leeds"},
  {_id: "bob", name: "Bob", city: "York"},
])
db.users.createIndex("city")
db.users.count()
```

```bash
cassa-cli -f seed.cassa                     # stops at the first error, exit code 1
cassa-cli -f seed.cassa --continue-on-error # keep going; exit code 1 if anything failed
```

With `-o json`, output is JSON Lines with no status messages, so it pipes cleanly:

```bash
$ cassa-cli -o json -e 'db.people.find({age: {$gte: 89}}).sort({age: -1}).limit(3)' | jq -c '{name, age}'
{"name":"user1319","age":90}
{"name":"user200","age":90}
{"name":"user108","age":90}

$ cassa-cli -o json -e 'db.users.count({city: "Leeds"})'
2

# Export a collection, one document per line
$ cassa-cli -o json -e 'db.users.find({})' > users.jsonl

# Import it into another collection
$ jq -c '. | del(._id)' users.jsonl | while read -r doc; do echo "db.users_copy.insert($doc)"; done | cassa-cli
```

Heredocs work too:

```bash
cassa-cli <<'EOF'
db.orders.insert({_id: "o1", user: "ann", total: 42.5, items: ["p1", "p5"]})
db.orders.find({total: {$gt: 40}})
EOF
```

Exit codes: `0` success, `1` a statement failed (bad syntax, server error, unreachable cluster),
`2` bad command-line flags.

## Rust driver

[`client/`](client/README.md) is an async driver (`cassa-client`) for using cassa from Rust:

```toml
[dependencies]
cassa-client = { path = "../cassa/client" }
```

```rust
use cassa_client::{Client, Filter, Order, Update};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Serialize, Deserialize)]
struct User {
    #[serde(rename = "_id")]
    id: String,
    name: String,
    age: u32,
    city: String,
}

#[tokio::main]
async fn main() -> cassa_client::Result<()> {
    let client = Client::builder().hosts(["127.0.0.1:7001", "127.0.0.1:7002"]).build()?;
    let users = client.collection::<User>("users");

    users.insert(&User { id: "ann".into(), name: "Ann".into(), age: 31, city: "Leeds".into() }).await?;
    let ann: Option<User> = users.get("ann").await?;

    let leeds = users
        .find(Filter::new().eq("city", "Leeds").gte("age", 18))   // or json!({...})
        .sort_by("age", Order::Desc)
        .limit(10)
        .await?;
    println!("{} of {} matches", leeds.docs.len(), leeds.matched);

    users.update("ann", Update::new().inc("age", 1)).await?;
    let n = users.count(json!({"age": {"$gte": 18}})).await?;
    users.delete("ann").await?;
    Ok(())
}
```

It covers everything the shell does: typed or schemaless (`serde_json::Value`) collections,
`find` with sort/skip/limit/explain, `count`, `update_many`/`delete_many`, indexes, consistency
per collection handle, cluster info, typed errors, and moving to the next host when one is down.
See the [driver README](client/README.md) for the full API, and run the example with
`cargo run -p cassa-client --example quickstart`.

## Operating a cluster

`scripts/cluster.sh` runs a local cluster under `data/` and `logs/`:

```bash
scripts/cluster.sh start 3          # start node1..node3 on ports 7001..7003
scripts/cluster.sh status           # cluster view from node1 (JSON)
scripts/cluster.sh kill 2           # hard-kill node2
scripts/cluster.sh restart 2        # start it again
scripts/cluster.sh join 4           # add a brand-new node4; it streams its share of the data
scripts/cluster.sh cleanup          # drop data the old owners gave away after a join
scripts/cluster.sh decommission 2   # node2 hands its data to the others and stops
scripts/cluster.sh removenode node3 # node3 is dead for good: copy its data from the survivors
scripts/cluster.sh repair           # anti-entropy repair on every node
scripts/cluster.sh stop             # stop everything (memtables are flushed)
scripts/cluster.sh clean            # stop and delete data/ and logs/
```

A walkthrough using the shell:

```bash
# 1. A node fails. Writes keep working at quorum and are queued for it.
scripts/cluster.sh kill 2
cassa-cli -e 'db.users.insert({_id: "zed", name: "Zed"})'
cassa-cli -e 'show cluster'          # a few seconds later: node2 DOWN, "pending hints 1"

# 2. It comes back. The queued writes are delivered within a few seconds.
scripts/cluster.sh restart 2
cassa-cli -e 'show cluster'          # node2 UP, "pending hints 0"

# 3. Make sure all replicas agree (e.g. after a long outage). Run on every node.
scripts/cluster.sh repair
#   node1: ranges=16 in_sync=16 repaired=0 skipped=0 streamed=0 31ms errors=[]
#   ...

# 4. Grow the cluster. node4 starts as JOINING, streams its ranges, then turns NORMAL.
scripts/cluster.sh join 4
cassa-cli -e 'show cluster'          # node4 JOINING ... then NORMAL
scripts/cluster.sh cleanup           # free the space the old owners no longer need

# 5. Shrink it again.
scripts/cluster.sh decommission 4    # streams node4's data away, marks it LEFT, stops it
```

Maintenance on a single node from the shell:

```text
cassa 127.0.0.1:7001> repair
repair on node1: 16 ranges, 16 in sync, 0 repaired, 0 skipped, 0 entries streamed (31 ms)
cassa 127.0.0.1:7001> compact major
compacted 3 sstables; 1 now on disk (52 ms)
cassa 127.0.0.1:7001> cleanup
cleanup dropped 0 entries (12 ms)
```

## HTTP API

The shell is a thin client over this API; you can use it directly. Document endpoints accept
`?consistency=one|quorum|all` (default `quorum`).

| Method & path | Purpose |
|---|---|
| `POST /collections/{c}/docs` | Insert (generates `_id` if absent) → `{"_id": ...}` |
| `GET /collections/{c}/docs/{id}` | Get a document |
| `PUT /collections/{c}/docs/{id}` | Insert or replace |
| `PATCH /collections/{c}/docs/{id}` | Apply update operators, returns the new document |
| `DELETE /collections/{c}/docs/{id}` | Delete |
| `POST /collections/{c}/find` | `{"filter", "sort", "skip", "limit", "explain"}` → `{"docs", "matched", "partial"}` |
| `POST /collections/{c}/indexes` | `{"field": "..."}`: create an index |
| `GET /collections/{c}/indexes` | Indexes and their build state on this node |
| `DELETE /collections/{c}/indexes/{field}` | Drop an index |
| `GET /cluster` | Members, UP/DOWN, state, ownership, pending hints |
| `GET /cluster/owners/{c}/{id}` | Replicas for a document |
| `GET /collections` | Local document counts and storage stats |
| `POST /admin/repair`, `GET /admin/repair` | Run a repair / last report |
| `POST /admin/compact[?major=true]` | Flush and compact |
| `POST /admin/cleanup` | Drop data this node no longer owns |
| `POST /admin/decommission` | Leave the cluster, handing data over |
| `POST /admin/removenode/{id}` | Remove a dead node |

```bash
curl -XPOST localhost:7001/collections/users/docs -H 'content-type: application/json' \
     -d '{"name":"ann","age":31,"addr":{"city":"Leeds"}}'
# {"_id":"6f1c..."}

curl 'localhost:7002/collections/users/docs/ann?consistency=one'

curl -XPATCH localhost:7001/collections/users/docs/ann -H 'content-type: application/json' \
     -d '{"$inc":{"age":1},"$set":{"addr.zip":"LS1"}}'

curl -XPOST localhost:7003/collections/users/find -H 'content-type: application/json' -d '{
  "filter": {"addr.city": "Leeds", "age": {"$gte": 30}},
  "sort": {"age": -1}, "skip": 0, "limit": 10, "explain": true
}'
# {"docs":[...],"matched":1,"partial":false,"explain":{"node1":{"index":null,"examined":4,"matched":1},...}}

curl -XPOST localhost:7001/collections/users/indexes -H 'content-type: application/json' -d '{"field":"addr.city"}'
curl -XDELETE localhost:7001/collections/users/docs/ann
```

Errors come back as `{"error": "..."}` with `400` bad request, `404` not found, `409` conflict
(e.g. a repair is already running), `503` not enough live replicas, or `504` replicas didn't
acknowledge in time.

## Running the server

```bash
cargo run --release -- --node-id n1 --listen 127.0.0.1:7001 --seeds 127.0.0.1:7001 --no-bootstrap
cargo run --release -- --node-id n2 --listen 127.0.0.1:7002 --seeds 127.0.0.1:7001
```

The first node of a new cluster can start with `--no-bootstrap`. Every other node joins by
streaming its share of the data from the existing nodes.

| Flag | Default | Meaning |
|---|---|---|
| `--node-id` | required | Unique, stable id; determines the node's ring tokens |
| `--listen` | `127.0.0.1:7001` | Address for client and node-to-node traffic |
| `--advertise` | `--listen` | Address other nodes use to reach this one |
| `--seeds` | | Comma-separated seed addresses |
| `--data-dir` | `./data/<node-id>` | Where commit logs, SSTables and the schema live |
| `--replication-factor` | `3` | Copies of each document; must match on every node |
| `--vnodes` | `16` | Tokens per node |
| `--gossip-interval-ms` | `1000` | How often nodes gossip |
| `--failure-timeout-ms` | `5000` | A node is DOWN after this long without a heartbeat |
| `--memtable-mb` | `16` | Flush the memtable to an SSTable at about this size |
| `--compaction-threshold` | `4` | Merge SSTables once this many have similar size |
| `--gc-grace-secs` | `864000` | Keep tombstones this long (10 days) |
| `--no-bootstrap` | off | Join without streaming data |
| `--ring-delay-ms` | `5000` | Wait for a status change to spread before streaming |
| `--repair-interval-secs` | `0` (off) | Run repair automatically every N seconds |
| `--fsync` | off | fsync the commit log after every write batch |

## How it works

```
src/
  main.rs         server entry point and flags
  gossip.rs       membership: push/pull state exchange each interval, (generation, heartbeat)
                  versioning, timeout-based failure detector, current + future rings
  ring.rs         token ring (FNV-1a + splitmix64), vnodes, replica placement, ownership
  coordinator.rs  replicated write/read with consistency levels, hinted handoff,
                  read repair, two-phase scatter/gather find
  index.rs        index definitions: gossiped schema, persistence, background build/drop
  movement.rs     bootstrap / decommission / removenode / cleanup, range streaming
  repair.rs       Merkle-tree anti-entropy repair (validation, tree diff, streaming), scheduler
  storage/
    mod.rs        LSM engine: memtable, flush/compaction worker, reads & scans, index upkeep
    commitlog.rs  segmented write-ahead log with CRC-framed records
    sstable.rs    SSTable format: 4 KiB blocks, sparse index, bloom filter, CRCs
    merge.rs      k-way merge with last-write-wins reconciliation
    secondary.rs  index key encoding (order-preserving) and query planner
  query.rs        filter matching, validation, sorting, update operators
  api.rs          axum HTTP API (public + /internal/* node-to-node)
  bin/cassa-cli/  the shell: statement parser, HTTP client, output formatting
client/           cassa-client, the async Rust driver (its own crate in the workspace)
tests/driver.rs   driver integration tests against a real server process
```

**Write path:** the coordinator timestamps the mutation, finds the `RF` replicas on the ring,
fails with `503` if fewer than the consistency level are alive, sends to all live replicas in
parallel, and returns once enough have acknowledged. Down or failed replicas get a *hint*, which
is replayed when gossip sees the node again.

**Read path:** asks the live replicas, waits for enough responses to satisfy the consistency
level, returns the newest version (LWW), then repairs stale replicas in the background.

**Find:** phase 1 asks every live node for ids whose local version matches (using an index if
one fits). Phase 2 fetches every node's version of those ids, resolves them by LWW, and runs the
filter again. That way a stale replica can't return a document that was since updated or deleted.

### On-disk storage

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
- **Writes never read** (except for indexed collections, see below). The newest version of a
  key is chosen at read time by timestamp across the memtable, memtables being flushed, and
  SSTables. Point reads skip an SSTable if its bloom filter rules the key out, or if its max
  timestamp is older than the best version already found.
- **Scans** (`find`) stream a k-way merge over all sources, so a node's data doesn't have to
  fit in RAM. Only memtables, indexes and bloom filters stay in memory.
- **Compaction** merges similar-sized SSTables, keeping the newest version of each key.
  Tombstones older than `--gc-grace-secs` are purged, but only if no SSTable outside the
  compaction and no memtable might still hold an older value for that key. Otherwise a
  deleted document could come back.
- **Graceful shutdown** (SIGINT) flushes the memtable. After a crash, the commit log is
  replayed on startup.

### Anti-entropy repair

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

### Adding and removing nodes

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

### Secondary indexes

Indexes are **local**, as in Cassandra: each node indexes only the documents it stores. `find`
still asks every node, but each node does an index lookup instead of scanning the collection.

- **Storage.** Index entries live in the same LSM tree as the documents, under their own key
  prefix (`\x01 c \0 field \0 value \0 id`). Values are encoded so their byte order matches
  their value order within a type (numbers via an order-preserving f64 transform, strings
  escaped), so equality, `$in` and `$gt/$gte/$lt/$lte` become key-range scans. Fields holding
  arrays get one entry per element, like MongoDB's multikey indexes.
- **Maintenance is exact.** Every local write to an indexed collection goes through
  `apply_batch`, whether it comes from a client, a queued write, repair or a node join. There
  the node reads the document's current winning version. If the incoming write wins, it writes
  tombstones for the old version's index entries and live entries for the new one, all at the
  new version's timestamp. A late, older write leaves the index alone. A lock keeps two writes
  to the same document from interleaving.
- **Verified.** Every document an index lookup turns up is re-checked against the full filter,
  so an index can narrow the search but never change the answer.
- **Planner.** It looks at top-level conditions and those inside a top-level `$and`. It prefers
  equality, then `$in`, then ranges. Null equality, `$ne`, `$exists`, `$or` and conditions on
  unindexed fields fall back to a scan. The choice is by rank, not by estimated cost.
- **Schema.** Index definitions travel in gossip, merged by last-write-wins, and are saved in
  `schema.json`. A node starts updating a new index on every write right away, backfills
  existing documents in the background, and only uses the index once that finishes. A node
  that was down when an index was created builds it after it comes back. A dropped index stops
  being used at once, and its entries are removed by a compaction.
- **Local only.** Index entries never appear in repair trees, streams or stats; each node
  rebuilds its own. Cleanup removes index entries for documents a node no longer owns.

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
- `find` still asks every node (indexes are local, not global), and sorting and limits
  happen on the coordinating node after all matches are gathered. There are no compound
  indexes and no index-backed sorting.
- `update` is read-modify-write and not atomic under concurrent updaters (LWW);
  `updateMany`/`deleteMany` are client-side loops.
- Inserts are upserts, as in Cassandra. A duplicate `_id` overwrites.
- Inter-node and client traffic is plain HTTP/JSON with no auth or TLS.

## Development

```bash
cargo test --workspace      # unit tests, driver unit tests + doc tests, and driver integration
                            # tests that start a real server
cargo clippy --workspace --all-targets  # no warnings
scripts/e2e.sh              # end-to-end: 4-5 node cluster, failures, repair, joins/leaves,
                            # indexes and the shell (~40 s)
```
