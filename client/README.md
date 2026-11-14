# cassango-client

Async Rust driver for [cassango](../README.md), the distributed JSON document database.

```toml
[dependencies]
cassango-client = { path = "path/to/cassa/client" }        # or: git = "https://…/cassango", package = "cassango-client"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The driver runs on tokio (through reqwest). It depends on reqwest, serde, serde_json and
futures, and on nothing from the server.

## Connecting

```rust
use cassango_client::{Client, Consistency};
use std::time::Duration;

let client = Client::new("127.0.0.1:7001")?;                  // one host, defaults

let client = Client::builder()
    .hosts(["10.0.0.5:7001", "10.0.0.6:7001", "10.0.0.7:7001"]) // tried in order if one is down
    .consistency(Consistency::Quorum)                          // default for reads and writes
    .timeout(Duration::from_secs(10))                          // per request (default 30 s)
    .connect_timeout(Duration::from_secs(1))                   // before trying the next host
    .build()?;

client.refresh_hosts().await?;    // replace the list with every UP node the cluster reports
```

`Client` is cheap to clone and safe to share across tasks; clones share connections. Any node
can serve any request. If one can't be reached, the request moves on to the next host, and that
host is used from then on.

## Collections and documents

Collections are created by the first write. Pick a document type with the turbofish: your own
serde struct, or `serde_json::Value` for schemaless documents.

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct User {
    #[serde(rename = "_id")]   // map a field to the document id
    id: String,
    name: String,
    age: u32,
    #[serde(default)]
    tags: Vec<String>,
}

let users = client.collection::<User>("users");
let events = client.collection::<serde_json::Value>("events");
```

| Method | Returns | Notes |
|---|---|---|
| `insert(&doc)` | `String` (the `_id`) | Upsert. `_id` is generated when the document has none |
| `insert_many(&[docs])` | `Vec<String>` | Up to 16 at a time, ids in input order |
| `put(id, &doc)` | `()` | Insert or replace by id |
| `get(id)` | `Option<T>` | `None` if missing or deleted |
| `update(id, update)` | `Option<T>` | Returns the updated document; `None` if missing |
| `delete(id)` | `()` | Deleting a missing document is fine |
| `find(filter)` / `find_all()` | `Find<T>` builder | `.sort() .sort_by() .skip() .limit() .explain()`, then `.await` |
| `find_one(filter)` | `Option<T>` | |
| `count(filter)` | `usize` | Cluster-wide |
| `update_many(filter, update)` | `usize` | Finds ids, then updates each (not atomic) |
| `delete_many(filter)` | `usize` | Same |
| `create_index(field)` / `drop_index(field)` | `bool` | `true` if created / dropped |
| `indexes()` | `Vec<IndexInfo>` | Build state on the current node |
| `wait_for_index(field, timeout)` | `()` | Until ready on every UP node |
| `owners(id)` | `Vec<Replica>` | Nodes storing the document |
| `with_consistency(cl)` | `Collection<T>` | Same collection, other consistency level |
| `typed::<U>()` | `Collection<U>` | Same collection, other document type |

## Examples

### Insert and read

```rust
let id = users.insert(&User { id: "ann".into(), name: "Ann".into(), age: 31, tags: vec![] }).await?;

let ids = users.insert_many(&[bob, cat, dan]).await?;

let ann: Option<User> = users.get("ann").await?;

let id = events.insert(&json!({"type": "signup", "user": "ann"})).await?;   // generated uuid
```

### Queries

Filters are MongoDB syntax. Write them with `serde_json::json!`:

```rust
use serde_json::json;

let r = users
    .find(json!({"age": {"$gte": 18, "$lt": 65}, "tags": "beta"}))
    .sort(json!({"age": -1, "name": 1}))
    .skip(20)
    .limit(10)
    .await?;

println!("{} of {} matches", r.docs.len(), r.matched);
for u in r.docs { /* ... */ }        // or: for u in r { ... }
if r.partial { /* some nodes were unreachable; results may be incomplete */ }
```

or with the `Filter` builder:

```rust
use cassango_client::{Filter, Order};

let leeds_adults = users
    .find(Filter::new().eq("city", "Leeds").gte("age", 18))
    .sort_by("age", Order::Desc)
    .sort_by("name", Order::Asc)
    .await?;

let f = Filter::new()
    .is_in("city", ["York", "Hull"])
    .exists("email", true)
    .or([Filter::new().lt("age", 18), Filter::new().eq("vip", true)]);
let n = users.count(f).await?;

let first = users.find_one(json!({"addr.zip": "LS1"})).await?;
let all = users.find_all().limit(100).await?;
```

`Filter` methods: `eq ne gt gte lt lte is_in not_in exists size or nor`. Several operators on
the same field are merged, so `.gte("age", 18).lt("age", 65)` becomes
`{"age": {"$gte": 18, "$lt": 65}}`.

### Updates and deletes

```rust
use cassango_client::Update;

let bob = users.update("bob", Update::new()
        .set("addr.city", "Leeds")      // dotted paths create nested objects
        .inc("visits", 1)
        .push("tags", "moved")
        .unset("nickname"))
    .await?;                            // Some(updated) or None if "bob" doesn't exist

users.update("bob", json!({"$inc": {"age": 1}})).await?;   // plain JSON works too

let n = users.update_many(json!({"city": "York"}), Update::new().set("region", "north")).await?;
users.put("eve", &eve).await?;          // replace the whole document
users.delete("eve").await?;
let n = users.delete_many(Filter::new().lt("age", 18)).await?;
```

### Indexes and explain

```rust
users.create_index("age").await?;
users.wait_for_index("age", Duration::from_secs(30)).await?;

let r = users.find(Filter::new().gt("age", 30)).explain().await?;
for (node, plan) in r.explain.unwrap() {
    println!("{node}: {:?} examined {} matched {}", plan.index, plan.examined, plan.matched);
}
// node1: Some("age") examined 2 matched 2

users.drop_index("age").await?;
```

### Consistency

```rust
let strict = users.with_consistency(Consistency::All);   // every replica must answer
let fast = users.with_consistency(Consistency::One);     // any single replica

match strict.get("ann").await {
    Err(Error::Unavailable(_)) => fast.get("ann").await?,  // a replica is down: degrade
    other => other?,
};
```

### Errors

All methods return `cassango_client::Result<T>`, an alias for `Result<T, cassango_client::Error>`:

| Variant | When |
|---|---|
| `BadRequest` | Invalid filter, update, document or collection name (400) |
| `NotFound` | Missing index/node on admin calls (404). Documents return `None` instead |
| `Conflict` | Something conflicting is already in progress (409) |
| `Unavailable` | Too few replicas alive for the consistency level; nothing was written (503) |
| `Timeout` | Replicas didn't acknowledge in time; the write may have been applied partially (504) |
| `Transport` | No host reachable, or the connection broke |
| `Serde` | A document didn't match your type |
| `Partial` | `update_many`/`delete_many` refused because some nodes were unreachable |

`err.is_retryable()` is true for `Unavailable`, `Timeout`, `Transport` and `Partial`.

### Cluster information

```rust
let info = client.cluster().await?;
for m in &info.members {
    println!("{} {} {} {:?}%", m.node_id, m.status, m.state, m.primary_ownership_pct);
}
let node = client.ping().await?;   // node_id, version, state, replication_factor
```

## A full program

[`examples/quickstart.rs`](examples/quickstart.rs) covers all of the above against a running cluster:

```bash
scripts/cluster.sh start 3
cargo run -p cassango-client --example quickstart
```

## Notes

- Enabling this crate turns on serde_json's `preserve_order` feature (Cargo features are
  shared across a build), so `serde_json::Map` keeps insertion order in your project too. The
  driver needs it so that `json!({"a": 1, "b": -1})` sort specs keep the order you wrote.
- `update` is read-modify-write on the server, and concurrent updates to one document resolve
  by last write wins. `update_many` and `delete_many` loop over the matches on the client, up
  to 16 at a time.
- The driver doesn't retry on its own beyond failing over to the next host when a connection
  fails. Use `Error::is_retryable` to decide whether to retry.
