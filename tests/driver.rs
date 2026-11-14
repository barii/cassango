//! Integration tests for the `cassango-client` driver against a real `cassango` server process.

use cassango_client::{Client, Consistency, Error, Filter, Order, Update};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// A single-node server in a temporary data directory, killed on drop.
struct Server {
    child: Child,
    addr: String,
    dir: PathBuf,
}

impl Server {
    async fn start() -> Server {
        let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let addr = format!("127.0.0.1:{port}");
        let dir = std::env::temp_dir().join(format!("cassango-driver-test-{port}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let child = Command::new(env!("CARGO_BIN_EXE_cassango"))
            .args(["--node-id", "t1", "--listen", &addr, "--seeds", &addr, "--no-bootstrap"])
            .args(["--replication-factor", "1", "--gossip-interval-ms", "200", "--ring-delay-ms", "200"])
            .arg("--data-dir")
            .arg(&dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start cassango");
        let server = Server { child, addr, dir };
        let client = Client::new(&server.addr).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while client.ping().await.is_err() {
            assert!(Instant::now() < deadline, "server did not start");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        server
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct User {
    #[serde(rename = "_id")]
    id: String,
    name: String,
    age: u32,
    city: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    tags: Vec<String>,
}

fn user(id: &str, age: u32, city: &str) -> User {
    User { id: id.into(), name: id.to_uppercase(), age, city: city.into(), tags: vec![] }
}

#[tokio::test]
async fn typed_crud_queries_and_indexes() {
    let server = Server::start().await;
    let client = Client::builder().host(&server.addr).consistency(Consistency::Quorum).build().unwrap();
    let users = client.collection::<User>("users");

    // insert / get / put
    assert_eq!(users.insert(&user("ann", 31, "Leeds")).await.unwrap(), "ann");
    let ids = users
        .insert_many(&[user("bob", 25, "York"), user("cat", 42, "Leeds"), user("dan", 17, "Hull")])
        .await
        .unwrap();
    assert_eq!(ids, vec!["bob", "cat", "dan"], "ids returned in input order");
    assert_eq!(users.get("ann").await.unwrap(), Some(user("ann", 31, "Leeds")));
    assert_eq!(users.get("nobody").await.unwrap(), None);
    users.put("eve", &user("eve", 58, "York")).await.unwrap();

    // find with json!, builders, sort, skip, limit
    let r = users.find(json!({"city": "Leeds"})).sort(json!({"age": -1})).await.unwrap();
    assert_eq!(r.docs.iter().map(|u| u.id.as_str()).collect::<Vec<_>>(), vec!["cat", "ann"]);
    assert_eq!((r.matched, r.partial), (2, false));

    let r = users.find(Filter::new().gte("age", 18).lt("age", 50)).sort_by("age", Order::Asc).limit(2).await.unwrap();
    assert_eq!(r.docs.iter().map(|u| u.age).collect::<Vec<_>>(), vec![25, 31]);
    assert_eq!(r.matched, 3, "matched counts before limit");

    let page2: Vec<String> = users.find_all().sort(json!({"_id": 1})).skip(2).limit(2).await.unwrap().into_iter().map(|u| u.id).collect();
    assert_eq!(page2, vec!["cat", "dan"]);

    let or = Filter::new().or([Filter::new().lt("age", 18), Filter::new().eq("city", "York")]);
    assert_eq!(users.count(or).await.unwrap(), 3);
    assert_eq!(users.find_one(json!({"city": "Hull"})).await.unwrap().map(|u| u.id), Some("dan".into()));
    assert_eq!(users.find(json!({"age": {"$gt": 100}})).one().await.unwrap(), None);

    // update returns the new document; None for a missing id
    let bob = users.update("bob", Update::new().inc("age", 1).push("tags", "moved")).await.unwrap().unwrap();
    assert_eq!((bob.age, bob.tags.clone()), (26, vec!["moved".to_string()]));
    assert_eq!(users.update("nobody", Update::new().set("x", 1)).await.unwrap(), None);

    // many
    assert_eq!(users.update_many(json!({"city": "York"}), Update::new().set("city", "Leeds")).await.unwrap(), 2);
    assert_eq!(users.count(json!({"city": "Leeds"})).await.unwrap(), 4);
    assert_eq!(users.delete_many(Filter::new().lt("age", 18)).await.unwrap(), 1);
    users.delete("eve").await.unwrap();
    users.delete("eve").await.unwrap(); // idempotent
    assert_eq!(users.count(json!({})).await.unwrap(), 3);

    // indexes + explain
    assert!(users.create_index("age").await.unwrap());
    assert!(!users.create_index("age").await.unwrap(), "already exists");
    users.wait_for_index("age", Duration::from_secs(10)).await.unwrap();
    assert!(users.indexes().await.unwrap().iter().any(|i| i.field == "age" && i.is_ready()));
    let r = users.find(Filter::new().gt("age", 30)).explain().await.unwrap();
    let plan = r.explain.unwrap().into_values().next().unwrap();
    assert_eq!((plan.index.as_deref(), plan.examined, plan.matched), (Some("age"), 2, 2));
    assert!(users.drop_index("age").await.unwrap());
    assert!(!users.drop_index("age").await.unwrap());

    // cluster info and owners
    let info = client.cluster().await.unwrap();
    assert_eq!((info.replication_factor, info.members.len()), (1, 1));
    assert!(info.members[0].is_up());
    assert_eq!(users.owners("ann").await.unwrap()[0].node_id, "t1");
    assert_eq!(client.ping().await.unwrap().state, "NORMAL");
}

/// Every driver future must be `Send`, so services can run them in `tokio::spawn`.
#[tokio::test]
async fn futures_can_be_spawned() {
    let server = Server::start().await;
    let users = Client::new(&server.addr).unwrap().collection::<User>("spawned");
    let ids = tokio::spawn({
        let users = users.clone();
        async move { users.insert_many(&[user("a", 30, "Leeds"), user("b", 40, "York")]).await }
    });
    assert_eq!(ids.await.unwrap().unwrap(), vec!["a", "b"]);
    let updated = tokio::spawn({
        let users = users.clone();
        async move { users.update_many(json!({}), Update::new().inc("age", 1)).await }
    });
    assert_eq!(updated.await.unwrap().unwrap(), 2);
    let found = tokio::spawn({
        let users = users.clone();
        async move { users.find(json!({"age": {"$gt": 35}})).await }
    });
    assert_eq!(found.await.unwrap().unwrap().matched, 1);
    let deleted = tokio::spawn({
        let users = users.clone();
        async move { users.delete_many(json!({})).await }
    });
    assert_eq!(deleted.await.unwrap().unwrap(), 2);
    let waited = tokio::spawn({
        let users = users.clone();
        async move {
            users.create_index("age").await?;
            users.wait_for_index("age", Duration::from_secs(10)).await
        }
    });
    waited.await.unwrap().unwrap();
}

#[tokio::test]
async fn schemaless_errors_and_failover() {
    let server = Server::start().await;
    // The first host refuses connections; the driver must fail over to the second.
    let dead = format!("127.0.0.1:{}", TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port());
    let client = Client::builder().hosts([dead.as_str(), server.addr.as_str()]).build().unwrap();
    let notes = client.collection::<Value>("notes");

    let id = notes.insert(&json!({"text": "hello", "n": 1})).await.unwrap();
    assert_eq!(id.len(), 36, "server-generated uuid");
    assert_eq!(client.current_host(), server.addr, "sticks to the working host");
    let doc = notes.get(&id).await.unwrap().unwrap();
    assert_eq!((doc["_id"].as_str(), doc["text"].as_str()), (Some(id.as_str()), Some("hello")));

    // Ids with characters that need URL encoding.
    let odd = "a b/c?d#é";
    notes.put(odd, &json!({"x": 1})).await.unwrap();
    assert_eq!(notes.get(odd).await.unwrap().unwrap()["_id"], json!(odd));

    // Typed errors.
    let err = notes.find(json!({"n": {"$bogus": 1}})).await.unwrap_err();
    assert!(matches!(err, Error::BadRequest(ref m) if m.contains("$bogus")), "{err}");
    let err = notes.update(&id, json!({"$set": {"_id": "x"}})).await.unwrap_err();
    assert!(matches!(err, Error::BadRequest(_)), "{err}");
    let err = client.collection::<Value>("bad name!").insert(&json!({})).await.unwrap_err();
    assert!(matches!(err, Error::BadRequest(_)), "{err}");

    // A document that doesn't fit the requested type is a serde error, not a panic.
    #[derive(Debug, Deserialize, Serialize)]
    struct Strict {
        text: u64,
    }
    let err = notes.typed::<Strict>().get(&id).await.unwrap_err();
    assert!(matches!(err, Error::Serde(_)), "{err}");

    // refresh_hosts learns the real node list (drops the dead host).
    assert_eq!(client.refresh_hosts().await.unwrap(), vec![server.addr.clone()]);

    // No reachable host at all.
    let nobody = Client::new(&dead).unwrap();
    assert!(matches!(nobody.ping().await.unwrap_err(), Error::Transport(_)));
}
