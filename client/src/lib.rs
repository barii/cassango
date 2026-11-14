//! Async Rust driver for [cassango](https://github.com/), a distributed JSON document database
//! with Cassandra-style replication and MongoDB-style queries.
//!
//! ```no_run
//! use cassango_client::{Client, Consistency, Filter, Update};
//! use serde::{Deserialize, Serialize};
//! use serde_json::json;
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct User {
//!     #[serde(rename = "_id")]
//!     id: String,
//!     name: String,
//!     age: u32,
//!     city: String,
//! }
//!
//! # async fn demo() -> cassango_client::Result<()> {
//! let client = Client::builder()
//!     .hosts(["127.0.0.1:7001", "127.0.0.1:7002"]) // later hosts are fallbacks
//!     .consistency(Consistency::Quorum)
//!     .build()?;
//!
//! let users = client.collection::<User>("users");
//! users.insert(&User { id: "ann".into(), name: "Ann".into(), age: 31, city: "Leeds".into() }).await?;
//!
//! let ann: Option<User> = users.get("ann").await?;
//!
//! // Filters and updates are plain JSON (json!) or built with Filter / Update.
//! let found = users
//!     .find(Filter::new().eq("city", "Leeds").gte("age", 18))
//!     .sort(json!({"age": -1}))
//!     .limit(10)
//!     .await?;
//! for u in &found.docs {
//!     println!("{} ({})", u.name, u.age);
//! }
//!
//! users.update("ann", Update::new().inc("age", 1)).await?;
//! let adults = users.count(json!({"age": {"$gte": 18}})).await?;
//! users.delete("ann").await?;
//! # Ok(()) }
//! ```
//!
//! Every node can coordinate any request, so the driver just needs one reachable host. If a
//! host can't be reached, the request is retried on the next one, and that host is used from
//! then on. [`Client::refresh_hosts`] learns the full node list from the cluster.

mod collection;
mod error;
mod query;
mod types;

pub use collection::{Collection, Find, FindResult};
pub use error::{Error, Result};
pub use query::{Filter, Order, Update};
pub use types::{ClusterInfo, Consistency, IndexInfo, Member, NodeInfo, NodePlan, Replica};

use reqwest::Method;
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// A handle to a cassango cluster. Cheap to clone; clones share connections and host state.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

struct Inner {
    hosts: RwLock<Vec<String>>,
    current: AtomicUsize,
    http: reqwest::Client,
    consistency: Consistency,
}

/// Configures a [`Client`].
pub struct ClientBuilder {
    hosts: Vec<String>,
    consistency: Consistency,
    timeout: Duration,
    connect_timeout: Duration,
    http: Option<reqwest::Client>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        ClientBuilder {
            hosts: Vec::new(),
            consistency: Consistency::Quorum,
            timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(2),
            http: None,
        }
    }
}

fn normalize_host(h: &str) -> String {
    h.trim().trim_start_matches("http://").trim_end_matches('/').to_string()
}

impl ClientBuilder {
    /// Add a node address (`host:port`).
    pub fn host(mut self, host: impl AsRef<str>) -> Self {
        self.hosts.push(normalize_host(host.as_ref()));
        self
    }

    /// Add several node addresses; requests fail over through them in order.
    pub fn hosts<S: AsRef<str>>(mut self, hosts: impl IntoIterator<Item = S>) -> Self {
        self.hosts.extend(hosts.into_iter().map(|h| normalize_host(h.as_ref())));
        self
    }

    /// Default consistency level for document reads and writes (default `Quorum`).
    pub fn consistency(mut self, cl: Consistency) -> Self {
        self.consistency = cl;
        self
    }

    /// Overall request timeout (default 30 s).
    pub fn timeout(mut self, t: Duration) -> Self {
        self.timeout = t;
        self
    }

    /// TCP connect timeout, after which the next host is tried (default 2 s).
    pub fn connect_timeout(mut self, t: Duration) -> Self {
        self.connect_timeout = t;
        self
    }

    /// Use your own `reqwest::Client` (timeouts set on the builder are then ignored).
    pub fn http_client(mut self, http: reqwest::Client) -> Self {
        self.http = Some(http);
        self
    }

    pub fn build(self) -> Result<Client> {
        if self.hosts.is_empty() {
            return Err(Error::Config("at least one host is required".into()));
        }
        let http = match self.http {
            Some(h) => h,
            None => reqwest::Client::builder()
                .timeout(self.timeout)
                .connect_timeout(self.connect_timeout)
                .build()
                .map_err(|e| Error::Config(e.to_string()))?,
        };
        Ok(Client {
            inner: Arc::new(Inner {
                hosts: RwLock::new(self.hosts),
                current: AtomicUsize::new(0),
                http,
                consistency: self.consistency,
            }),
        })
    }
}

/// Percent-encode one URL path segment (ids and field names can be arbitrary strings).
pub(crate) fn enc(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for b in segment.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Connect to a single host with default settings.
    pub fn new(host: impl AsRef<str>) -> Result<Client> {
        Client::builder().host(host).build()
    }

    /// A collection of documents deserialized as `T`. Use `serde_json::Value` for
    /// schemaless access. Collections are created implicitly by the first write.
    pub fn collection<T>(&self, name: &str) -> Collection<T> {
        Collection::new(self.clone(), name, self.inner.consistency)
    }

    /// The host requests currently go to.
    pub fn current_host(&self) -> String {
        let hosts = self.inner.hosts.read().unwrap();
        hosts[self.inner.current.load(Ordering::Relaxed) % hosts.len()].clone()
    }

    /// All known hosts, in failover order.
    pub fn hosts(&self) -> Vec<String> {
        self.inner.hosts.read().unwrap().clone()
    }

    /// Basic information about the current node.
    pub async fn ping(&self) -> Result<NodeInfo> {
        Ok(serde_json::from_value(self.call(Method::GET, "/", None, None).await?)?)
    }

    /// Cluster membership as seen by the current node.
    pub async fn cluster(&self) -> Result<ClusterInfo> {
        Ok(serde_json::from_value(self.call(Method::GET, "/cluster", None, None).await?)?)
    }

    /// Replace the host list with every UP, NORMAL node the cluster reports (keeping the
    /// current host first). Useful when you only configured a seed or two.
    pub async fn refresh_hosts(&self) -> Result<Vec<String>> {
        let info = self.cluster().await?;
        let current = self.current_host();
        let mut hosts: Vec<String> = info
            .members
            .iter()
            .filter(|m| m.is_up() && m.state == "NORMAL")
            .map(|m| m.addr.clone())
            .collect();
        if hosts.is_empty() {
            return Ok(self.hosts());
        }
        hosts.sort_by_key(|h| *h != current);
        *self.inner.hosts.write().unwrap() = hosts.clone();
        self.inner.current.store(0, Ordering::Relaxed);
        Ok(hosts)
    }

    /// Send a request and return (status, body), failing over between hosts on
    /// connection errors. Other errors are returned without retrying: a request that
    /// timed out may still be running.
    pub(crate) async fn raw(&self, method: Method, path: &str, cl: Option<Consistency>, body: Option<&Value>) -> Result<(u16, Value)> {
        let n = self.inner.hosts.read().unwrap().len();
        let mut last = String::new();
        for _ in 0..n {
            let idx = self.inner.current.load(Ordering::Relaxed);
            let host = {
                let hosts = self.inner.hosts.read().unwrap();
                hosts[idx % hosts.len()].clone()
            };
            let mut url = format!("http://{host}{path}");
            if let Some(cl) = cl {
                url.push_str(if path.contains('?') { "&consistency=" } else { "?consistency=" });
                url.push_str(cl.as_str());
            }
            let mut req = self.inner.http.request(method.clone(), url);
            if let Some(b) = body {
                req = req.json(b);
            }
            match req.send().await {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let bytes = resp.bytes().await.map_err(|e| Error::Transport(format!("{host}: {e}")))?;
                    let body = if bytes.is_empty() {
                        Value::Null
                    } else {
                        serde_json::from_slice(&bytes).unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()))
                    };
                    return Ok((status, body));
                }
                Err(e) if e.is_connect() => {
                    last = format!("{host}: {e}");
                    // Move on to the next host (unless another request already did).
                    let _ = self.inner.current.compare_exchange(idx, idx + 1, Ordering::Relaxed, Ordering::Relaxed);
                }
                Err(e) => return Err(Error::Transport(format!("{host}: {e}"))),
            }
        }
        Err(Error::Transport(format!("no reachable host ({last})")))
    }

    /// Like `raw`, but non-success statuses become typed errors.
    pub(crate) async fn call(&self, method: Method, path: &str, cl: Option<Consistency>, body: Option<&Value>) -> Result<Value> {
        let (status, body) = self.raw(method, path, cl, body).await?;
        if (200..300).contains(&status) {
            Ok(body)
        } else {
            Err(error_from(status, &body))
        }
    }
}

pub(crate) fn error_from(status: u16, body: &Value) -> Error {
    let msg = body
        .get("error")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| body.to_string());
    Error::from_status(status, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_validates_and_normalizes() {
        assert!(matches!(Client::builder().build(), Err(Error::Config(_))));
        let c = Client::builder().hosts(["http://a:1/", " b:2 "]).build().unwrap();
        assert_eq!(c.hosts(), vec!["a:1", "b:2"]);
        assert_eq!(c.current_host(), "a:1");
    }

    #[test]
    fn encoding_and_errors() {
        assert_eq!(enc("a b/c?é"), "a%20b%2Fc%3F%C3%A9");
        assert!(matches!(error_from(503, &serde_json::json!({"error": "x"})), Error::Unavailable(m) if m == "x"));
        assert!(matches!(error_from(418, &Value::Null), Error::Server { status: 418, .. }));
        assert!(Error::Timeout(String::new()).is_retryable());
        assert!(!Error::BadRequest(String::new()).is_retryable());
    }
}
