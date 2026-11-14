//! The coordinator: any node can receive a client request and acts as coordinator
//! for it, fanning reads/writes out to the replicas that own the key and waiting
//! for the number of acknowledgements the consistency level demands.
//!
//! Also implements:
//!   * hinted handoff: writes for down/failed replicas are queued and replayed
//!     once gossip reports the replica alive again (in-memory; lost on restart),
//!   * read repair: after a read, stale replicas are sent the newest version,
//!   * scatter/gather `find` across the whole cluster.

use crate::gossip::Gossiper;
use crate::model::*;
use crate::query;
use crate::ring::key_token;
use crate::storage::{blocking, Storage};
use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

const MAX_HINTS_PER_NODE: usize = 100_000;

pub struct Node {
    pub id: String,
    pub rf: usize,
    pub gossip: Arc<Gossiper>,
    pub storage: Arc<Storage>,
    hints: Mutex<HashMap<String, Vec<Mutation>>>,
    client: reqwest::Client,
    clock: AtomicU64,
    pub(crate) repair_lock: tokio::sync::Mutex<()>,
    pub(crate) last_repair: Mutex<Option<crate::repair::RepairReport>>,
}

#[derive(Deserialize, Default)]
pub struct FindRequest {
    #[serde(default)]
    pub filter: Value,
    pub sort: Option<Map<String, Value>>,
    pub skip: Option<usize>,
    pub limit: Option<usize>,
}

#[derive(Serialize)]
pub struct FindResult {
    pub docs: Vec<Value>,
    /// Total matches before skip/limit.
    pub matched: usize,
    /// True if some nodes were down or failed, so results may be incomplete.
    pub partial: bool,
}

impl Node {
    pub fn new(id: String, rf: usize, gossip: Arc<Gossiper>, storage: Arc<Storage>, client: reqwest::Client) -> Arc<Self> {
        Arc::new(Node {
            id,
            rf: rf.max(1),
            gossip,
            storage,
            hints: Mutex::new(HashMap::new()),
            client,
            clock: AtomicU64::new(0),
            repair_lock: tokio::sync::Mutex::new(()),
            last_repair: Mutex::new(None),
        })
    }

    /// Monotonic microsecond timestamps, so two writes through the same
    /// coordinator never collide.
    fn next_ts(&self) -> u64 {
        let now = now_micros();
        let prev = self
            .clock
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |last| Some(now.max(last + 1)))
            .unwrap();
        now.max(prev + 1)
    }

    pub fn replicas_for(&self, c: &str, k: &str) -> Vec<String> {
        self.gossip.ring().replicas(key_token(c, k), self.rf)
    }

    // ------------------------------------------------------------ RPC plumbing

    pub(crate) async fn rpc<B: Serialize, R: DeserializeOwned>(&self, node: &str, path: &str, body: &B) -> Result<R, String> {
        let addr = self.gossip.addr_of(node).ok_or_else(|| format!("unknown node {node}"))?;
        let resp = self
            .client
            .post(format!("http://{addr}{path}"))
            .json(body)
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(|e| e.to_string())?;
        resp.json::<R>().await.map_err(|e| e.to_string())
    }

    pub(crate) async fn send_mutations(&self, node: &str, muts: Vec<Mutation>) -> Result<(), String> {
        if node == self.id {
            let st = self.storage.clone();
            blocking(move || st.apply_batch(muts)).await.map(|_| ())
        } else {
            self.rpc::<_, Value>(node, "/internal/apply", &muts).await.map(|_| ())
        }
    }

    async fn fetch_record(&self, node: &str, c: &str, k: &str) -> Result<Option<Record>, String> {
        if node == self.id {
            let (st, c, k) = (self.storage.clone(), c.to_string(), k.to_string());
            blocking(move || st.get(&c, &k)).await
        } else {
            self.rpc(node, "/internal/get", &GetReq { c: c.into(), k: k.into() }).await
        }
    }

    async fn scan_ids(&self, node: &str, c: &str, filter: &Value) -> Result<Vec<String>, String> {
        if node == self.id {
            let (st, c, f) = (self.storage.clone(), c.to_string(), filter.clone());
            blocking(move || st.scan_ids(&c, &f)).await
        } else {
            self.rpc(node, "/internal/scan", &ScanReq { c: c.into(), filter: filter.clone() }).await
        }
    }

    async fn multiget(&self, node: &str, c: &str, ids: &[String]) -> Result<Vec<(String, Record)>, String> {
        if node == self.id {
            let (st, c, ids) = (self.storage.clone(), c.to_string(), ids.to_vec());
            blocking(move || st.multiget(&c, &ids)).await
        } else {
            self.rpc(node, "/internal/multiget", &MultigetReq { c: c.into(), ids: ids.to_vec() }).await
        }
    }

    // ------------------------------------------------------------ hinted handoff

    fn add_hint(&self, node: &str, m: Mutation) {
        let mut hints = self.hints.lock();
        let q = hints.entry(node.to_string()).or_default();
        if q.len() < MAX_HINTS_PER_NODE {
            q.push(m);
        } else {
            warn!(%node, "hint queue full; dropping hint (repair needed)");
        }
    }

    pub fn hint_counts(&self) -> BTreeMap<String, usize> {
        self.hints.lock().iter().map(|(k, v)| (k.clone(), v.len())).filter(|(_, n)| *n > 0).collect()
    }

    pub fn spawn_hint_replayer(self: &Arc<Self>) {
        let me = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(2));
            loop {
                tick.tick().await;
                let ready: Vec<String> = me
                    .hint_counts()
                    .into_keys()
                    .filter(|n| me.gossip.is_alive(n))
                    .collect();
                for node in ready {
                    let batch = me.hints.lock().remove(&node).unwrap_or_default();
                    let n = batch.len();
                    match me.send_mutations(&node, batch.clone()).await {
                        Ok(()) => info!(%node, hints = n, "replayed hints"),
                        Err(e) => {
                            debug!(%node, "hint replay failed: {e}");
                            me.hints.lock().entry(node).or_default().extend(batch);
                        }
                    }
                }
            }
        });
    }

    // ------------------------------------------------------------ writes

    /// Write (or delete, with `value = None`) a document.
    pub async fn write(self: &Arc<Self>, c: &str, k: &str, value: Option<Value>, cl: Consistency) -> Result<(), DbError> {
        let m = Mutation { c: c.into(), k: k.into(), r: Record { ts: self.next_ts(), value } };
        self.replicate(m, cl).await
    }

    async fn replicate(self: &Arc<Self>, m: Mutation, cl: Consistency) -> Result<(), DbError> {
        let replicas = self.replicas_for(&m.c, &m.k);
        if replicas.is_empty() {
            return Err(DbError::Internal("ring is empty".into()));
        }
        let required = cl.required(replicas.len());
        let (alive, down): (Vec<String>, Vec<String>) =
            replicas.into_iter().partition(|n| self.gossip.is_alive(n));
        if alive.len() < required {
            return Err(DbError::Unavailable { required, alive: alive.len() });
        }
        for n in &down {
            self.add_hint(n, m.clone());
        }

        // Each replica write runs in its own task so it completes (or gets hinted)
        // even after we've returned to the client.
        let (tx, mut rx) = mpsc::channel(alive.len());
        for n in alive {
            let (me, m, tx) = (self.clone(), m.clone(), tx.clone());
            tokio::spawn(async move {
                let ok = match me.send_mutations(&n, vec![m.clone()]).await {
                    Ok(()) => true,
                    Err(e) => {
                        warn!(node = %n, "replica write failed, storing hint: {e}");
                        me.add_hint(&n, m);
                        false
                    }
                };
                let _ = tx.send(ok).await;
            });
        }
        drop(tx);

        let mut acked = 0;
        while let Some(ok) = rx.recv().await {
            acked += ok as usize;
            if acked >= required {
                return Ok(());
            }
        }
        Err(DbError::Timeout { required, acked })
    }

    // ------------------------------------------------------------ reads

    /// Read the newest version of a document from `cl` replicas, then repair stale replicas
    /// in the background. Returns `None` if missing or deleted.
    pub async fn read(self: &Arc<Self>, c: &str, k: &str, cl: Consistency) -> Result<Option<Value>, DbError> {
        let replicas = self.replicas_for(c, k);
        if replicas.is_empty() {
            return Err(DbError::Internal("ring is empty".into()));
        }
        let required = cl.required(replicas.len());
        let alive: Vec<String> = replicas.into_iter().filter(|n| self.gossip.is_alive(n)).collect();
        if alive.len() < required {
            return Err(DbError::Unavailable { required, alive: alive.len() });
        }

        let (tx, mut rx) = mpsc::channel(alive.len());
        for n in alive {
            let (me, tx, c, k) = (self.clone(), tx.clone(), c.to_string(), k.to_string());
            tokio::spawn(async move {
                let r = me.fetch_record(&n, &c, &k).await;
                let _ = tx.send((n, r)).await;
            });
        }
        drop(tx);

        let mut got: Vec<(String, Option<Record>)> = Vec::new();
        while got.len() < required {
            match rx.recv().await {
                Some((n, Ok(r))) => got.push((n, r)),
                Some((n, Err(e))) => debug!(node = %n, "replica read failed: {e}"),
                None => break,
            }
        }
        if got.len() < required {
            return Err(DbError::Timeout { required, acked: got.len() });
        }
        let result = newest(got.iter().map(|(_, r)| r));

        // Background read repair, using every replica's answer.
        let (me, c, k) = (self.clone(), c.to_string(), k.to_string());
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if let (n, Ok(r)) = msg {
                    got.push((n, r));
                }
            }
            let Some(latest) = newest(got.iter().map(|(_, r)| r)) else { return };
            for (n, r) in got {
                if r.as_ref().is_none_or(|r| latest.supersedes(r)) {
                    debug!(node = %n, coll = %c, key = %k, "read repair");
                    let m = Mutation { c: c.clone(), k: k.clone(), r: latest.clone() };
                    if let Err(e) = me.send_mutations(&n, vec![m]).await {
                        debug!(node = %n, "read repair failed: {e}");
                    }
                }
            }
        });

        Ok(result.and_then(|r| r.value))
    }

    /// Read-modify-write with Mongo update operators. Not atomic across
    /// concurrent updaters: last writer wins, as in Cassandra.
    pub async fn update(self: &Arc<Self>, c: &str, k: &str, update: &Value, cl: Consistency) -> Result<Value, DbError> {
        let mut doc = self.read(c, k, cl).await?.ok_or(DbError::NotFound)?;
        query::apply_update(&mut doc, update).map_err(DbError::BadRequest)?;
        self.write(c, k, Some(doc.clone()), cl).await?;
        Ok(doc)
    }

    // ------------------------------------------------------------ queries

    /// Scatter/gather query across all live nodes.
    ///
    /// Phase 1 asks every node for ids whose *local* version matches the filter.
    /// Because a node might hold a stale version that matches while a newer one
    /// doesn't (or is deleted), phase 2 fetches every node's version of each
    /// candidate, resolves them with LWW, and re-applies the filter.
    pub async fn find(self: &Arc<Self>, c: &str, req: FindRequest) -> Result<FindResult, DbError> {
        let filter = if req.filter.is_null() { Value::Object(Map::new()) } else { req.filter };
        query::validate(&filter).map_err(DbError::BadRequest)?;

        let members = self.gossip.members().len();
        let nodes = self.gossip.live_members();
        let mut partial = nodes.len() < members;

        let phase1 = futures::future::join_all(nodes.iter().map(|n| self.scan_ids(n, c, &filter))).await;
        let mut candidates = BTreeSet::new();
        for (n, res) in nodes.iter().zip(phase1) {
            match res {
                Ok(ids) => candidates.extend(ids),
                Err(e) => {
                    warn!(node = %n, "scan failed: {e}");
                    partial = true;
                }
            }
        }
        let ids: Vec<String> = candidates.into_iter().collect();

        let mut latest: HashMap<String, Record> = HashMap::new();
        if !ids.is_empty() {
            let phase2 = futures::future::join_all(nodes.iter().map(|n| self.multiget(n, c, &ids))).await;
            for res in phase2 {
                match res {
                    Ok(recs) => {
                        for (k, r) in recs {
                            match latest.get(&k) {
                                Some(cur) if !r.supersedes(cur) => {}
                                _ => {
                                    latest.insert(k, r);
                                }
                            }
                        }
                    }
                    Err(_) => partial = true,
                }
            }
        }

        let mut docs: Vec<Value> = latest
            .into_values()
            .filter_map(|r| r.value)
            .filter(|v| query::matches(v, &filter))
            .collect();
        match &req.sort {
            Some(spec) => query::sort_docs(&mut docs, spec).map_err(DbError::BadRequest)?,
            None => docs.sort_by(|a, b| query::total_cmp(a.get("_id"), b.get("_id"))),
        }
        let matched = docs.len();
        let docs = docs
            .into_iter()
            .skip(req.skip.unwrap_or(0))
            .take(req.limit.unwrap_or(usize::MAX))
            .collect();
        Ok(FindResult { docs, matched, partial })
    }
}
