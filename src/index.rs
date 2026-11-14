//! Secondary index definitions: the cluster-wide schema and each node's local build state.
//!
//! An index is defined by `(collection, field)`. Definitions spread through gossip
//! (every gossip message carries the full list) and merge by last-write-wins on a
//! timestamp, so a drop and a later re-create resolve the same way everywhere.
//! Each node saves the schema plus the indexes it has finished building in
//! `<data-dir>/schema.json`.
//!
//! Indexes are local, as in Cassandra: each node indexes only the documents it
//! stores. When a node learns of a new index it starts maintaining it on every
//! write at once, backfills existing documents in the background, and only then
//! uses it for queries. A node that was down when an index was created builds it
//! when it comes back and hears about it. A dropped index stops being used at once
//! and its entries are purged by a compaction. Builds and drops run one at a time
//! on a single worker, so a quick drop-and-recreate can't interleave.

use crate::model::now_micros;
use crate::storage::Storage;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::{mpsc, Arc};
use tracing::{error, info};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IndexDef {
    pub collection: String,
    pub field: String,
    /// Version of this definition (creation or drop time, microseconds).
    pub ts: u64,
    #[serde(default)]
    pub dropped: bool,
}

impl IndexDef {
    fn supersedes(&self, o: &IndexDef) -> bool {
        (self.ts, self.dropped) > (o.ts, o.dropped)
    }
}

#[derive(Serialize, Deserialize, Default, Clone)]
struct Persisted {
    defs: Vec<IndexDef>,
    /// (collection, field, definition ts) of indexes fully built on this node.
    built: Vec<(String, String, u64)>,
}

#[derive(Serialize, Clone, Debug)]
pub struct IndexStatus {
    pub field: String,
    pub created_ts: u64,
    /// "ready" (used for queries) or "building" on this node.
    pub state: &'static str,
}

enum Job {
    Build(IndexDef),
    Drop(String, String),
}

pub struct IndexManager {
    path: PathBuf,
    storage: Arc<Storage>,
    state: Mutex<Persisted>,
    jobs: mpsc::Sender<Job>,
}

impl IndexManager {
    pub fn open(data_dir: &std::path::Path, storage: Arc<Storage>) -> io::Result<Arc<Self>> {
        let path = data_dir.join("schema.json");
        let state: Persisted = if path.exists() { serde_json::from_slice(&fs::read(&path)?)? } else { Persisted::default() };
        let (tx, rx) = mpsc::channel();
        let mgr = Arc::new(IndexManager { path, storage, state: Mutex::new(state), jobs: tx });
        let worker = Arc::downgrade(&mgr);
        std::thread::Builder::new().name("cassa-index".into()).spawn(move || {
            for job in rx {
                let Some(mgr) = worker.upgrade() else { return };
                mgr.run(job);
            }
        })?;
        mgr.reconcile();
        Ok(mgr)
    }

    fn persist(&self, state: &Persisted) {
        let tmp = self.path.with_extension("json.tmp");
        let res = serde_json::to_vec_pretty(state)
            .map_err(io::Error::from)
            .and_then(|b| fs::write(&tmp, b))
            .and_then(|_| fs::rename(&tmp, &self.path));
        if let Err(e) = res {
            error!("failed to save index schema: {e}");
        }
    }

    /// Current definitions, including drop markers (gossiped as-is).
    pub fn defs(&self) -> Vec<IndexDef> {
        self.state.lock().defs.clone()
    }

    /// Merge definitions (from gossip or a local create/drop); returns true if anything changed.
    pub fn merge(&self, incoming: Vec<IndexDef>) -> bool {
        let changed = {
            let mut st = self.state.lock();
            let mut changed = false;
            for d in incoming {
                match st.defs.iter_mut().find(|x| x.collection == d.collection && x.field == d.field) {
                    Some(cur) if d.supersedes(cur) => {
                        *cur = d;
                        changed = true;
                    }
                    Some(_) => {}
                    None => {
                        st.defs.push(d);
                        changed = true;
                    }
                }
            }
            if changed {
                let snapshot = st.clone();
                drop(st);
                self.persist(&snapshot);
            }
            changed
        };
        if changed {
            self.reconcile();
        }
        changed
    }

    /// Bring local storage in line with the schema.
    fn reconcile(&self) {
        let st = self.state.lock().clone();
        for d in &st.defs {
            let built = st.built.iter().any(|(c, f, ts)| *c == d.collection && *f == d.field && *ts == d.ts);
            let local = self.storage.index_state(&d.collection, &d.field);
            if d.dropped {
                let had_entries = local.is_some() || st.built.iter().any(|(c, f, _)| *c == d.collection && *f == d.field);
                if had_entries {
                    self.storage.remove_index(&d.collection, &d.field);
                    let _ = self.jobs.send(Job::Drop(d.collection.clone(), d.field.clone()));
                }
            } else if built {
                self.storage.set_index(&d.collection, &d.field, true);
            } else if local.is_none() {
                // Maintain on writes from now on, then backfill.
                self.storage.set_index(&d.collection, &d.field, false);
                let _ = self.jobs.send(Job::Build(d.clone()));
            }
        }
    }

    fn is_current(&self, d: &IndexDef) -> bool {
        self.state.lock().defs.iter().any(|x| x == d)
    }

    fn run(&self, job: Job) {
        match job {
            Job::Build(d) => {
                if !self.is_current(&d) {
                    return;
                }
                info!(collection = %d.collection, field = %d.field, "building index");
                match self.storage.build_index(&d.collection, &d.field) {
                    Ok(docs) => {
                        if !self.is_current(&d) {
                            return; // dropped or re-created meanwhile; a newer job handles it
                        }
                        let snapshot = {
                            let mut st = self.state.lock();
                            st.built.retain(|(c, f, _)| !(*c == d.collection && *f == d.field));
                            st.built.push((d.collection.clone(), d.field.clone(), d.ts));
                            st.clone()
                        };
                        self.persist(&snapshot);
                        self.storage.set_index(&d.collection, &d.field, true);
                        info!(collection = %d.collection, field = %d.field, docs, "index ready");
                    }
                    Err(e) => error!(collection = %d.collection, field = %d.field, "index build failed: {e}"),
                }
            }
            Job::Drop(c, f) => {
                match self.storage.purge_index(&c, &f) {
                    Ok(n) => info!(collection = %c, field = %f, entries = n, "index dropped and purged"),
                    Err(e) => {
                        error!(collection = %c, field = %f, "index purge failed: {e}");
                        return;
                    }
                }
                let snapshot = {
                    let mut st = self.state.lock();
                    st.built.retain(|(bc, bf, _)| !(*bc == c && *bf == f));
                    st.clone()
                };
                self.persist(&snapshot);
                // A re-create may have arrived while we were purging.
                self.reconcile();
            }
        }
    }

    // ---------------------------------------------------------------- API

    pub fn create(&self, collection: &str, field: &str) -> Result<(IndexDef, bool), String> {
        if field.is_empty() || field.len() > 256 || field.contains(['\0', '\u{1}']) || field.split('.').any(str::is_empty) {
            return Err("field must be a non-empty dotted path without NUL characters".into());
        }
        if field == "_id" {
            return Err("_id is the primary key and is always indexed".into());
        }
        if let Some(d) = self.active(collection, field) {
            return Ok((d, false));
        }
        let d = IndexDef { collection: collection.into(), field: field.into(), ts: now_micros(), dropped: false };
        self.merge(vec![d.clone()]);
        Ok((d, true))
    }

    pub fn drop_index(&self, collection: &str, field: &str) -> bool {
        if self.active(collection, field).is_none() {
            return false;
        }
        self.merge(vec![IndexDef { collection: collection.into(), field: field.into(), ts: now_micros(), dropped: true }]);
        true
    }

    fn active(&self, collection: &str, field: &str) -> Option<IndexDef> {
        self.state
            .lock()
            .defs
            .iter()
            .find(|d| d.collection == collection && d.field == field && !d.dropped)
            .cloned()
    }

    /// Active indexes on a collection with their build state on this node.
    pub fn list(&self, collection: &str) -> Vec<IndexStatus> {
        let st = self.state.lock();
        let mut out: BTreeMap<String, IndexStatus> = BTreeMap::new();
        for d in st.defs.iter().filter(|d| d.collection == collection && !d.dropped) {
            let ready = self.storage.index_state(collection, &d.field).is_some_and(|i| i.ready);
            out.insert(
                d.field.clone(),
                IndexStatus { field: d.field.clone(), created_ts: d.ts, state: if ready { "ready" } else { "building" } },
            );
        }
        out.into_values().collect()
    }
}

