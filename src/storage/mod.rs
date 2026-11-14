//! Per-node on-disk storage: a log-structured merge tree in the style of Cassandra.
//!
//! ```text
//!  write ──► commit log segment (fsync optional) ──► memtable (sorted, in memory)
//!                                                       │ when > memtable_bytes
//!                                                       ▼
//!                                  frozen memtable ──► background flush ──► SSTable
//!                                                                             │
//!                          size-tiered compaction merges similar-sized SSTables
//! ```
//!
//! * Writes never read: the newest version of a key is resolved at read time by
//!   timestamp across memtable, frozen memtables and SSTables (LWW).
//! * Point reads skip SSTables via bloom filters and per-table max timestamps.
//! * Scans stream a k-way merge over all sources, so datasets can exceed RAM.
//! * Expired tombstones (older than gc_grace) are purged during compaction, but only
//!   when no SSTable outside the compaction, nor any memtable, might hold the key.
//!
//! Files in the data directory:
//!   commitlog-<id>.log   write-ahead log segments not yet flushed
//!   sst-<gen>.db         immutable SSTables
//!   *.tmp                partial outputs from a crash; deleted on startup

mod commitlog;
mod merge;
pub mod secondary;
mod sstable;

use crate::model::{now_micros, Mutation, Record};
use crate::query;
use commitlog::CommitLog;
use merge::{Merge, Source};
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::Value;
use sstable::{Entry, SsTable};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::io;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc};
use tracing::{error, info, warn};

/// Maximum number of SSTables merged in one compaction.
const MAX_COMPACTION_INPUTS: usize = 32;
/// SSTables smaller than this are all considered the same size tier.
const MIN_TIER_BYTES: u64 = 1 << 20;

#[derive(Clone, Debug)]
pub struct Config {
    pub memtable_bytes: usize,
    pub compaction_threshold: usize,
    pub gc_grace_secs: u64,
    pub fsync: bool,
}

type Mem = BTreeMap<Vec<u8>, Record>;

/// Decides, per composite key, whether a cleanup compaction keeps an entry.
pub type KeyFilter = Arc<dyn Fn(&[u8]) -> bool + Send + Sync>;

struct Frozen {
    mem: Mem,
    /// Commit log segments with id <= this can be deleted once flushed.
    segment: u64,
    generation: u64,
}

struct State {
    mem: Mem,
    mem_bytes: usize,
    frozen: Vec<Arc<Frozen>>,
    sstables: Vec<Arc<SsTable>>,
    log: CommitLog,
    next_id: u64,
}

impl State {
    fn alloc_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

struct Shared {
    dir: PathBuf,
    cfg: Config,
    state: Mutex<State>,
}

enum Job {
    Flush(Arc<Frozen>),
    Compact { major: bool, done: Option<mpsc::Sender<io::Result<usize>>> },
    Cleanup { keep: KeyFilter, done: mpsc::Sender<io::Result<u64>> },
    Shutdown(mpsc::Sender<()>),
}

pub struct Storage {
    shared: Arc<Shared>,
    jobs: mpsc::Sender<Job>,
    /// Secondary indexes maintained on writes: collection -> indexes.
    indexes: parking_lot::RwLock<HashMap<String, Vec<IndexState>>>,
    /// Serializes read-current-version + write for indexed collections, so index
    /// maintenance from concurrent writes to the same document can't interleave.
    index_write_lock: Mutex<()>,
}

#[derive(Clone, Debug)]
pub struct IndexState {
    pub field: String,
    /// False while the index is being built; maintained on writes but not queried.
    pub ready: bool,
}

/// Result of a local query: matching ids plus how they were found.
#[derive(Serialize, serde::Deserialize, Debug, Default)]
pub struct ScanResult {
    pub ids: Vec<String>,
    /// The index used, or `None` for a full collection scan.
    pub index: Option<String>,
    /// Documents (full scan) or index candidates (index scan) examined.
    pub examined: usize,
}

#[derive(Serialize)]
pub struct SsTableInfo {
    pub generation: u64,
    pub entries: u64,
    pub bytes: u64,
}

#[derive(Serialize)]
pub struct StorageInfo {
    pub data_dir: String,
    pub memtable_entries: usize,
    pub memtable_bytes: usize,
    pub flushing_memtables: usize,
    pub commitlog_segment: u64,
    pub sstable_count: usize,
    pub sstable_bytes: u64,
    pub sstables: Vec<SsTableInfo>,
}

// ------------------------------------------------------------------ keys

/// Composite key: `collection \0 id`. Collection names never contain NUL, so all
/// documents of a collection form one contiguous key range.
fn encode_key(c: &str, k: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(c.len() + 1 + k.len());
    key.extend(c.as_bytes());
    key.push(0);
    key.extend(k.as_bytes());
    key
}

/// The data key whose token decides where an entry lives: the key itself for
/// documents, or the indexed document's key for index entries.
pub fn data_key_for(key: &[u8]) -> std::borrow::Cow<'_, [u8]> {
    match secondary::parse_index_key(key) {
        Some((c, _, id)) => {
            let mut k = c.to_vec();
            k.push(0);
            k.extend(id);
            std::borrow::Cow::Owned(k)
        }
        None => std::borrow::Cow::Borrowed(key),
    }
}

pub fn split_key(key: &[u8]) -> (&[u8], &[u8]) {
    match key.iter().position(|b| *b == 0) {
        Some(i) => (&key[..i], &key[i + 1..]),
        None => (key, &[]),
    }
}

fn collection_range(c: &str) -> (Vec<u8>, Vec<u8>) {
    let mut start = c.as_bytes().to_vec();
    let mut end = start.clone();
    start.push(0);
    end.push(1);
    (start, end)
}

fn sst_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("sst-{generation:010}.db"))
}

fn keep_newest(best: &mut Option<Record>, cand: Option<&Record>) {
    if let Some(c) = cand
        && best.as_ref().is_none_or(|b| c.supersedes(b))
    {
        *best = Some(c.clone());
    }
}

fn mem_insert(mem: &mut Mem, key: Vec<u8>, rec: Record) {
    match mem.get(&key) {
        Some(existing) if !rec.supersedes(existing) => {}
        _ => {
            mem.insert(key, rec);
        }
    }
}

/// Run blocking storage work off the async runtime.
pub async fn blocking<T: Send + 'static>(f: impl FnOnce() -> io::Result<T> + Send + 'static) -> Result<T, String> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())
}

// ------------------------------------------------------------------ storage

impl Storage {
    pub fn open(dir: &Path, cfg: Config) -> io::Result<Self> {
        fs::create_dir_all(dir)?;

        let mut sstables = Vec::new();
        let mut max_id = 0u64;
        for e in fs::read_dir(dir)? {
            let e = e?;
            let name = e.file_name().to_string_lossy().into_owned();
            if name.ends_with(".tmp") {
                warn!(file = %name, "removing partial file from an interrupted flush/compaction");
                fs::remove_file(e.path())?;
            } else if let Some(generation) = name.strip_prefix("sst-").and_then(|s| s.strip_suffix(".db")).and_then(|s| s.parse::<u64>().ok()) {
                sstables.push(Arc::new(SsTable::open(&e.path(), generation)?));
                max_id = max_id.max(generation);
            }
        }
        sstables.sort_by_key(|t| t.generation);

        let segments = commitlog::list_segments(dir)?;
        let mut replayed_mem = Mem::new();
        let mut replayed = 0usize;
        for &seg in &segments {
            max_id = max_id.max(seg);
            for m in commitlog::replay(dir, seg)? {
                mem_insert(&mut replayed_mem, encode_key(&m.c, &m.k), m.r);
                replayed += 1;
            }
        }

        let mut next_id = max_id + 1;
        let log = CommitLog::create(dir, next_id, cfg.fsync)?;
        next_id += 1;

        // Replayed data goes straight to a frozen memtable and is flushed in the background.
        let mut frozen = Vec::new();
        if let Some(&last) = segments.last() {
            frozen.push(Arc::new(Frozen { mem: replayed_mem, segment: last, generation: next_id }));
            next_id += 1;
        }

        let shared = Arc::new(Shared {
            dir: dir.to_path_buf(),
            cfg,
            state: Mutex::new(State {
                mem: Mem::new(),
                mem_bytes: 0,
                frozen: frozen.clone(),
                sstables,
                log,
                next_id,
            }),
        });

        let (tx, rx) = mpsc::channel();
        let worker_shared = shared.clone();
        std::thread::Builder::new()
            .name("cassa-storage".into())
            .spawn(move || worker(worker_shared, rx))?;
        for f in frozen {
            let _ = tx.send(Job::Flush(f));
        }
        let _ = tx.send(Job::Compact { major: false, done: None });

        {
            let s = shared.state.lock();
            info!(dir = %dir.display(), sstables = s.sstables.len(), replayed, "storage opened");
        }
        Ok(Storage { shared, jobs: tx, indexes: Default::default(), index_write_lock: Mutex::new(()) })
    }

    /// Freeze the active memtable, start a new commit log segment, and queue a flush.
    fn rotate(&self, s: &mut State) -> io::Result<()> {
        if s.mem.is_empty() {
            return Ok(());
        }
        s.log.sync()?;
        let new_seg = s.alloc_id();
        let old_seg = std::mem::replace(&mut s.log, CommitLog::create(&self.shared.dir, new_seg, self.shared.cfg.fsync)?).id;
        let generation = s.alloc_id();
        let frozen = Arc::new(Frozen { mem: std::mem::take(&mut s.mem), segment: old_seg, generation });
        s.mem_bytes = 0;
        s.frozen.push(frozen.clone());
        self.jobs
            .send(Job::Flush(frozen))
            .map_err(|_| io::Error::other("storage worker has stopped"))
    }

    /// Durably log and apply mutations. Returns the number written.
    pub fn apply_batch(&self, muts: Vec<Mutation>) -> io::Result<usize> {
        let n = muts.len();
        let indexes = self.indexes.read().clone();
        let indexed = !indexes.is_empty() && muts.iter().any(|m| indexes.contains_key(&m.c));
        // Held across reading current versions and writing, see `index_write_lock`.
        let _guard = indexed.then(|| self.index_write_lock.lock());
        let muts = if indexed { self.with_index_entries(muts, &indexes)? } else { muts };
        self.write_raw(muts)?;
        Ok(n)
    }

    /// Add index maintenance entries for every mutation that will become the winning
    /// version of an indexed document: tombstones for the old version's index keys
    /// and live entries for the new one, all at the new version's timestamp.
    fn with_index_entries(&self, muts: Vec<Mutation>, indexes: &HashMap<String, Vec<IndexState>>) -> io::Result<Vec<Mutation>> {
        let mut overlay: HashMap<(String, String), Record> = HashMap::new(); // earlier writes in this batch
        let mut out = Vec::with_capacity(muts.len() * 2);
        for m in muts {
            if let Some(specs) = indexes.get(&m.c).filter(|s| !s.is_empty()) {
                let key = (m.c.clone(), m.k.clone());
                let cur = match overlay.get(&key) {
                    Some(r) => Some(r.clone()),
                    None => self.get(&m.c, &m.k)?,
                };
                if cur.as_ref().is_none_or(|c| m.r.supersedes(c)) {
                    let keys_of = |r: Option<&Record>, field: &str| -> BTreeSet<String> {
                        r.and_then(|r| r.value.as_ref())
                            .map(|v| secondary::index_keys(&m.c, field, v, &m.k))
                            .unwrap_or_default()
                    };
                    for spec in specs {
                        let old = keys_of(cur.as_ref(), &spec.field);
                        let new = keys_of(Some(&m.r), &spec.field);
                        for k in old.difference(&new) {
                            out.push(Mutation { c: secondary::INDEX_C.into(), k: k.clone(), r: Record { ts: m.r.ts, value: None } });
                        }
                        for k in new {
                            out.push(Mutation { c: secondary::INDEX_C.into(), k, r: Record { ts: m.r.ts, value: Some(Value::Bool(true)) } });
                        }
                    }
                    overlay.insert(key, m.r.clone());
                }
            }
            out.push(m);
        }
        Ok(out)
    }

    /// Log and apply mutations without index maintenance.
    fn write_raw(&self, muts: Vec<Mutation>) -> io::Result<()> {
        let mut s = self.shared.state.lock();
        for m in muts {
            let bytes = s.log.append(&m)?;
            s.mem_bytes += bytes + 64; // rough per-entry overhead
            mem_insert(&mut s.mem, encode_key(&m.c, &m.k), m.r);
        }
        s.log.sync()?;
        if s.mem_bytes >= self.shared.cfg.memtable_bytes {
            self.rotate(&mut s)?;
        }
        Ok(())
    }

    // ------------------------------------------------------------ secondary indexes

    /// Start maintaining an index on writes (`ready = false`) or mark it queryable.
    pub fn set_index(&self, c: &str, field: &str, ready: bool) {
        let mut idx = self.indexes.write();
        let list = idx.entry(c.to_string()).or_default();
        match list.iter_mut().find(|i| i.field == field) {
            Some(i) => i.ready = ready,
            None => list.push(IndexState { field: field.to_string(), ready }),
        }
    }

    pub fn remove_index(&self, c: &str, field: &str) {
        let mut idx = self.indexes.write();
        if let Some(list) = idx.get_mut(c) {
            list.retain(|i| i.field != field);
            if list.is_empty() {
                idx.remove(c);
            }
        }
    }

    pub fn index_state(&self, c: &str, field: &str) -> Option<IndexState> {
        self.indexes.read().get(c)?.iter().find(|i| i.field == field).cloned()
    }

    /// Backfill an index from existing documents. Entries carry the document's
    /// timestamp, so if a concurrent write already replaced a document (and wrote a
    /// newer tombstone for its old index key), the stale backfill entry loses.
    pub fn build_index(&self, c: &str, field: &str) -> io::Result<usize> {
        let (start, end) = collection_range(c);
        let (mut batch, mut docs) = (Vec::new(), 0usize);
        for e in self.scan(&start, Some(end))? {
            let (key, rec) = e?;
            let Some(v) = &rec.value else { continue };
            let id = String::from_utf8_lossy(split_key(&key).1).into_owned();
            for k in secondary::index_keys(c, field, v, &id) {
                batch.push(Mutation { c: secondary::INDEX_C.into(), k, r: Record { ts: rec.ts, value: Some(Value::Bool(true)) } });
            }
            docs += 1;
            if batch.len() >= 1000 {
                self.write_raw(std::mem::take(&mut batch))?;
            }
        }
        if !batch.is_empty() {
            self.write_raw(batch)?;
        }
        Ok(docs)
    }

    /// Physically remove every entry of an index (a cleanup compaction).
    pub fn purge_index(&self, c: &str, field: &str) -> io::Result<u64> {
        let prefix = secondary::index_prefix(c, field);
        self.cleanup(Arc::new(move |k: &[u8]| !k.starts_with(&prefix)))
    }

    /// Live document ids in `c` matching `filter`, using a ready index when one fits.
    pub fn query_ids(&self, c: &str, filter: &Value) -> io::Result<ScanResult> {
        let ready: Vec<String> = self
            .indexes
            .read()
            .get(c)
            .map(|l| l.iter().filter(|i| i.ready).map(|i| i.field.clone()).collect())
            .unwrap_or_default();
        if let Some(plan) = secondary::choose(c, filter, &ready) {
            let mut seen = HashSet::new();
            let mut candidates = Vec::new();
            for (start, end) in &plan.ranges {
                for e in self.scan(start, Some(end.clone()))? {
                    let (key, rec) = e?;
                    if rec.is_tombstone() {
                        continue;
                    }
                    if let Some((_, _, id)) = secondary::parse_index_key(&key) {
                        let id = String::from_utf8_lossy(id).into_owned();
                        if seen.insert(id.clone()) {
                            candidates.push(id);
                        }
                    }
                }
            }
            // The index only narrows the search; the full filter decides.
            let mut ids = Vec::new();
            for id in &candidates {
                if self.get(c, id)?.and_then(|r| r.value).is_some_and(|v| query::matches(&v, filter)) {
                    ids.push(id.clone());
                }
            }
            return Ok(ScanResult { ids, index: Some(plan.field), examined: candidates.len() });
        }

        let (start, end) = collection_range(c);
        let mut res = ScanResult::default();
        for e in self.scan(&start, Some(end))? {
            let (key, rec) = e?;
            if let Some(v) = &rec.value {
                res.examined += 1;
                if query::matches(v, filter) {
                    res.ids.push(String::from_utf8_lossy(split_key(&key).1).into_owned());
                }
            }
        }
        Ok(res)
    }

    pub fn get(&self, c: &str, k: &str) -> io::Result<Option<Record>> {
        let key = encode_key(c, k);
        let (mut best, mut tables) = {
            let s = self.shared.state.lock();
            let mut best = s.mem.get(&key).cloned();
            for f in &s.frozen {
                keep_newest(&mut best, f.mem.get(&key));
            }
            (best, s.sstables.clone())
        };
        // Newest tables first; stop once no remaining table can hold anything newer.
        tables.sort_by_key(|t| Reverse(t.max_ts));
        for t in tables {
            if best.as_ref().is_some_and(|b| b.ts > t.max_ts) {
                break;
            }
            keep_newest(&mut best, t.get(&key)?.as_ref());
        }
        Ok(best)
    }

    /// A merged, reconciled stream over keys in `[start, end)`.
    fn scan(&self, start: &[u8], end: Option<Vec<u8>>) -> io::Result<Merge> {
        let upper = match &end {
            Some(e) => Bound::Excluded(e.as_slice()),
            None => Bound::Unbounded,
        };
        let range = (Bound::Included(start), upper);
        let mut sources: Vec<Source> = Vec::new();
        let tables = {
            let s = self.shared.state.lock();
            // Memtables are bounded in size, so copying the range out is cheap and
            // lets us release the lock before touching disk.
            let snap = |m: &Mem| -> Source {
                let v: Vec<io::Result<Entry>> = m.range::<[u8], _>(range).map(|(k, r)| Ok((k.clone(), r.clone()))).collect();
                Box::new(v.into_iter())
            };
            sources.push(snap(&s.mem));
            for f in &s.frozen {
                sources.push(snap(&f.mem));
            }
            s.sstables.clone()
        };
        for t in tables {
            sources.push(Box::new(t.iter_from(start)));
        }
        Merge::new(sources, end)
    }

    /// Visit every reconciled entry on this node (tombstones included) in key order.
    /// Keys are composite `collection \0 id`, so `ring::hash64(key)` is the token.
    pub fn for_each_entry(&self, mut f: impl FnMut(&[u8], &Record)) -> io::Result<()> {
        self.scan_from(&[], |k, r| {
            f(k, r);
            true
        })
    }

    /// Visit reconciled entries with key >= `start` in key order until `f` returns false.
    /// Index entries are local derived data, so they're excluded: repair, streaming and
    /// stats only ever see documents.
    pub fn scan_from(&self, start: &[u8], mut f: impl FnMut(&[u8], &Record) -> bool) -> io::Result<()> {
        let start = if start < secondary::DATA_START { secondary::DATA_START } else { start };
        for e in self.scan(start, None)? {
            let (key, rec) = e?;
            if !f(&key, &rec) {
                break;
            }
        }
        Ok(())
    }

    /// IDs of live documents in collection `c` matching `filter`.
    #[cfg(test)]
    pub fn scan_ids(&self, c: &str, filter: &Value) -> io::Result<Vec<String>> {
        Ok(self.query_ids(c, filter)?.ids)
    }

    /// Local records (including tombstones) for the given ids.
    pub fn multiget(&self, c: &str, ids: &[String]) -> io::Result<Vec<(String, Record)>> {
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(r) = self.get(c, id)? {
                out.push((id.clone(), r));
            }
        }
        Ok(out)
    }

    /// Live document count per collection on this node (full scan).
    pub fn stats(&self) -> io::Result<BTreeMap<String, usize>> {
        let mut counts = BTreeMap::new();
        self.scan_from(&[], |key, rec| {
            if !rec.is_tombstone() {
                *counts.entry(String::from_utf8_lossy(split_key(key).0).into_owned()).or_default() += 1;
            }
            true
        })?;
        Ok(counts)
    }

    pub fn info(&self) -> StorageInfo {
        let s = self.shared.state.lock();
        StorageInfo {
            data_dir: self.shared.dir.display().to_string(),
            memtable_entries: s.mem.len(),
            memtable_bytes: s.mem_bytes,
            flushing_memtables: s.frozen.len(),
            commitlog_segment: s.log.id,
            sstable_count: s.sstables.len(),
            sstable_bytes: s.sstables.iter().map(|t| t.size).sum(),
            sstables: s
                .sstables
                .iter()
                .map(|t| SsTableInfo { generation: t.generation, entries: t.entries, bytes: t.size })
                .collect(),
        }
    }

    /// Flush the memtable and run compaction, waiting for completion.
    /// `major` merges every SSTable into one (purging all expired tombstones).
    pub fn compact(&self, major: bool) -> io::Result<usize> {
        {
            let mut s = self.shared.state.lock();
            self.rotate(&mut s)?;
        }
        let (tx, rx) = mpsc::channel();
        self.jobs
            .send(Job::Compact { major, done: Some(tx) })
            .map_err(|_| io::Error::other("storage worker has stopped"))?;
        rx.recv().map_err(|_| io::Error::other("storage worker has stopped"))?
    }

    /// Cleanup compaction: flush, then rewrite every SSTable dropping entries for which
    /// `keep` returns false (data for token ranges this node no longer owns).
    /// Returns the number of entries dropped.
    pub fn cleanup(&self, keep: KeyFilter) -> io::Result<u64> {
        {
            let mut s = self.shared.state.lock();
            self.rotate(&mut s)?;
        }
        let (tx, rx) = mpsc::channel();
        self.jobs
            .send(Job::Cleanup { keep, done: tx })
            .map_err(|_| io::Error::other("storage worker has stopped"))?;
        rx.recv().map_err(|_| io::Error::other("storage worker has stopped"))?
    }

    /// Flush the memtable and stop the background worker.
    pub fn shutdown(&self) -> io::Result<()> {
        {
            let mut s = self.shared.state.lock();
            self.rotate(&mut s)?;
        }
        let (tx, rx) = mpsc::channel();
        if self.jobs.send(Job::Shutdown(tx)).is_ok() {
            let _ = rx.recv();
        }
        Ok(())
    }
}

// ------------------------------------------------------------------ background worker

fn worker(shared: Arc<Shared>, rx: mpsc::Receiver<Job>) {
    for job in rx {
        match job {
            Job::Flush(f) => {
                if let Err(e) = flush(&shared, &f) {
                    // The data stays readable in the frozen memtable and its commit
                    // log segment is kept, so nothing is lost; it is retried on restart.
                    error!("memtable flush failed: {e}");
                    continue;
                }
                if let Err(e) = maybe_compact(&shared) {
                    error!("compaction failed: {e}");
                }
            }
            Job::Compact { major, done } => {
                let res = if major { major_compact(&shared) } else { maybe_compact(&shared) };
                if let Err(e) = &res {
                    error!("compaction failed: {e}");
                }
                if let Some(d) = done {
                    let _ = d.send(res);
                }
            }
            Job::Cleanup { keep, done } => {
                let tables = shared.state.lock().sstables.clone();
                let res = if tables.is_empty() {
                    Ok(0)
                } else {
                    compact(&shared, tables, Some(&keep)).map(|(_, dropped)| dropped)
                };
                if let Err(e) = &res {
                    error!("cleanup failed: {e}");
                }
                let _ = done.send(res);
            }
            Job::Shutdown(ack) => {
                let _ = ack.send(());
                return;
            }
        }
    }
}

fn flush(shared: &Shared, f: &Arc<Frozen>) -> io::Result<()> {
    let table = if f.mem.is_empty() {
        None
    } else {
        let path = sst_path(&shared.dir, f.generation);
        let n = sstable::write(&path, f.mem.len(), f.mem.iter().map(|(k, r)| Ok((k.clone(), r.clone()))))?;
        let t = Arc::new(SsTable::open(&path, f.generation)?);
        info!(generation = f.generation, entries = n, bytes = t.size, "flushed memtable to sstable");
        Some(t)
    };
    {
        let mut s = shared.state.lock();
        if let Some(t) = table {
            s.sstables.push(t);
        }
        s.frozen.retain(|x| !Arc::ptr_eq(x, f));
    }
    // Only now that the SSTable is durable can the commit log be dropped.
    for seg in commitlog::list_segments(&shared.dir)? {
        if seg <= f.segment {
            fs::remove_file(commitlog::segment_path(&shared.dir, seg))?;
        }
    }
    Ok(())
}

/// Size-tiered bucketing: group SSTables of similar size and return the first group
/// with at least `threshold` members.
fn pick_bucket(tables: &[Arc<SsTable>], threshold: usize) -> Option<Vec<Arc<SsTable>>> {
    let mut sorted = tables.to_vec();
    sorted.sort_by_key(|t| t.size);
    let mut buckets: Vec<Vec<Arc<SsTable>>> = Vec::new();
    for t in sorted {
        let fits = buckets.last().is_some_and(|b| {
            let avg = b.iter().map(|x| x.size).sum::<u64>() / b.len() as u64;
            t.size <= (avg * 2).max(MIN_TIER_BYTES)
        });
        if fits {
            buckets.last_mut().unwrap().push(t);
        } else {
            buckets.push(vec![t]);
        }
    }
    buckets.into_iter().find(|b| b.len() >= threshold.max(2)).map(|mut b| {
        b.truncate(MAX_COMPACTION_INPUTS);
        b
    })
}

fn maybe_compact(shared: &Shared) -> io::Result<usize> {
    let mut compacted = 0;
    loop {
        let tables = shared.state.lock().sstables.clone();
        match pick_bucket(&tables, shared.cfg.compaction_threshold) {
            Some(bucket) => compacted += compact(shared, bucket, None)?.0,
            None => return Ok(compacted),
        }
    }
}

fn major_compact(shared: &Shared) -> io::Result<usize> {
    let tables = shared.state.lock().sstables.clone();
    if tables.is_empty() {
        return Ok(0);
    }
    compact(shared, tables, None).map(|(n, _)| n)
}

/// Merge `inputs` into one SSTable. Returns (inputs compacted, entries dropped by `keep`).
fn compact(shared: &Shared, inputs: Vec<Arc<SsTable>>, keep: Option<&KeyFilter>) -> io::Result<(usize, u64)> {
    let (others, generation) = {
        let mut s = shared.state.lock();
        let others: Vec<Arc<SsTable>> = s
            .sstables
            .iter()
            .filter(|t| !inputs.iter().any(|i| Arc::ptr_eq(i, t)))
            .cloned()
            .collect();
        (others, s.alloc_id())
    };
    let cutoff = now_micros().saturating_sub(shared.cfg.gc_grace_secs.saturating_mul(1_000_000));
    let in_memory = |key: &[u8]| {
        let s = shared.state.lock();
        s.mem.contains_key(key) || s.frozen.iter().any(|f| f.mem.contains_key(key))
    };

    let sources: Vec<Source> = inputs.iter().map(|t| Box::new(t.iter_from(&[])) as Source).collect();
    let expected: u64 = inputs.iter().map(|t| t.entries).sum();
    let (mut purged, mut dropped) = (0u64, 0u64);
    let merged = Merge::new(sources, None)?.filter(|e| match e {
        Ok((key, rec)) => {
            if keep.is_some_and(|k| !k(key)) {
                dropped += 1;
                return false;
            }
            let purge = rec.is_tombstone()
                && rec.ts < cutoff
                && !others.iter().any(|o| o.may_contain(key))
                && !in_memory(key);
            purged += purge as u64;
            !purge
        }
        Err(_) => true,
    });

    let path = sst_path(&shared.dir, generation);
    let n = sstable::write(&path, expected as usize, merged)?;
    let output = if n > 0 {
        Some(Arc::new(SsTable::open(&path, generation)?))
    } else {
        fs::remove_file(&path)?;
        None
    };
    let out_bytes = output.as_ref().map_or(0, |t| t.size);
    {
        let mut s = shared.state.lock();
        s.sstables.retain(|t| !inputs.iter().any(|i| Arc::ptr_eq(i, t)));
        if let Some(t) = output {
            s.sstables.push(t);
        }
    }
    for t in &inputs {
        t.mark_obsolete(); // deleted when the last in-flight reader drops it
    }
    info!(
        inputs = inputs.len(),
        in_bytes = inputs.iter().map(|t| t.size).sum::<u64>(),
        out_entries = n,
        out_bytes,
        purged_tombstones = purged,
        dropped_unowned = dropped,
        "compacted sstables"
    );
    Ok((inputs.len(), dropped))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn tmpdir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("cassa-store-{name}-{}", now_micros()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    fn cfg(memtable_bytes: usize, gc_grace_secs: u64) -> Config {
        Config { memtable_bytes, compaction_threshold: 4, gc_grace_secs, fsync: false }
    }

    fn m(k: &str, ts: u64, v: Option<Value>) -> Mutation {
        Mutation { c: "users".into(), k: k.into(), r: Record { ts, value: v } }
    }

    fn files(dir: &Path, prefix: &str) -> usize {
        fs::read_dir(dir).unwrap().filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().starts_with(prefix)).count()
    }

    #[test]
    fn lww_across_memtable_and_sstables() {
        let dir = tmpdir("lww");
        let s = Storage::open(&dir, cfg(1 << 20, 3600)).unwrap();
        let t = now_micros();
        s.apply_batch(vec![m("a", t + 10, Some(json!({"n": 1})))]).unwrap();
        s.compact(false).unwrap(); // a@10 now in an sstable
        s.apply_batch(vec![m("a", t + 5, Some(json!({"n": 0})))]).unwrap(); // older write lands in memtable
        assert_eq!(s.get("users", "a").unwrap().unwrap().value, Some(json!({"n": 1})), "newer sstable value wins");
        s.apply_batch(vec![m("a", t + 20, None)]).unwrap();
        assert!(s.get("users", "a").unwrap().unwrap().is_tombstone());
        assert!(s.scan_ids("users", &json!({})).unwrap().is_empty());
        s.shutdown().unwrap();
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn recovery_from_commitlog_and_sstables() {
        let dir = tmpdir("recover");
        let t = now_micros();
        {
            let s = Storage::open(&dir, cfg(1 << 20, 3600)).unwrap();
            s.apply_batch(vec![m("a", t, Some(json!({"n": 1})))]).unwrap();
            s.compact(false).unwrap();
            s.apply_batch(vec![m("b", t, Some(json!({"n": 2}))), m("c", t, Some(json!({"n": 3}))), m("c", t + 1, None)]).unwrap();
            // dropped without shutdown: b and c exist only in the commit log
        }
        let s = Storage::open(&dir, cfg(1 << 20, 3600)).unwrap();
        assert_eq!(s.get("users", "a").unwrap().unwrap().value, Some(json!({"n": 1})));
        assert_eq!(s.get("users", "b").unwrap().unwrap().value, Some(json!({"n": 2})));
        assert!(s.get("users", "c").unwrap().unwrap().is_tombstone());
        s.compact(false).unwrap(); // wait for the replayed memtable to flush
        assert_eq!(files(&dir, "commitlog-"), 1, "flushed segments deleted, only active one left");
        s.shutdown().unwrap();
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn flush_compaction_and_scans_beyond_memtable() {
        let dir = tmpdir("compact");
        // Tiny memtable so we produce many sstables.
        let s = Storage::open(&dir, cfg(4 * 1024, 3600)).unwrap();
        let t = now_micros();
        for i in 0..2000u64 {
            s.apply_batch(vec![m(&format!("{i:05}"), t + i, Some(json!({"i": i, "even": i % 2 == 0})))]).unwrap();
        }
        // Overwrite and delete some keys so versions span sstables.
        for i in (0..2000u64).step_by(10) {
            s.apply_batch(vec![m(&format!("{i:05}"), t + 10_000 + i, Some(json!({"i": i, "even": false, "updated": true})))]).unwrap();
        }
        for i in (5..2000u64).step_by(100) {
            s.apply_batch(vec![m(&format!("{i:05}"), t + 20_000 + i, None)]).unwrap();
        }
        s.compact(false).unwrap();
        let info = s.info();
        assert!(info.sstable_count < 20, "size-tiered compaction kept table count down: {}", info.sstable_count);

        assert_eq!(s.stats().unwrap()["users"], 2000 - 20);
        assert_eq!(s.get("users", "00010").unwrap().unwrap().value.unwrap()["updated"], json!(true));
        assert!(s.get("users", "00105").unwrap().unwrap().is_tombstone());
        // evens minus the updated ones (every 10th, which are even)
        assert_eq!(s.scan_ids("users", &json!({"even": true})).unwrap().len(), 1000 - 200);

        s.compact(true).unwrap();
        assert_eq!(s.info().sstable_count, 1, "major compaction leaves one table");
        assert_eq!(s.stats().unwrap()["users"], 2000 - 20);
        s.shutdown().unwrap();
        assert_eq!(files(&dir, "sst-"), 1, "obsolete sstables deleted");

        // Everything survives a restart.
        let s = Storage::open(&dir, cfg(4 * 1024, 3600)).unwrap();
        assert_eq!(s.stats().unwrap()["users"], 2000 - 20);
        s.shutdown().unwrap();
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn cleanup_drops_unowned_keys_and_paged_scan() {
        let dir = tmpdir("cleanup");
        let s = Storage::open(&dir, cfg(4 * 1024, 3600)).unwrap();
        let t = now_micros();
        for i in 0..300u64 {
            s.apply_batch(vec![m(&format!("{i:03}"), t + i, Some(json!({"i": i})))]).unwrap();
        }
        let mut seen = Vec::new();
        s.scan_from(&encode_key("users", "295"), |k, _| {
            seen.push(String::from_utf8_lossy(split_key(k).1).into_owned());
            seen.len() < 3
        })
        .unwrap();
        assert_eq!(seen, vec!["295", "296", "297"]);

        // keep only even ids
        let dropped = s
            .cleanup(Arc::new(|k: &[u8]| String::from_utf8_lossy(split_key(k).1).parse::<u64>().unwrap() % 2 == 0))
            .unwrap();
        assert_eq!(dropped, 150);
        assert_eq!(s.stats().unwrap()["users"], 150);
        assert!(s.get("users", "001").unwrap().is_none());
        assert!(s.get("users", "002").unwrap().is_some());
        s.shutdown().unwrap();
        fs::remove_dir_all(dir).unwrap();
    }

    /// Live index entries for `c.field`, as (encoded value suffix, id) strings.
    fn live_index_entries(s: &Storage, c: &str, field: &str) -> Vec<String> {
        let prefix = secondary::index_prefix(c, field);
        let mut end = prefix.clone();
        *end.last_mut().unwrap() = 1;
        s.scan(&prefix, Some(end))
            .unwrap()
            .map(Result::unwrap)
            .filter(|(_, r)| !r.is_tombstone())
            .map(|(k, _)| String::from_utf8_lossy(&k[prefix.len()..]).replace('\0', "|"))
            .collect()
    }

    #[test]
    fn index_maintenance_is_exact_under_updates_deletes_and_reordering() {
        let dir = tmpdir("index");
        let s = Storage::open(&dir, cfg(2 * 1024, 3600)).unwrap();
        let t = now_micros();
        s.apply_batch(vec![m("pre", t, Some(json!({"city": "York"})))]).unwrap();
        s.set_index("users", "city", false);
        assert_eq!(s.build_index("users", "city").unwrap(), 1);
        s.set_index("users", "city", true);

        s.apply_batch(vec![m("a", t + 1, Some(json!({"city": "Leeds"}))), m("b", t + 1, Some(json!({"city": ["Leeds", "Hull"]})))]).unwrap();
        s.apply_batch(vec![m("a", t + 5, Some(json!({"city": "York"})))]).unwrap(); // update
        s.apply_batch(vec![m("a", t + 3, Some(json!({"city": "Bath"})))]).unwrap(); // late, older write: ignored
        s.apply_batch(vec![m("b", t + 6, None)]).unwrap(); // delete
        s.apply_batch(vec![m("c", t + 7, Some(json!({"city": "Hull"}))), m("c", t + 8, Some(json!({"city": "Bath"})))]).unwrap(); // same batch

        let mut entries = live_index_entries(&s, "users", "city");
        entries.sort();
        let hull = secondary::encode_value(&json!("Bath")).unwrap();
        let york = secondary::encode_value(&json!("York")).unwrap();
        assert_eq!(entries, vec![format!("{hull}|c"), format!("{york}|a"), format!("{york}|pre")], "no stale entries");

        let r = s.query_ids("users", &json!({"city": "York"})).unwrap();
        assert_eq!((r.ids.clone(), r.index.as_deref(), r.examined), (vec!["a".to_string(), "pre".to_string()], Some("city"), 2));
        assert!(s.query_ids("users", &json!({"city": "Leeds"})).unwrap().ids.is_empty());
        let full = s.query_ids("users", &json!({"zip": 1})).unwrap();
        assert_eq!((full.index, full.examined), (None, 3));

        // Index entries never leak into data scans or stats.
        assert_eq!(s.stats().unwrap().len(), 1);
        let mut n = 0;
        s.for_each_entry(|k, _| {
            assert_ne!(k[0], 1);
            n += 1;
        })
        .unwrap();
        assert_eq!(n, 4, "pre, a, b (tombstone), c, and no index keys");

        // Survives restart (commit log replay of index mutations).
        s.shutdown().unwrap();
        drop(s);
        let s = Storage::open(&dir, cfg(2 * 1024, 3600)).unwrap();
        assert_eq!(live_index_entries(&s, "users", "city").len(), 3);
        assert_eq!(s.purge_index("users", "city").unwrap(), 7, "3 live + 4 tombstones (Leeds|a, Leeds|b, Hull|b, Hull|c)");
        assert!(live_index_entries(&s, "users", "city").is_empty());
        s.shutdown().unwrap();
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn data_key_for_index_entries() {
        let k = secondary::index_keys("users", "city", &json!({"city": "Leeds"}), "id7").into_iter().next().unwrap();
        let raw = encode_key(secondary::INDEX_C, &k);
        assert_eq!(data_key_for(&raw).as_ref(), encode_key("users", "id7").as_slice());
        assert_eq!(data_key_for(b"users\0x").as_ref(), b"users\0x");
    }

    #[test]
    fn tombstones_purged_only_after_gc_grace_on_major_compaction() {
        let dir = tmpdir("gc");
        let s = Storage::open(&dir, cfg(1 << 20, 0)).unwrap();
        let t = now_micros();
        s.apply_batch(vec![m("a", t - 100, Some(json!({})))]).unwrap();
        s.compact(false).unwrap();
        s.apply_batch(vec![m("a", t - 50, None), m("b", t - 50, Some(json!({})))]).unwrap();
        s.compact(false).unwrap();
        // Minor compaction can't run (2 tables < threshold) and the tombstone must
        // still shadow the older value in the other sstable.
        assert!(s.get("users", "a").unwrap().unwrap().is_tombstone());
        s.compact(true).unwrap();
        assert!(s.get("users", "a").unwrap().is_none(), "tombstone and shadowed value both purged");
        assert!(s.get("users", "b").unwrap().is_some());
        s.shutdown().unwrap();
        fs::remove_dir_all(dir).unwrap();
    }
}
