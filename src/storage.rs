//! Per-node local storage: an append-only commit log + in-memory table, periodically
//! compacted into a snapshot file. A simplified take on Cassandra's
//! commitlog/memtable/SSTable design, without the SSTables.
//!
//! On disk (in the node's data directory):
//!   snapshot.json   full state as of the last compaction
//!   commitlog.jsonl one JSON `Mutation` per line, written since the last compaction
//!
//! Recovery = load snapshot, replay commit log (LWW makes replay idempotent).

use crate::model::{now_micros, Mutation, Record};
use crate::query;
use parking_lot::Mutex;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use tracing::{info, warn};

const SNAPSHOT: &str = "snapshot.json";
const SNAPSHOT_TMP: &str = "snapshot.json.tmp";
const LOG: &str = "commitlog.jsonl";

type Table = BTreeMap<String, Record>;

pub struct Storage {
    dir: PathBuf,
    inner: Mutex<Inner>,
    compact_every: usize,
    gc_grace_micros: u64,
    fsync: bool,
}

struct Inner {
    data: HashMap<String, Table>,
    log: BufWriter<File>,
    since_snapshot: usize,
}

fn open_log(path: &Path, truncate: bool) -> io::Result<BufWriter<File>> {
    let f = OpenOptions::new()
        .create(true)
        .write(true)
        .append(!truncate)
        .truncate(truncate)
        .open(path)?;
    Ok(BufWriter::new(f))
}

fn supersedes_existing(data: &HashMap<String, Table>, m: &Mutation) -> bool {
    match data.get(&m.c).and_then(|t| t.get(&m.k)) {
        Some(existing) => m.r.supersedes(existing),
        None => true,
    }
}

fn apply_mem(data: &mut HashMap<String, Table>, m: Mutation) {
    data.entry(m.c).or_default().insert(m.k, m.r);
}

impl Storage {
    pub fn open(dir: &Path, compact_every: usize, gc_grace_secs: u64, fsync: bool) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let snap = dir.join(SNAPSHOT);
        let mut data: HashMap<String, Table> = if snap.exists() {
            serde_json::from_reader(BufReader::new(File::open(&snap)?))?
        } else {
            HashMap::new()
        };

        let log_path = dir.join(LOG);
        let mut replayed = 0usize;
        if log_path.exists() {
            for (i, line) in BufReader::new(File::open(&log_path)?).lines().enumerate() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                match serde_json::from_str::<Mutation>(&line) {
                    Ok(m) => {
                        if supersedes_existing(&data, &m) {
                            apply_mem(&mut data, m);
                        }
                        replayed += 1;
                    }
                    Err(e) => {
                        // Most likely a torn write at the tail from a crash.
                        warn!("commit log line {}: {e}; stopping replay", i + 1);
                        break;
                    }
                }
            }
        }

        let storage = Storage {
            dir: dir.to_path_buf(),
            inner: Mutex::new(Inner {
                data,
                log: open_log(&log_path, false)?,
                since_snapshot: replayed,
            }),
            compact_every: compact_every.max(1),
            gc_grace_micros: gc_grace_secs.saturating_mul(1_000_000),
            fsync,
        };
        {
            let mut g = storage.inner.lock();
            storage.compact_locked(&mut g)?;
            let docs: usize = g.data.values().map(|t| t.len()).sum();
            info!(dir = %dir.display(), replayed, records = docs, "storage opened");
        }
        Ok(storage)
    }

    /// Write a fresh snapshot (dropping expired tombstones) and truncate the commit log.
    fn compact_locked(&self, g: &mut Inner) -> io::Result<()> {
        g.log.flush()?;
        let cutoff = now_micros().saturating_sub(self.gc_grace_micros);
        for t in g.data.values_mut() {
            t.retain(|_, r| !(r.is_tombstone() && r.ts < cutoff));
        }
        g.data.retain(|_, t| !t.is_empty());

        let tmp = self.dir.join(SNAPSHOT_TMP);
        {
            let mut w = BufWriter::new(File::create(&tmp)?);
            serde_json::to_writer(&mut w, &g.data)?;
            w.flush()?;
            w.get_ref().sync_all()?;
        }
        fs::rename(&tmp, self.dir.join(SNAPSHOT))?;
        // Only truncate once the snapshot is durable; a crash in between just
        // means replaying entries already in the snapshot, which is harmless.
        g.log = open_log(&self.dir.join(LOG), true)?;
        g.since_snapshot = 0;
        Ok(())
    }

    /// Apply mutations with last-write-wins. Returns how many actually changed state.
    pub fn apply_batch(&self, muts: Vec<Mutation>) -> io::Result<usize> {
        let mut g = self.inner.lock();
        let mut applied = 0;
        for m in muts {
            if !supersedes_existing(&g.data, &m) {
                continue;
            }
            serde_json::to_writer(&mut g.log, &m)?;
            g.log.write_all(b"\n")?;
            apply_mem(&mut g.data, m);
            applied += 1;
        }
        if applied > 0 {
            g.log.flush()?;
            if self.fsync {
                g.log.get_ref().sync_data()?;
            }
            g.since_snapshot += applied;
            if g.since_snapshot >= self.compact_every {
                self.compact_locked(&mut g)?;
            }
        }
        Ok(applied)
    }

    pub fn get(&self, c: &str, k: &str) -> Option<Record> {
        self.inner.lock().data.get(c).and_then(|t| t.get(k)).cloned()
    }

    /// IDs of live (non-tombstone) local documents matching `filter`.
    pub fn scan_ids(&self, c: &str, filter: &Value) -> Vec<String> {
        let g = self.inner.lock();
        let Some(t) = g.data.get(c) else { return vec![] };
        t.iter()
            .filter(|(_, r)| r.value.as_ref().is_some_and(|v| query::matches(v, filter)))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Local records (including tombstones) for the given ids.
    pub fn multiget(&self, c: &str, ids: &[String]) -> Vec<(String, Record)> {
        let g = self.inner.lock();
        let Some(t) = g.data.get(c) else { return vec![] };
        ids.iter()
            .filter_map(|k| t.get(k).map(|r| (k.clone(), r.clone())))
            .collect()
    }

    /// Live document count per collection on this node (replicas included).
    pub fn stats(&self) -> BTreeMap<String, usize> {
        let g = self.inner.lock();
        g.data
            .iter()
            .map(|(c, t)| (c.clone(), t.values().filter(|r| !r.is_tombstone()).count()))
            .filter(|(_, n)| *n > 0)
            .collect()
    }

    pub fn flush(&self) -> io::Result<()> {
        let mut g = self.inner.lock();
        self.compact_locked(&mut g)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn tmpdir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("cassa-test-{name}-{}", now_micros()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    fn mutation(k: &str, ts: u64, v: Option<Value>) -> Mutation {
        Mutation { c: "users".into(), k: k.into(), r: Record { ts, value: v } }
    }

    #[test]
    fn lww_and_recovery() {
        let dir = tmpdir("recovery");
        {
            let t = now_micros();
            let s = Storage::open(&dir, 1000, 3600, false).unwrap();
            assert_eq!(s.apply_batch(vec![mutation("a", t + 10, Some(json!({"n": 1})))]).unwrap(), 1);
            assert_eq!(s.apply_batch(vec![mutation("a", t + 5, Some(json!({"n": 0})))]).unwrap(), 0, "older write ignored");
            s.apply_batch(vec![mutation("b", t + 10, Some(json!({"n": 2}))), mutation("b", t + 11, None)]).unwrap();
            // no flush: rely on commit log replay
        }
        let s = Storage::open(&dir, 1000, 3600, false).unwrap();
        assert_eq!(s.get("users", "a").unwrap().value, Some(json!({"n": 1})));
        assert!(s.get("users", "b").unwrap().is_tombstone());
        assert_eq!(s.scan_ids("users", &json!({})), vec!["a".to_string()]);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn compaction_and_tombstone_gc() {
        let dir = tmpdir("compact");
        let s = Storage::open(&dir, 2, 0, false).unwrap();
        s.apply_batch(vec![mutation("a", now_micros() - 10, None)]).unwrap();
        s.apply_batch(vec![mutation("b", 1, Some(json!({})))]).unwrap(); // triggers compaction
        assert!(s.get("users", "a").is_none(), "expired tombstone purged");
        assert!(s.get("users", "b").is_some());
        assert_eq!(fs::metadata(dir.join(LOG)).unwrap().len(), 0);
        fs::remove_dir_all(&dir).unwrap();
    }
}
