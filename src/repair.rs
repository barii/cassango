//! Anti-entropy repair with Merkle trees (Cassandra's `nodetool repair -pr`).
//!
//! Hinted handoff and read repair fix most divergence, but not all of it: hints
//! are lost when a coordinator restarts or a queue overflows, a node can lose its
//! disk, and read repair only touches keys that get read. Anti-entropy repair
//! compares replicas wholesale and fixes everything.
//!
//! A repair session run on node N, for each token range where N is the *primary*
//! owner (so running it on every node covers the ring exactly once):
//!
//!  1. **Validation.** Every replica of the range (N included) scans its local data
//!     and builds a Merkle tree over it. The range is split evenly into 2^depth
//!     leaves by token. A leaf's hash is the XOR of the hashes of its
//!     `(key, timestamp, value)` entries, so it doesn't depend on order. Inner
//!     nodes hash their two children.
//!  2. **Comparison.** Each replica's tree is compared with N's from the root down.
//!     Matching subtrees are skipped, so only differing leaves come out.
//!  3. **Streaming.** N fetches every replica's entries (tombstones included) in the
//!     differing leaves. It works out the newest version of each key by
//!     last-write-wins, and sends each replica exactly the entries it is missing or
//!     has stale.
//!
//! Each replica does one full local scan to build trees and at most one more to
//! serve fetches, however many ranges are being repaired.
//!
//! As in Cassandra, run repair on every node more often than `gc_grace_secs`.
//! Otherwise a replica that missed a delete can bring the document back once the
//! tombstone has been purged elsewhere.

use crate::coordinator::Node;
use crate::model::{DbError, Mutation, Record};
use crate::ring::{hash64, TokenRange};
use crate::storage::{blocking, split_key, Storage};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

pub const DEFAULT_DEPTH: u32 = 10;
const MAX_DEPTH: u32 = 16;
const STREAM_BATCH: usize = 500;

// ------------------------------------------------------------------ Merkle tree

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MerkleTree {
    pub depth: u32,
    /// Heap layout: nodes[0] is the root, children of i are 2i+1 and 2i+2,
    /// leaves occupy the last 2^depth slots.
    pub nodes: Vec<u64>,
}

impl MerkleTree {
    pub fn new(depth: u32) -> Self {
        let leaves = 1usize << depth;
        MerkleTree { depth, nodes: vec![0; 2 * leaves - 1] }
    }

    pub fn leaves(&self) -> usize {
        1 << self.depth
    }

    fn add(&mut self, leaf: usize, h: u64) {
        let base = self.leaves() - 1;
        self.nodes[base + leaf] ^= h;
    }

    fn finalize(&mut self) {
        for i in (0..self.leaves() - 1).rev() {
            let (l, r) = (self.nodes[2 * i + 1], self.nodes[2 * i + 2]);
            self.nodes[i] = if l == 0 && r == 0 {
                0
            } else {
                let mut b = [0u8; 16];
                b[..8].copy_from_slice(&l.to_le_bytes());
                b[8..].copy_from_slice(&r.to_le_bytes());
                hash64(&b)
            };
        }
    }

    #[cfg(test)]
    pub fn root(&self) -> u64 {
        self.nodes[0]
    }

    /// Indices of leaves that differ, found by descending only into differing subtrees.
    pub fn diff(&self, other: &MerkleTree) -> Vec<usize> {
        let mut out = Vec::new();
        if self.depth != other.depth || self.nodes.len() != other.nodes.len() {
            return (0..self.leaves()).collect();
        }
        let base = self.leaves() - 1;
        let mut stack = vec![0usize];
        while let Some(i) = stack.pop() {
            if self.nodes[i] == other.nodes[i] {
                continue;
            }
            if i >= base {
                out.push(i - base);
            } else {
                stack.push(2 * i + 2);
                stack.push(2 * i + 1);
            }
        }
        out.sort_unstable();
        out
    }
}

fn entry_hash(key: &[u8], r: &Record) -> u64 {
    let mut b = Vec::with_capacity(key.len() + 64);
    b.extend((key.len() as u32).to_le_bytes());
    b.extend(key);
    b.extend(r.ts.to_le_bytes());
    match &r.value {
        None => b.push(0),
        Some(v) => {
            b.push(1);
            b.extend(v.to_string().as_bytes());
        }
    }
    // Never 0, so an entry can't be confused with an empty leaf.
    hash64(&b) | 1
}

/// Which of `ranges` contains `token`? Ranges are disjoint.
fn find_range(ranges: &[TokenRange], token: u64) -> Option<usize> {
    ranges.iter().position(|r| r.contains(token))
}

// ------------------------------------------------------------------ replica-side work

#[derive(Serialize, Deserialize)]
pub struct TreesReq {
    pub ranges: Vec<TokenRange>,
    pub depth: u32,
}

#[derive(Serialize, Deserialize)]
pub struct FetchReq {
    pub depth: u32,
    /// (range, differing leaf indices)
    pub items: Vec<(TokenRange, Vec<u32>)>,
}

/// Validation: one full scan of local data, building a Merkle tree per range.
pub fn build_trees(storage: &Storage, ranges: &[TokenRange], depth: u32) -> io::Result<Vec<MerkleTree>> {
    let depth = depth.min(MAX_DEPTH);
    let mut trees: Vec<MerkleTree> = ranges.iter().map(|_| MerkleTree::new(depth)).collect();
    let leaves = 1usize << depth;
    storage.for_each_entry(|key, rec| {
        let token = hash64(key); // == ring::key_token(collection, id)
        if let Some(i) = find_range(ranges, token) {
            trees[i].add(ranges[i].leaf(token, leaves), entry_hash(key, rec));
        }
    })?;
    for t in &mut trees {
        t.finalize();
    }
    Ok(trees)
}

/// Streaming: all local entries (tombstones included) that fall in the given leaves.
pub fn fetch_entries(storage: &Storage, req: &FetchReq) -> io::Result<Vec<Mutation>> {
    let leaves = 1usize << req.depth.min(MAX_DEPTH);
    let ranges: Vec<TokenRange> = req.items.iter().map(|(r, _)| *r).collect();
    let wanted: Vec<HashSet<usize>> = req.items.iter().map(|(_, l)| l.iter().map(|x| *x as usize).collect()).collect();
    let mut out = Vec::new();
    storage.for_each_entry(|key, rec| {
        let token = hash64(key);
        if let Some(i) = find_range(&ranges, token)
            && wanted[i].contains(&ranges[i].leaf(token, leaves))
        {
            let (c, k) = split_key(key);
            out.push(Mutation {
                c: String::from_utf8_lossy(c).into_owned(),
                k: String::from_utf8_lossy(k).into_owned(),
                r: rec.clone(),
            });
        }
    })?;
    Ok(out)
}

// ------------------------------------------------------------------ session

#[derive(Serialize, Clone, Debug, Default)]
pub struct RepairReport {
    pub node_id: String,
    pub started_at_micros: u64,
    pub duration_ms: u64,
    pub ranges: usize,
    pub ranges_in_sync: usize,
    pub ranges_repaired: usize,
    pub ranges_skipped: usize,
    pub mismatched_leaves: usize,
    pub keys_compared: usize,
    /// Entries streamed to each replica.
    pub streamed: BTreeMap<String, usize>,
    pub errors: Vec<String>,
}

impl Node {
    async fn remote_trees(self: &Arc<Self>, node: &str, ranges: Vec<TokenRange>, depth: u32) -> Result<Vec<MerkleTree>, String> {
        if node == self.id {
            let st = self.storage.clone();
            blocking(move || build_trees(&st, &ranges, depth)).await
        } else {
            self.rpc(node, "/internal/repair/trees", &TreesReq { ranges, depth }).await
        }
    }

    async fn remote_fetch(self: &Arc<Self>, node: &str, req: FetchReq) -> Result<Vec<Mutation>, String> {
        if node == self.id {
            let st = self.storage.clone();
            blocking(move || fetch_entries(&st, &req)).await
        } else {
            self.rpc(node, "/internal/repair/fetch", &req).await
        }
    }

    /// Run one repair session over this node's primary ranges.
    pub async fn repair(self: &Arc<Self>, depth: u32) -> Result<RepairReport, DbError> {
        let Ok(_guard) = self.repair_lock.try_lock() else {
            return Err(DbError::Conflict("a repair is already running on this node".into()));
        };
        let depth = depth.clamp(1, MAX_DEPTH);
        let started = Instant::now();
        let ring = self.gossip.ring();
        let mut report = RepairReport {
            node_id: self.id.clone(),
            started_at_micros: crate::model::now_micros(),
            ..Default::default()
        };

        // Which ranges to repair, and which replicas hold each one.
        let mut ranges: Vec<(TokenRange, Vec<String>)> = Vec::new();
        for range in ring.primary_ranges(&self.id) {
            report.ranges += 1;
            let replicas = ring.replicas(range.end, self.rf);
            if replicas.len() < 2 {
                report.ranges_in_sync += 1; // nothing to compare against
                continue;
            }
            if let Some(down) = replicas.iter().find(|n| !self.gossip.is_alive(n)) {
                report.ranges_skipped += 1;
                report.errors.push(format!("range ({}, {}] skipped: replica {down} is down", range.start, range.end));
                continue;
            }
            ranges.push((range, replicas));
        }
        if ranges.is_empty() {
            report.duration_ms = started.elapsed().as_millis() as u64;
            *self.last_repair.lock() = Some(report.clone());
            return Ok(report);
        }

        // 1. Validation: one tree request per replica covering all its shared ranges.
        let mut per_node: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for (i, (_, replicas)) in ranges.iter().enumerate() {
            for n in replicas {
                per_node.entry(n.clone()).or_default().push(i);
            }
        }
        let tree_results = futures::future::join_all(per_node.iter().map(|(n, idxs)| {
            let rs: Vec<TokenRange> = idxs.iter().map(|i| ranges[*i].0).collect();
            async move { (n.clone(), self.remote_trees(n, rs, depth).await) }
        }))
        .await;
        // trees[(node, range index)]
        let mut trees: HashMap<(String, usize), MerkleTree> = HashMap::new();
        let mut failed_nodes: HashSet<String> = HashSet::new();
        for (n, res) in tree_results {
            match res {
                Ok(ts) => {
                    for (i, t) in per_node[&n].iter().zip(ts) {
                        trees.insert((n.clone(), *i), t);
                    }
                }
                Err(e) => {
                    report.errors.push(format!("validation failed on {n}: {e}"));
                    failed_nodes.insert(n);
                }
            }
        }

        // 2. Comparison against our own trees.
        let mut fetch_plan: BTreeMap<String, Vec<(TokenRange, Vec<u32>)>> = BTreeMap::new();
        let mut repairing: Vec<usize> = Vec::new();
        for (i, (range, replicas)) in ranges.iter().enumerate() {
            if replicas.iter().any(|n| failed_nodes.contains(n)) {
                report.ranges_skipped += 1;
                continue;
            }
            let mine = &trees[&(self.id.clone(), i)];
            let mut leaves: Vec<usize> = replicas
                .iter()
                .filter(|n| **n != self.id)
                .flat_map(|n| mine.diff(&trees[&(n.clone(), i)]))
                .collect();
            leaves.sort_unstable();
            leaves.dedup();
            if leaves.is_empty() {
                report.ranges_in_sync += 1;
                continue;
            }
            report.mismatched_leaves += leaves.len();
            repairing.push(i);
            let leaves: Vec<u32> = leaves.into_iter().map(|l| l as u32).collect();
            for n in replicas {
                fetch_plan.entry(n.clone()).or_default().push((*range, leaves.clone()));
            }
        }

        // 3. Streaming: gather every replica's version of each key in the bad leaves.
        if !fetch_plan.is_empty() {
            let fetched = futures::future::join_all(fetch_plan.into_iter().map(|(n, items)| async move {
                let res = self.remote_fetch(&n, FetchReq { depth, items }).await;
                (n, res)
            }))
            .await;
            // key -> per-node record
            let mut versions: HashMap<(String, String), HashMap<String, Record>> = HashMap::new();
            let mut fetched_ok: HashSet<String> = HashSet::new();
            for (n, res) in fetched {
                match res {
                    Ok(muts) => {
                        fetched_ok.insert(n.clone());
                        for m in muts {
                            versions.entry((m.c, m.k)).or_default().insert(n.clone(), m.r);
                        }
                    }
                    Err(e) => report.errors.push(format!("fetch failed on {n}: {e}")),
                }
            }
            report.keys_compared = versions.len();

            let mut outgoing: BTreeMap<String, Vec<Mutation>> = BTreeMap::new();
            for ((c, k), per_node) in versions {
                let newest = crate::model::newest(per_node.values().map(|r| Some(r.clone())).collect::<Vec<_>>().iter())
                    .expect("at least one version");
                let replicas = ring.replicas(crate::ring::key_token(&c, &k), self.rf);
                for n in replicas.iter().filter(|n| fetched_ok.contains(*n)) {
                    if per_node.get(n).is_none_or(|r| newest.supersedes(r)) {
                        outgoing.entry(n.clone()).or_default().push(Mutation { c: c.clone(), k: k.clone(), r: newest.clone() });
                    }
                }
            }
            for (n, muts) in outgoing {
                let total = muts.len();
                let mut sent = 0;
                for chunk in muts.chunks(STREAM_BATCH) {
                    match self.send_mutations(&n, chunk.to_vec()).await {
                        Ok(()) => sent += chunk.len(),
                        Err(e) => {
                            report.errors.push(format!("streaming to {n} failed after {sent}/{total}: {e}"));
                            break;
                        }
                    }
                }
                report.streamed.insert(n, sent);
            }
            report.ranges_repaired = repairing.len();
        }

        report.duration_ms = started.elapsed().as_millis() as u64;
        let streamed: usize = report.streamed.values().sum();
        if report.errors.is_empty() {
            info!(ranges = report.ranges, in_sync = report.ranges_in_sync, repaired = report.ranges_repaired,
                  streamed, ms = report.duration_ms, "repair finished");
        } else {
            warn!(ranges = report.ranges, skipped = report.ranges_skipped, streamed, errors = ?report.errors,
                  "repair finished with errors");
        }
        *self.last_repair.lock() = Some(report.clone());
        Ok(report)
    }

    /// Periodically repair this node's primary ranges.
    pub fn spawn_repair_scheduler(self: &Arc<Self>, interval_secs: u64) {
        if interval_secs == 0 {
            return;
        }
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                // Jitter so the nodes in a cluster don't all repair at once.
                let jitter = crate::model::rand_below((interval_secs / 5).max(1) as usize) as u64;
                tokio::time::sleep(std::time::Duration::from_secs(interval_secs + jitter)).await;
                if let Err(e) = me.repair(DEFAULT_DEPTH).await {
                    warn!("scheduled repair: {e}");
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merkle_diff_finds_exactly_the_changed_leaves() {
        let mut a = MerkleTree::new(6);
        let mut b = MerkleTree::new(6);
        for leaf in 0..64 {
            a.add(leaf, 1000 + leaf as u64);
            b.add(leaf, 1000 + leaf as u64);
        }
        b.add(5, 77);
        b.add(40, 99);
        a.finalize();
        b.finalize();
        assert_ne!(a.root(), b.root());
        assert_eq!(a.diff(&b), vec![5, 40]);
        b.add(5, 77); // XOR again cancels the change
        b.add(40, 99);
        b.finalize();
        assert_eq!(a.root(), b.root());
        assert!(a.diff(&b).is_empty());
    }

    #[test]
    fn entry_hash_sensitive_to_version_and_value() {
        let r1 = Record { ts: 1, value: Some(serde_json::json!({"a": 1})) };
        let r2 = Record { ts: 2, value: Some(serde_json::json!({"a": 1})) };
        let r3 = Record { ts: 1, value: Some(serde_json::json!({"a": 2})) };
        let t = Record { ts: 1, value: None };
        let hs = [entry_hash(b"k", &r1), entry_hash(b"k", &r2), entry_hash(b"k", &r3), entry_hash(b"k", &t), entry_hash(b"j", &r1)];
        let uniq: HashSet<_> = hs.iter().collect();
        assert_eq!(uniq.len(), hs.len());
    }
}
