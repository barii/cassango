//! Range movements: moving data when nodes join or leave the cluster.
//!
//! Everything is built from one primitive, **pull**: a node copies a set of token
//! ranges from the live replicas that currently hold them. It pages through each
//! source's data with `/internal/stream` and applies the entries locally.
//! Last-write-wins makes it safe to pull the same range from several replicas, and
//! the newest version always wins.
//!
//! * **Bootstrap** (a node starting with an empty data directory). It gossips as
//!   JOINING, so it becomes a *pending* replica for its future ranges and receives
//!   new writes for them. It waits `ring_delay` for every node to hear that, pulls
//!   the ranges it will own, then switches to NORMAL.
//! * **Decommission** (`POST /admin/decommission` on the node that should leave).
//!   It gossips as LEAVING, so the future owners of its ranges become pending
//!   replicas. Each future owner pulls its new ranges, then the node goes LEFT.
//! * **Removenode** (`POST /admin/removenode/<id>` on any node, for a node that is
//!   dead and gone). The dead node is marked LEAVING on its behalf, new owners pull
//!   from the surviving replicas, then it is marked LEFT.
//! * **Cleanup** (`POST /admin/cleanup`). After a join, the old owners still hold
//!   data they no longer own. A cleanup compaction drops it.
//!
//! The ranges that move are found by comparing the current ring (NORMAL + LEAVING)
//! with the future ring (NORMAL + JOINING) over `ring::movement_ranges`.

use crate::coordinator::Node;
use crate::gossip::NodeStatus;
use crate::model::{DbError, Mutation};
use crate::ring::{hash64, movement_ranges, Ring, TokenRange};
use crate::storage::{blocking, split_key, KeyFilter, Storage};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

const PAGE_LIMIT: usize = 1000;
/// Bound the work one page does even when few keys match the requested ranges.
const PAGE_MAX_EXAMINED: usize = 50_000;
const PAGE_TIMEOUT: Duration = Duration::from_secs(60);
const PULL_TIMEOUT: Duration = Duration::from_secs(6 * 3600);
const MARKER: &str = "bootstrapped";

/// A plan item: a range to copy and the replicas to copy it from.
pub type PlanItem = (TokenRange, Vec<String>);

#[derive(Serialize, Deserialize)]
pub struct StreamReq {
    pub ranges: Vec<TokenRange>,
    /// Resume after this composite key (exclusive).
    pub after: Option<Vec<u8>>,
    pub limit: usize,
}

#[derive(Serialize, Deserialize)]
pub struct StreamPage {
    pub entries: Vec<Mutation>,
    /// Cursor for the next page; `None` when the scan is complete.
    pub next: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
pub struct PullReq {
    pub plan: Vec<PlanItem>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct PullReport {
    pub node_id: String,
    pub ranges: usize,
    pub entries: usize,
    /// Entries received from each source.
    pub from: BTreeMap<String, usize>,
    /// Ranges for which no replica was alive to copy from (repair them later).
    pub ranges_without_source: usize,
    pub duration_ms: u64,
}

#[derive(Serialize, Debug)]
pub struct MoveReport {
    pub operation: &'static str,
    pub node: String,
    pub pulls: Vec<PullReport>,
    pub entries_streamed: usize,
    pub duration_ms: u64,
}

// ------------------------------------------------------------------ range lookup

/// Disjoint token ranges with O(log n) lookup.
pub struct RangeIndex {
    ranges: Vec<TokenRange>, // sorted by end
}

impl RangeIndex {
    pub fn new(mut ranges: Vec<TokenRange>) -> Self {
        ranges.sort_by_key(|r| r.end);
        RangeIndex { ranges }
    }

    pub fn contains(&self, token: u64) -> bool {
        if self.ranges.is_empty() {
            return false;
        }
        // The only candidate is the first range ending at or after the token; past the
        // last end we wrap to the first range, which is the wrapping one if present.
        let i = self.ranges.partition_point(|r| r.end < token) % self.ranges.len();
        self.ranges[i].contains(token)
    }
}

// ------------------------------------------------------------------ source side

/// Serve one page of all local entries (tombstones included) in `req.ranges`.
pub fn stream_page(storage: &Storage, req: &StreamReq) -> io::Result<StreamPage> {
    let index = RangeIndex::new(req.ranges.clone());
    let limit = req.limit.clamp(1, 10_000);
    let start = match &req.after {
        Some(a) => {
            let mut s = a.clone();
            s.push(0); // smallest key strictly greater than `after`
            s
        }
        None => Vec::new(),
    };
    let (mut entries, mut examined, mut last, mut more) = (Vec::new(), 0usize, None, false);
    storage.scan_from(&start, |key, rec| {
        if entries.len() >= limit || examined >= PAGE_MAX_EXAMINED {
            more = true;
            return false;
        }
        examined += 1;
        last = Some(key.to_vec());
        if index.contains(hash64(key)) {
            let (c, k) = split_key(key);
            entries.push(Mutation {
                c: String::from_utf8_lossy(c).into_owned(),
                k: String::from_utf8_lossy(k).into_owned(),
                r: rec.clone(),
            });
        }
        true
    })?;
    Ok(StreamPage { entries, next: if more { last } else { None } })
}

// ------------------------------------------------------------------ planning

/// For every range whose replica set differs between `cur` and `fut`, the nodes that
/// gain it, each mapped to (range, live old replicas to copy from).
fn plan_transfers(node: &Node, cur: &Ring, fut: &Ring, exclude_sources: &[&str]) -> BTreeMap<String, Vec<PlanItem>> {
    let mut plan: BTreeMap<String, Vec<PlanItem>> = BTreeMap::new();
    for r in movement_ranges(cur, fut) {
        let old = cur.replicas(r.end, node.rf);
        let new = fut.replicas(r.end, node.rf);
        let sources: Vec<String> = old
            .iter()
            .filter(|n| node.gossip.is_alive(n) && !exclude_sources.contains(&n.as_str()))
            .cloned()
            .collect();
        for gainer in new.iter().filter(|n| !old.contains(n)) {
            plan.entry(gainer.clone()).or_default().push((r, sources.clone()));
        }
    }
    plan
}

pub fn is_bootstrapped(data_dir: &Path) -> bool {
    data_dir.join(MARKER).exists()
}

pub fn mark_bootstrapped(data_dir: &Path) -> io::Result<()> {
    std::fs::write(data_dir.join(MARKER), b"")
}

// ------------------------------------------------------------------ operations

impl Node {
    /// Copy the given ranges from their sources into local storage.
    pub async fn pull_ranges(self: &Arc<Self>, plan: Vec<PlanItem>) -> Result<PullReport, String> {
        let started = Instant::now();
        let mut report = PullReport { node_id: self.id.clone(), ranges: plan.len(), ..Default::default() };
        let mut by_source: BTreeMap<String, Vec<TokenRange>> = BTreeMap::new();
        for (range, sources) in &plan {
            let live: Vec<&String> = sources.iter().filter(|s| **s != self.id && self.gossip.is_alive(s)).collect();
            if live.is_empty() && !sources.contains(&self.id) {
                report.ranges_without_source += 1;
                warn!(start = range.start, end = range.end, "no live replica to stream range from; repair it later");
            }
            for s in live {
                by_source.entry(s.clone()).or_default().push(*range);
            }
        }
        let results = futures::future::join_all(
            by_source
                .into_iter()
                .map(|(src, ranges)| async move { (src.clone(), self.stream_from(&src, ranges).await) }),
        )
        .await;
        for (src, res) in results {
            let n = res.map_err(|e| format!("streaming from {src} failed: {e}"))?;
            report.entries += n;
            report.from.insert(src, n);
        }
        report.duration_ms = started.elapsed().as_millis() as u64;
        info!(ranges = report.ranges, entries = report.entries, from = ?report.from, ms = report.duration_ms, "pulled ranges");
        Ok(report)
    }

    async fn stream_from(self: &Arc<Self>, src: &str, ranges: Vec<TokenRange>) -> Result<usize, String> {
        let (mut after, mut total) = (None, 0usize);
        loop {
            let req = StreamReq { ranges: ranges.clone(), after: after.take(), limit: PAGE_LIMIT };
            let page: StreamPage = self.rpc_timeout(src, "/internal/stream", &req, Some(PAGE_TIMEOUT)).await?;
            total += page.entries.len();
            if !page.entries.is_empty() {
                let (st, entries) = (self.storage.clone(), page.entries);
                blocking(move || st.apply_batch(entries)).await?;
            }
            match page.next {
                Some(k) => after = Some(k),
                None => return Ok(total),
            }
        }
    }

    async fn request_pull(self: &Arc<Self>, node: &str, plan: Vec<PlanItem>) -> Result<PullReport, String> {
        if node == self.id {
            self.pull_ranges(plan).await
        } else {
            self.rpc_timeout(node, "/internal/pull", &PullReq { plan }, Some(PULL_TIMEOUT)).await
        }
    }

    async fn run_pulls(self: &Arc<Self>, plan: BTreeMap<String, Vec<PlanItem>>) -> Result<Vec<PullReport>, String> {
        if let Some(dead) = plan.keys().find(|n| !self.gossip.is_alive(n)) {
            return Err(format!("{dead} must receive data but is down"));
        }
        let results =
            futures::future::join_all(plan.into_iter().map(|(n, items)| async move { (n.clone(), self.request_pull(&n, items).await) }))
                .await;
        results
            .into_iter()
            .map(|(n, r)| r.map_err(|e| format!("{n}: {e}")))
            .collect()
    }

    // ---------------------------------------------------------------- bootstrap

    /// Join the cluster in the background, retrying until it succeeds.
    pub fn spawn_bootstrap(self: &Arc<Self>) {
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                match me.bootstrap().await {
                    Ok(()) => return,
                    Err(e) => {
                        warn!("bootstrap failed: {e}; retrying in 5s");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        });
    }

    async fn bootstrap(self: &Arc<Self>) -> Result<(), String> {
        let _guard = self.movement_lock.lock().await;
        let has_normal_peer = || {
            self.gossip
                .members()
                .iter()
                .any(|s| s.node_id != self.id && s.status == NodeStatus::Normal)
        };
        // 1. Find out whether there is an existing cluster to join.
        let deadline = Instant::now() + self.ring_delay;
        while Instant::now() < deadline && !has_normal_peer() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if !has_normal_peer() {
            info!("no existing cluster found; starting as a new cluster member without streaming");
            return self.finish_bootstrap();
        }

        // 2. Give every node time to learn we're JOINING, so writes for our future
        //    ranges already reach us before we snapshot the existing data.
        info!(delay_ms = self.ring_delay.as_millis() as u64, "joining existing cluster; waiting for the ring to settle");
        tokio::time::sleep(self.ring_delay).await;

        // 3. Stream our future ranges from their current replicas.
        let (cur, fut) = (self.gossip.ring(), self.gossip.future_ring());
        let plan = plan_transfers(self, &cur, &fut, &[]).remove(&self.id).unwrap_or_default();
        let report = self.pull_ranges(plan).await?;
        info!(entries = report.entries, ranges = report.ranges, "bootstrap streaming complete");
        self.finish_bootstrap()
    }

    fn finish_bootstrap(&self) -> Result<(), String> {
        mark_bootstrapped(&self.data_dir).map_err(|e| e.to_string())?;
        self.gossip.set_status(NodeStatus::Normal);
        info!("node is NORMAL");
        Ok(())
    }

    // ---------------------------------------------------------------- decommission

    pub async fn decommission(self: &Arc<Self>) -> Result<MoveReport, DbError> {
        let Ok(_guard) = self.movement_lock.try_lock() else {
            return Err(DbError::Conflict("a range movement is already running on this node".into()));
        };
        let status = self.gossip.my_status();
        if status != NodeStatus::Normal {
            return Err(DbError::Conflict(format!("node is {status:?}, not NORMAL")));
        }
        if self.gossip.has_pending_movements() {
            return Err(DbError::Conflict("another node is joining or leaving; try again when it's done".into()));
        }
        let normal = self.gossip.members().iter().filter(|s| s.status == NodeStatus::Normal).count();
        if normal <= 1 {
            return Err(DbError::Conflict("cannot decommission the last node".into()));
        }
        let started = Instant::now();
        let cur = self.gossip.ring();
        info!("decommissioning: LEAVING");
        self.gossip.set_status(NodeStatus::Leaving);
        tokio::time::sleep(self.ring_delay).await;

        let plan = plan_transfers(self, &cur, &self.gossip.future_ring(), &[]);
        let pulls = match self.run_pulls(plan).await {
            Ok(p) => p,
            Err(e) => {
                warn!("decommission aborted: {e}");
                self.gossip.set_status(NodeStatus::Normal);
                return Err(DbError::Internal(format!("decommission aborted, node is NORMAL again: {e}")));
            }
        };
        self.gossip.set_status(NodeStatus::Left);
        info!("decommission complete: LEFT; this node can be shut down");
        tokio::time::sleep(self.ring_delay).await; // let LEFT spread before shutdown
        Ok(MoveReport {
            operation: "decommission",
            node: self.id.clone(),
            entries_streamed: pulls.iter().map(|p| p.entries).sum(),
            pulls,
            duration_ms: started.elapsed().as_millis() as u64,
        })
    }

    // ---------------------------------------------------------------- removenode

    pub async fn removenode(self: &Arc<Self>, dead: &str) -> Result<MoveReport, DbError> {
        let Ok(_guard) = self.movement_lock.try_lock() else {
            return Err(DbError::Conflict("a range movement is already running on this node".into()));
        };
        if dead == self.id {
            return Err(DbError::BadRequest("use /admin/decommission to remove this node".into()));
        }
        match self.gossip.status_of(dead) {
            None => return Err(DbError::NotFound),
            Some(NodeStatus::Normal | NodeStatus::Leaving) => {}
            Some(s) => return Err(DbError::Conflict(format!("{dead} is {s:?}; only NORMAL or LEAVING nodes can be removed"))),
        }
        if self.gossip.is_alive(dead) {
            return Err(DbError::Conflict(format!("{dead} is alive; run /admin/decommission on it instead")));
        }
        let others_moving = self.gossip.members().iter().any(|s| {
            s.node_id != dead && matches!(s.status, NodeStatus::Joining | NodeStatus::Leaving)
        });
        if others_moving {
            return Err(DbError::Conflict("another node is joining or leaving; try again when it's done".into()));
        }
        let started = Instant::now();
        // The current ring still contains the dead node, even if a previous attempt left it LEAVING.
        let cur = self.gossip.ring();
        info!(node = %dead, "removenode: marking LEAVING");
        self.gossip.set_status_of(dead, NodeStatus::Leaving);
        tokio::time::sleep(self.ring_delay).await;

        let plan = plan_transfers(self, &cur, &self.gossip.future_ring(), &[dead]);
        let pulls = match self.run_pulls(plan).await {
            Ok(p) => p,
            Err(e) => {
                warn!(node = %dead, "removenode aborted: {e}");
                return Err(DbError::Internal(format!("removenode aborted ({dead} stays LEAVING; retry): {e}")));
            }
        };
        self.gossip.set_status_of(dead, NodeStatus::Left);
        info!(node = %dead, "removenode complete: LEFT");
        Ok(MoveReport {
            operation: "removenode",
            node: dead.to_string(),
            entries_streamed: pulls.iter().map(|p| p.entries).sum(),
            pulls,
            duration_ms: started.elapsed().as_millis() as u64,
        })
    }

    // ---------------------------------------------------------------- cleanup

    /// Drop local data for token ranges this node neither owns nor is about to own.
    pub async fn cleanup(self: &Arc<Self>) -> Result<u64, DbError> {
        let status = self.gossip.my_status();
        if status != NodeStatus::Normal {
            return Err(DbError::Conflict(format!("node is {status:?}, not NORMAL")));
        }
        // While a range movement is in flight (as far as this node knows), ownership
        // is about to change; cleaning up now would either keep too much or drop data
        // that's still needed. Cassandra has the same rule.
        if self.gossip.has_pending_movements() {
            return Err(DbError::Conflict("a node is joining or leaving; run cleanup after it finishes".into()));
        }
        let (cur, fut, rf, me) = (self.gossip.ring(), self.gossip.future_ring(), self.rf, self.id.clone());
        let keep: KeyFilter = Arc::new(move |key: &[u8]| {
            let t = hash64(&crate::storage::data_key_for(key)); // index entries follow their document
            cur.replicas(t, rf).contains(&me) || fut.replicas(t, rf).contains(&me)
        });
        let st = self.storage.clone();
        let dropped = blocking(move || st.cleanup(keep)).await.map_err(DbError::Internal)?;
        info!(dropped, "cleanup complete");
        Ok(dropped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_index_lookup_with_wraparound() {
        let idx = RangeIndex::new(vec![
            TokenRange { start: 100, end: 200 },
            TokenRange { start: u64::MAX - 5, end: 10 },
            TokenRange { start: 500, end: 600 },
        ]);
        for (t, want) in [(150, true), (100, false), (200, true), (201, false), (0, true), (u64::MAX, true), (10, true), (11, false), (550, true), (700, false)] {
            assert_eq!(idx.contains(t), want, "token {t}");
        }
        assert!(!RangeIndex::new(vec![]).contains(5));
        assert!(RangeIndex::new(vec![TokenRange { start: 7, end: 7 }]).contains(12345), "full ring");
    }
}
