//! Response types for cluster and index information.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Consistency level: how many of a document's replicas must answer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Consistency {
    /// One replica. Fastest; may read stale data.
    One,
    /// A majority of replicas (the default). Quorum writes + quorum reads see each other.
    #[default]
    Quorum,
    /// Every replica. Fails if any replica is down.
    All,
}

impl Consistency {
    pub fn as_str(self) -> &'static str {
        match self {
            Consistency::One => "one",
            Consistency::Quorum => "quorum",
            Consistency::All => "all",
        }
    }
}

/// The cluster as seen by the node that answered.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ClusterInfo {
    pub coordinator: String,
    pub replication_factor: usize,
    #[serde(default)]
    pub pending_range_movements: bool,
    pub members: Vec<Member>,
    /// Writes queued for unreachable nodes, by node id.
    #[serde(default)]
    pub pending_hints: BTreeMap<String, usize>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Member {
    pub node_id: String,
    pub addr: String,
    /// `"UP"` or `"DOWN"`.
    pub status: String,
    /// `"JOINING"`, `"NORMAL"`, `"LEAVING"` or `"LEFT"`.
    pub state: String,
    pub generation: u64,
    pub heartbeat: u64,
    pub last_seen_secs_ago: Option<f64>,
    pub vnodes: usize,
    /// Share of the token ring this node is primary for, in percent.
    pub primary_ownership_pct: Option<f64>,
}

impl Member {
    pub fn is_up(&self) -> bool {
        self.status == "UP"
    }
}

/// A secondary index and its build state on the node that answered.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct IndexInfo {
    pub field: String,
    pub created_ts: u64,
    /// `"ready"` (used by queries) or `"building"`.
    pub state: String,
}

impl IndexInfo {
    pub fn is_ready(&self) -> bool {
        self.state == "ready"
    }
}

/// How one node answered a `find`, from [`crate::Find::explain`].
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct NodePlan {
    /// The index used, or `None` for a full scan.
    pub index: Option<String>,
    /// Documents (full scan) or index entries examined.
    pub examined: usize,
    pub matched: usize,
}

/// A replica of a document, from [`crate::Collection::owners`].
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Replica {
    pub node_id: String,
    pub alive: bool,
}

/// Basic information about a node, from [`crate::Client::ping`].
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub version: String,
    pub state: String,
    pub replication_factor: usize,
}
