//! Gossip-based cluster membership and failure detection.
//!
//! Every node keeps an `EndpointState` for each node it knows about. Once per
//! interval a node bumps its own heartbeat and does a push/pull exchange of the
//! whole state map with:
//!   * one random live peer,
//!   * sometimes one random unreachable peer (so recoveries are noticed),
//!   * sometimes a seed (so partitions heal and new nodes get introduced).
//!
//! States are versioned by `(generation, heartbeat)`; the higher version wins.
//! `generation` is the node's start time, so a restarted node supersedes its old state.
//!
//! Failure detection is timeout-based: a peer is considered down if we haven't
//! seen its heartbeat advance (directly or second-hand) within `failure_timeout`.
//! (Cassandra uses a phi-accrual detector; a fixed timeout keeps this readable.)

use crate::model::rand_below;
use crate::ring::{self, Ring};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EndpointState {
    pub node_id: String,
    pub addr: String,
    pub generation: u64,
    pub heartbeat: u64,
    pub tokens: Vec<u64>,
}

impl EndpointState {
    fn version(&self) -> (u64, u64) {
        (self.generation, self.heartbeat)
    }
}

#[derive(Serialize, Deserialize)]
pub struct GossipMsg {
    pub from: String,
    pub states: Vec<EndpointState>,
}

pub struct Gossiper {
    pub self_id: String,
    self_addr: String,
    seeds: Vec<String>,
    interval: Duration,
    failure_timeout: Duration,
    states: RwLock<HashMap<String, EndpointState>>,
    /// When we last saw each peer's version advance.
    last_seen: RwLock<HashMap<String, Instant>>,
    ring: RwLock<Arc<Ring>>,
    /// Last reported liveness, used only to log UP/DOWN transitions.
    reported: Mutex<HashMap<String, bool>>,
    client: reqwest::Client,
}

impl Gossiper {
    pub fn new(
        self_id: String,
        self_addr: String,
        seeds: Vec<String>,
        vnodes: usize,
        interval: Duration,
        failure_timeout: Duration,
        client: reqwest::Client,
    ) -> Arc<Self> {
        let generation = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let me = EndpointState {
            node_id: self_id.clone(),
            addr: self_addr.clone(),
            generation,
            heartbeat: 0,
            tokens: ring::tokens_for(&self_id, vnodes),
        };
        let g = Arc::new(Gossiper {
            self_id: self_id.clone(),
            self_addr,
            seeds,
            interval,
            failure_timeout,
            states: RwLock::new(HashMap::from([(self_id, me)])),
            last_seen: RwLock::new(HashMap::new()),
            ring: RwLock::new(Arc::new(Ring::default())),
            reported: Mutex::new(HashMap::new()),
            client,
        });
        g.rebuild_ring();
        g
    }

    pub fn ring(&self) -> Arc<Ring> {
        self.ring.read().clone()
    }

    pub fn is_alive(&self, node_id: &str) -> bool {
        node_id == self.self_id
            || self
                .last_seen
                .read()
                .get(node_id)
                .is_some_and(|t| t.elapsed() < self.failure_timeout)
    }

    pub fn addr_of(&self, node_id: &str) -> Option<String> {
        self.states.read().get(node_id).map(|s| s.addr.clone())
    }

    pub fn members(&self) -> Vec<EndpointState> {
        let mut v: Vec<_> = self.states.read().values().cloned().collect();
        v.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        v
    }

    pub fn live_members(&self) -> Vec<String> {
        self.members()
            .into_iter()
            .map(|s| s.node_id)
            .filter(|id| self.is_alive(id))
            .collect()
    }

    pub fn seconds_since_seen(&self, node_id: &str) -> Option<f64> {
        if node_id == self.self_id {
            return Some(0.0);
        }
        self.last_seen.read().get(node_id).map(|t| t.elapsed().as_secs_f64())
    }

    pub fn ownership(&self) -> BTreeMap<String, f64> {
        self.ring().ownership()
    }

    pub fn message(&self) -> GossipMsg {
        GossipMsg {
            from: self.self_id.clone(),
            states: self.states.read().values().cloned().collect(),
        }
    }

    /// Merge remote states into ours, keeping the newest version of each.
    pub fn merge(&self, incoming: Vec<EndpointState>) {
        let mut ring_changed = false;
        {
            let mut states = self.states.write();
            let mut seen = self.last_seen.write();
            for s in incoming {
                if s.node_id == self.self_id {
                    continue; // we are the authority on ourselves
                }
                match states.get(&s.node_id) {
                    Some(cur) if cur.version() >= s.version() => continue,
                    Some(cur) => {
                        if cur.generation != s.generation {
                            info!(node = %s.node_id, "node restarted (generation {} -> {})", cur.generation, s.generation);
                        }
                        ring_changed |= cur.tokens != s.tokens || cur.addr != s.addr;
                    }
                    None => {
                        info!(node = %s.node_id, addr = %s.addr, "discovered new node");
                        ring_changed = true;
                    }
                }
                seen.insert(s.node_id.clone(), Instant::now());
                states.insert(s.node_id.clone(), s);
            }
        }
        if ring_changed {
            self.rebuild_ring();
        }
    }

    fn rebuild_ring(&self) {
        // Down nodes stay in the ring (they still own their ranges, as in Cassandra);
        // requests to them fail over to hints / other replicas.
        let ring = Ring::new(
            self.states
                .read()
                .values()
                .map(|s| (s.node_id.clone(), s.tokens.clone())),
        );
        *self.ring.write() = Arc::new(ring);
    }

    fn log_transitions(&self) {
        let mut reported = self.reported.lock();
        for s in self.members() {
            if s.node_id == self.self_id {
                continue;
            }
            let alive = self.is_alive(&s.node_id);
            let prev = reported.insert(s.node_id.clone(), alive);
            if prev != Some(alive) {
                if alive {
                    info!(node = %s.node_id, "node is UP");
                } else {
                    warn!(node = %s.node_id, "node is DOWN");
                }
            }
        }
    }

    pub fn spawn(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let me = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(me.interval);
            loop {
                tick.tick().await;
                me.round().await;
            }
        })
    }

    async fn round(&self) {
        if let Some(me) = self.states.write().get_mut(&self.self_id) {
            me.heartbeat += 1;
        }
        self.log_transitions();

        let (live, down): (Vec<EndpointState>, Vec<EndpointState>) = self
            .members()
            .into_iter()
            .filter(|s| s.node_id != self.self_id)
            .partition(|s| self.is_alive(&s.node_id));

        let mut targets: Vec<String> = Vec::new();
        if !live.is_empty() {
            targets.push(live[rand_below(live.len())].addr.clone());
        }
        if !down.is_empty() && rand_below(live.len() + 1) == 0 {
            targets.push(down[rand_below(down.len())].addr.clone());
        }
        let seeds: Vec<&String> = self.seeds.iter().filter(|s| **s != self.self_addr).collect();
        if !seeds.is_empty() && (live.is_empty() || rand_below(live.len() + 1) == 0) {
            targets.push(seeds[rand_below(seeds.len())].clone());
        }
        targets.sort();
        targets.dedup();

        futures::future::join_all(targets.iter().map(|addr| self.exchange(addr))).await;
    }

    async fn exchange(&self, addr: &str) {
        let res = self
            .client
            .post(format!("http://{addr}/internal/gossip"))
            .timeout(self.interval.max(Duration::from_millis(500)))
            .json(&self.message())
            .send()
            .await
            .and_then(|r| r.error_for_status());
        match res {
            Ok(resp) => match resp.json::<GossipMsg>().await {
                Ok(msg) => self.merge(msg.states),
                Err(e) => debug!(%addr, "bad gossip reply: {e}"),
            },
            Err(e) => debug!(%addr, "gossip failed: {e}"),
        }
    }
}
