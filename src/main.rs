//! cassa — a small distributed JSON document database.
//!
//! Cassandra-style distribution (gossip membership, consistent-hash ring with vnodes,
//! tunable consistency, hinted handoff, read repair, LWW conflict resolution) with
//! MongoDB-style JSON documents and queries.

mod api;
mod coordinator;
mod gossip;
mod model;
mod movement;
mod query;
mod repair;
mod ring;
mod storage;

use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "cassa", version, about = "Distributed JSON document database (gossip + consistent hashing)")]
struct Args {
    /// Unique, stable node id. Determines this node's ring tokens.
    #[arg(long)]
    node_id: String,

    /// Address to listen on for client and internal traffic.
    #[arg(long, default_value = "127.0.0.1:7001")]
    listen: String,

    /// Address other nodes should use to reach this node (defaults to --listen).
    #[arg(long)]
    advertise: Option<String>,

    /// Comma-separated seed node addresses, e.g. 127.0.0.1:7001,127.0.0.1:7002
    #[arg(long, value_delimiter = ',')]
    seeds: Vec<String>,

    /// Data directory (defaults to ./data/<node-id>).
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Number of replicas for each document. Must be the same on every node.
    #[arg(long, default_value_t = 3)]
    replication_factor: usize,

    /// Virtual nodes (tokens) per node.
    #[arg(long, default_value_t = 16)]
    vnodes: usize,

    #[arg(long, default_value_t = 1000)]
    gossip_interval_ms: u64,

    /// A peer is marked DOWN if its heartbeat hasn't advanced for this long.
    #[arg(long, default_value_t = 5000)]
    failure_timeout_ms: u64,

    /// Flush the memtable to an SSTable once it holds roughly this many MiB.
    #[arg(long, default_value_t = 16)]
    memtable_mb: usize,

    /// Size-tiered compaction merges SSTables once this many have similar size.
    #[arg(long, default_value_t = 4)]
    compaction_threshold: usize,

    /// How long tombstones are kept before being purged (default 10 days).
    #[arg(long, default_value_t = 864_000)]
    gc_grace_secs: u64,

    /// Don't stream data when joining: start NORMAL immediately (Cassandra's
    /// auto_bootstrap=false). Use for the initial nodes of a new cluster, or to
    /// rejoin in place after data loss and fix it with repair.
    #[arg(long)]
    no_bootstrap: bool,

    /// How long to wait for a status change to spread through gossip before
    /// streaming begins (joins, decommission, removenode).
    #[arg(long, default_value_t = 5000)]
    ring_delay_ms: u64,

    /// Run anti-entropy repair of this node's primary ranges every N seconds
    /// (0 = only on demand via POST /admin/repair). Keep it below --gc-grace-secs.
    #[arg(long, default_value_t = 0)]
    repair_interval_secs: u64,

    /// fsync the commit log after every write batch (durable but slower).
    #[arg(long)]
    fsync: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(false).init();
    let args = Args::parse();

    let advertise = args.advertise.clone().unwrap_or_else(|| args.listen.clone());
    let data_dir = args
        .data_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("data").join(&args.node_id));

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .connect_timeout(Duration::from_millis(500))
        .build()?;

    let storage = Arc::new(storage::Storage::open(
        &data_dir,
        storage::Config {
            memtable_bytes: args.memtable_mb.max(1) << 20,
            compaction_threshold: args.compaction_threshold,
            gc_grace_secs: args.gc_grace_secs,
            fsync: args.fsync,
        },
    )?);
    // A node that has never finished joining starts as JOINING and bootstraps.
    let bootstrapped = movement::is_bootstrapped(&data_dir);
    if args.no_bootstrap && !bootstrapped {
        movement::mark_bootstrapped(&data_dir)?;
    }
    let status = if bootstrapped || args.no_bootstrap { gossip::NodeStatus::Normal } else { gossip::NodeStatus::Joining };

    let gossiper = gossip::Gossiper::new(
        args.node_id.clone(),
        advertise.clone(),
        args.seeds.clone(),
        args.vnodes,
        status,
        Duration::from_millis(args.gossip_interval_ms),
        Duration::from_millis(args.failure_timeout_ms),
        client.clone(),
    );
    let node = coordinator::Node::new(
        args.node_id.clone(),
        args.replication_factor,
        gossiper.clone(),
        storage.clone(),
        client,
        data_dir.clone(),
        Duration::from_millis(args.ring_delay_ms),
    );

    gossiper.spawn();
    node.spawn_hint_replayer();
    if status == gossip::NodeStatus::Joining {
        node.spawn_bootstrap();
    }
    node.spawn_repair_scheduler(args.repair_interval_secs);

    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    info!(node = %args.node_id, listen = %args.listen, advertise = %advertise, seeds = ?args.seeds,
          rf = args.replication_factor, state = ?status, "cassa node started");

    axum::serve(listener, api::router(node))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    info!("shutting down; flushing storage");
    storage.shutdown()?;
    Ok(())
}
