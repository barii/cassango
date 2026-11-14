//! HTTP API: the public document API plus the internal node-to-node endpoints.

use crate::coordinator::{FindRequest, Node};
use crate::gossip::GossipMsg;
use crate::model::*;
use crate::storage::blocking;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::sync::Arc;

type AppState = Arc<Node>;
type ApiResult<T> = Result<T, ApiError>;

pub fn router(node: Arc<Node>) -> Router {
    Router::new()
        // public
        .route("/", get(info))
        .route("/cluster", get(cluster))
        .route("/cluster/owners/{c}/{id}", get(owners))
        .route("/collections", get(local_collections))
        .route("/collections/{c}/docs", post(insert))
        .route(
            "/collections/{c}/docs/{id}",
            get(get_doc).put(replace_doc).patch(patch_doc).delete(delete_doc),
        )
        .route("/collections/{c}/find", post(find))
        .route("/collections/{c}/indexes", post(create_index).get(list_indexes))
        .route("/collections/{c}/indexes/{field}", delete(drop_index))
        .route("/admin/compact", post(admin_compact))
        .route("/admin/repair", post(admin_repair).get(last_repair))
        .route("/admin/decommission", post(admin_decommission))
        .route("/admin/removenode/{id}", post(admin_removenode))
        .route("/admin/cleanup", post(admin_cleanup))
        .route("/internal/stream", post(i_stream))
        .route("/internal/pull", post(i_pull))
        .route("/internal/repair/trees", post(i_repair_trees))
        .route("/internal/repair/fetch", post(i_repair_fetch))
        // internal
        .route("/internal/gossip", post(i_gossip))
        .route("/internal/apply", post(i_apply))
        .route("/internal/get", post(i_get))
        .route("/internal/scan", post(i_scan))
        .route("/internal/multiget", post(i_multiget))
        .with_state(node)
}

// ------------------------------------------------------------------ errors

pub struct ApiError(DbError);

impl From<DbError> for ApiError {
    fn from(e: DbError) -> Self {
        ApiError(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            DbError::BadRequest(_) => StatusCode::BAD_REQUEST,
            DbError::NotFound => StatusCode::NOT_FOUND,
            DbError::Unavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,
            DbError::Timeout { .. } => StatusCode::GATEWAY_TIMEOUT,
            DbError::Conflict(_) => StatusCode::CONFLICT,
            DbError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(json!({ "error": self.0.to_string() }))).into_response()
    }
}

fn bad(msg: impl Into<String>) -> ApiError {
    ApiError(DbError::BadRequest(msg.into()))
}

// ------------------------------------------------------------------ helpers

#[derive(Deserialize)]
struct Opts {
    #[serde(default)]
    consistency: Consistency,
}

fn check_collection(c: &str) -> ApiResult<()> {
    let ok = !c.is_empty()
        && c.len() <= 128
        && c.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-');
    if ok { Ok(()) } else { Err(bad("collection names must be 1-128 chars of [A-Za-z0-9_-]")) }
}

/// Ensure the body is an object and put `_id` first.
fn with_id(doc: Value, id: &str) -> ApiResult<Value> {
    let Value::Object(fields) = doc else {
        return Err(bad("document must be a JSON object"));
    };
    let mut out = Map::with_capacity(fields.len() + 1);
    out.insert("_id".into(), Value::String(id.to_string()));
    out.extend(fields.into_iter().filter(|(k, _)| k != "_id"));
    Ok(Value::Object(out))
}

// ------------------------------------------------------------------ public handlers

async fn info(State(n): State<AppState>) -> Json<Value> {
    Json(json!({
        "name": "cassango",
        "version": env!("CARGO_PKG_VERSION"),
        "node_id": n.id,
        "state": n.gossip.my_status(),
        "replication_factor": n.rf,
    }))
}

async fn cluster(State(n): State<AppState>) -> Json<Value> {
    let own = n.gossip.ownership();
    let members: Vec<Value> = n
        .gossip
        .members()
        .into_iter()
        .map(|s| {
            json!({
                "node_id": s.node_id,
                "addr": s.addr,
                "status": if n.gossip.is_alive(&s.node_id) { "UP" } else { "DOWN" },
                "state": s.status,
                "generation": s.generation,
                "heartbeat": s.heartbeat,
                "last_seen_secs_ago": n.gossip.seconds_since_seen(&s.node_id),
                "vnodes": s.tokens.len(),
                "primary_ownership_pct": own.get(&s.node_id).map(|f| (f * 10000.0).round() / 100.0),
            })
        })
        .collect();
    Json(json!({
        "coordinator": n.id,
        "replication_factor": n.rf,
        "pending_range_movements": n.gossip.has_pending_movements(),
        "members": members,
        "pending_hints": n.hint_counts(),
    }))
}

async fn owners(State(n): State<AppState>, Path((c, id)): Path<(String, String)>) -> Json<Value> {
    let replicas: Vec<Value> = n
        .replicas_for(&c, &id)
        .into_iter()
        .map(|r| json!({ "node_id": r, "alive": n.gossip.is_alive(&r) }))
        .collect();
    Json(json!({ "collection": c, "_id": id, "replicas": replicas }))
}

fn internal(e: String) -> ApiError {
    ApiError(DbError::Internal(e))
}

async fn local_collections(State(n): State<AppState>) -> ApiResult<Json<Value>> {
    let st = n.storage.clone();
    let counts = blocking(move || st.stats()).await.map_err(internal)?;
    Ok(Json(json!({
        "node_id": n.id,
        "local_document_counts": counts,
        "storage": n.storage.info(),
    })))
}

#[derive(Deserialize)]
struct CompactOpts {
    #[serde(default)]
    major: bool,
}

/// Flush this node's memtable and compact. `?major=true` merges all SSTables into one.
async fn admin_compact(State(n): State<AppState>, Query(o): Query<CompactOpts>) -> ApiResult<Json<Value>> {
    let st = n.storage.clone();
    let compacted = blocking(move || st.compact(o.major)).await.map_err(internal)?;
    Ok(Json(json!({ "node_id": n.id, "sstables_compacted": compacted, "storage": n.storage.info() })))
}

async fn insert(
    State(n): State<AppState>,
    Path(c): Path<String>,
    Query(o): Query<Opts>,
    Json(doc): Json<Value>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    check_collection(&c)?;
    let id = match doc.get("_id") {
        None | Some(Value::Null) => uuid::Uuid::new_v4().to_string(),
        Some(Value::String(s)) if !s.is_empty() => s.clone(),
        Some(Value::Number(num)) => num.to_string(),
        Some(_) => return Err(bad("_id must be a non-empty string or a number")),
    };
    let doc = with_id(doc, &id)?;
    n.write(&c, &id, Some(doc), o.consistency).await?;
    Ok((StatusCode::CREATED, Json(json!({ "_id": id }))))
}

async fn get_doc(
    State(n): State<AppState>,
    Path((c, id)): Path<(String, String)>,
    Query(o): Query<Opts>,
) -> ApiResult<Json<Value>> {
    check_collection(&c)?;
    n.read(&c, &id, o.consistency).await?.map(Json).ok_or(ApiError(DbError::NotFound))
}

async fn replace_doc(
    State(n): State<AppState>,
    Path((c, id)): Path<(String, String)>,
    Query(o): Query<Opts>,
    Json(doc): Json<Value>,
) -> ApiResult<Json<Value>> {
    check_collection(&c)?;
    let doc = with_id(doc, &id)?;
    n.write(&c, &id, Some(doc.clone()), o.consistency).await?;
    Ok(Json(doc))
}

async fn patch_doc(
    State(n): State<AppState>,
    Path((c, id)): Path<(String, String)>,
    Query(o): Query<Opts>,
    Json(update): Json<Value>,
) -> ApiResult<Json<Value>> {
    check_collection(&c)?;
    Ok(Json(n.update(&c, &id, &update, o.consistency).await?))
}

async fn delete_doc(
    State(n): State<AppState>,
    Path((c, id)): Path<(String, String)>,
    Query(o): Query<Opts>,
) -> ApiResult<StatusCode> {
    check_collection(&c)?;
    n.write(&c, &id, None, o.consistency).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn find(
    State(n): State<AppState>,
    Path(c): Path<String>,
    body: Option<Json<FindRequest>>,
) -> ApiResult<Json<Value>> {
    check_collection(&c)?;
    let req = body.map(|Json(r)| r).unwrap_or_default();
    let res = n.find(&c, req).await?;
    Ok(Json(serde_json::to_value(res).map_err(|e| ApiError(DbError::Internal(e.to_string())))?))
}

#[derive(Deserialize)]
struct RepairOpts {
    depth: Option<u32>,
}

/// Run an anti-entropy repair of this node's primary ranges and return the report.
async fn admin_repair(State(n): State<AppState>, Query(o): Query<RepairOpts>) -> ApiResult<Json<Value>> {
    let report = n.repair(o.depth.unwrap_or(crate::repair::DEFAULT_DEPTH)).await?;
    Ok(Json(serde_json::to_value(report).map_err(|e| internal(e.to_string()))?))
}

/// Stream this node's data to the nodes taking over its ranges, then leave the cluster.
async fn admin_decommission(State(n): State<AppState>) -> ApiResult<Json<Value>> {
    let report = n.decommission().await?;
    Ok(Json(serde_json::to_value(report).map_err(|e| internal(e.to_string()))?))
}

/// Remove a dead node: re-replicate its ranges from surviving replicas and mark it LEFT.
async fn admin_removenode(State(n): State<AppState>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let report = n.removenode(&id).await?;
    Ok(Json(serde_json::to_value(report).map_err(|e| internal(e.to_string()))?))
}

/// Drop local data for ranges this node no longer owns (run after a node joins).
async fn admin_cleanup(State(n): State<AppState>) -> ApiResult<Json<Value>> {
    let dropped = n.cleanup().await?;
    Ok(Json(json!({ "node_id": n.id, "dropped_entries": dropped, "storage": n.storage.info() })))
}

async fn last_repair(State(n): State<AppState>) -> Json<Value> {
    Json(json!({ "node_id": n.id, "last_repair": *n.last_repair.lock() }))
}

#[derive(Deserialize)]
struct CreateIndex {
    field: String,
}

/// Define a secondary index cluster-wide. Every node builds it locally in the background.
async fn create_index(
    State(n): State<AppState>,
    Path(c): Path<String>,
    Json(body): Json<CreateIndex>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    check_collection(&c)?;
    let (def, created) = n.indexes.create(&c, &body.field).map_err(bad)?;
    let status = if created { StatusCode::CREATED } else { StatusCode::OK };
    Ok((status, Json(json!({ "collection": def.collection, "field": def.field, "created_ts": def.ts }))))
}

/// Indexes on a collection and whether each is ready on *this* node.
async fn list_indexes(State(n): State<AppState>, Path(c): Path<String>) -> ApiResult<Json<Value>> {
    check_collection(&c)?;
    Ok(Json(json!({ "node_id": n.id, "collection": c, "indexes": n.indexes.list(&c) })))
}

async fn drop_index(State(n): State<AppState>, Path((c, field)): Path<(String, String)>) -> ApiResult<StatusCode> {
    check_collection(&c)?;
    if n.indexes.drop_index(&c, &field) { Ok(StatusCode::NO_CONTENT) } else { Err(ApiError(DbError::NotFound)) }
}

// ------------------------------------------------------------------ internal handlers

async fn i_stream(
    State(n): State<AppState>,
    Json(r): Json<crate::movement::StreamReq>,
) -> ApiResult<Json<crate::movement::StreamPage>> {
    let st = n.storage.clone();
    Ok(Json(blocking(move || crate::movement::stream_page(&st, &r)).await.map_err(internal)?))
}

async fn i_pull(State(n): State<AppState>, Json(r): Json<crate::movement::PullReq>) -> ApiResult<Json<crate::movement::PullReport>> {
    Ok(Json(n.pull_ranges(r.plan).await.map_err(internal)?))
}

async fn i_repair_trees(
    State(n): State<AppState>,
    Json(r): Json<crate::repair::TreesReq>,
) -> ApiResult<Json<Vec<crate::repair::MerkleTree>>> {
    let st = n.storage.clone();
    let trees = blocking(move || crate::repair::build_trees(&st, &r.ranges, r.depth)).await.map_err(internal)?;
    Ok(Json(trees))
}

async fn i_repair_fetch(State(n): State<AppState>, Json(r): Json<crate::repair::FetchReq>) -> ApiResult<Json<Vec<Mutation>>> {
    let st = n.storage.clone();
    Ok(Json(blocking(move || crate::repair::fetch_entries(&st, &r)).await.map_err(internal)?))
}

async fn i_gossip(State(n): State<AppState>, Json(msg): Json<GossipMsg>) -> Json<GossipMsg> {
    n.gossip.merge_msg(msg);
    Json(n.gossip.message())
}

async fn i_apply(State(n): State<AppState>, Json(muts): Json<Vec<Mutation>>) -> ApiResult<Json<Value>> {
    let st = n.storage.clone();
    let applied = blocking(move || st.apply_batch(muts)).await.map_err(internal)?;
    Ok(Json(json!({ "applied": applied })))
}

async fn i_get(State(n): State<AppState>, Json(r): Json<GetReq>) -> ApiResult<Json<Option<Record>>> {
    let st = n.storage.clone();
    Ok(Json(blocking(move || st.get(&r.c, &r.k)).await.map_err(internal)?))
}

async fn i_scan(State(n): State<AppState>, Json(r): Json<ScanReq>) -> ApiResult<Json<crate::storage::ScanResult>> {
    crate::query::validate(&r.filter).map_err(bad)?;
    let st = n.storage.clone();
    Ok(Json(blocking(move || st.query_ids(&r.c, &r.filter)).await.map_err(internal)?))
}

async fn i_multiget(State(n): State<AppState>, Json(r): Json<MultigetReq>) -> ApiResult<Json<Vec<(String, Record)>>> {
    let st = n.storage.clone();
    Ok(Json(blocking(move || st.multiget(&r.c, &r.ids)).await.map_err(internal)?))
}
