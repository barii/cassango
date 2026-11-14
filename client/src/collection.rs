use crate::query::Order;
use crate::types::{Consistency, IndexInfo, NodePlan, Replica};
use crate::{enc, error_from, Client, Error, Result};
use futures::{StreamExt, TryStreamExt};
use reqwest::Method;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;

/// How many requests `insert_many`/`update_many`/`delete_many` keep in flight.
const CONCURRENCY: usize = 16;

/// A collection whose documents are (de)serialized as `T`.
///
/// `T` is any serde type. To control the document id, give it a field renamed to `_id`:
///
/// ```
/// #[derive(serde::Serialize, serde::Deserialize)]
/// struct Product {
///     #[serde(rename = "_id")]
///     sku: String,
///     price: f64,
/// }
/// ```
///
/// Without an `_id` field the server generates one (returned by [`Collection::insert`]).
pub struct Collection<T> {
    client: Client,
    name: String,
    consistency: Consistency,
    _doc: PhantomData<fn() -> T>,
}

impl<T> Clone for Collection<T> {
    fn clone(&self) -> Self {
        Collection { client: self.client.clone(), name: self.name.clone(), consistency: self.consistency, _doc: PhantomData }
    }
}

/// The result of a [`Find`].
#[derive(Debug, Clone)]
pub struct FindResult<T> {
    /// Matching documents after sort/skip/limit.
    pub docs: Vec<T>,
    /// Total number of matches before skip/limit.
    pub matched: usize,
    /// True if some nodes were unreachable, so results may be incomplete.
    pub partial: bool,
    /// Per-node query plans, when [`Find::explain`] was requested.
    pub explain: Option<BTreeMap<String, NodePlan>>,
}

impl<T> IntoIterator for FindResult<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        self.docs.into_iter()
    }
}

impl<T> Collection<T> {
    pub(crate) fn new(client: Client, name: &str, consistency: Consistency) -> Self {
        Collection { client, name: name.to_string(), consistency, _doc: PhantomData }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// The same collection, using another consistency level for reads and writes.
    pub fn with_consistency(&self, cl: Consistency) -> Self {
        let mut c = self.clone();
        c.consistency = cl;
        c
    }

    /// The same collection viewed with another document type.
    pub fn typed<U>(&self) -> Collection<U> {
        Collection::new(self.client.clone(), &self.name, self.consistency)
    }

    fn docs_path(&self) -> String {
        format!("/collections/{}/docs", enc(&self.name))
    }

    fn doc_path(&self, id: &str) -> String {
        format!("/collections/{}/docs/{}", enc(&self.name), enc(id))
    }
}

impl<T> Collection<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Insert a document and return its `_id` (generated if the document has none).
    /// Like Cassandra, this is an upsert: an existing document with the same `_id` is replaced.
    pub async fn insert(&self, doc: &T) -> Result<String> {
        let body = serde_json::to_value(doc)?;
        let r = self.client.call(Method::POST, &self.docs_path(), Some(self.consistency), Some(&body)).await?;
        r["_id"].as_str().map(str::to_string).ok_or_else(|| Error::Transport(format!("unexpected response: {r}")))
    }

    /// Insert many documents concurrently. Returns their ids in input order.
    /// Stops at the first error; documents already sent stay inserted.
    pub async fn insert_many(&self, docs: &[T]) -> Result<Vec<String>> {
        futures::stream::iter(docs).map(|d| self.insert(d)).buffered(CONCURRENCY).try_collect().await
    }

    /// Insert or replace the document with this id.
    pub async fn put(&self, id: &str, doc: &T) -> Result<()> {
        let body = serde_json::to_value(doc)?;
        self.client.call(Method::PUT, &self.doc_path(id), Some(self.consistency), Some(&body)).await?;
        Ok(())
    }

    /// Fetch a document by id. `None` if it doesn't exist (or was deleted).
    pub async fn get(&self, id: &str) -> Result<Option<T>> {
        let (status, body) = self.client.raw(Method::GET, &self.doc_path(id), Some(self.consistency), None).await?;
        match status {
            200 => Ok(Some(serde_json::from_value(body)?)),
            404 => Ok(None),
            s => Err(error_from(s, &body)),
        }
    }

    /// Apply update operators (`$set`, `$unset`, `$inc`, `$push`) to one document and
    /// return it as updated, or `None` if it doesn't exist. Read-modify-write: concurrent
    /// updates to the same document resolve by last write wins.
    pub async fn update(&self, id: &str, update: impl Into<Value>) -> Result<Option<T>> {
        let body = update.into();
        let (status, resp) = self.client.raw(Method::PATCH, &self.doc_path(id), Some(self.consistency), Some(&body)).await?;
        match status {
            200 => Ok(Some(serde_json::from_value(resp)?)),
            404 => Ok(None),
            s => Err(error_from(s, &resp)),
        }
    }

    /// Delete a document. Deleting a missing document is not an error.
    pub async fn delete(&self, id: &str) -> Result<()> {
        self.client.call(Method::DELETE, &self.doc_path(id), Some(self.consistency), None).await?;
        Ok(())
    }

    /// Start a query. Add `.sort()`, `.skip()`, `.limit()`, `.explain()`, then `.await` it.
    pub fn find(&self, filter: impl Into<Value>) -> Find<T> {
        Find {
            coll: self.clone(),
            filter: filter.into(),
            sort: Map::new(),
            skip: None,
            limit: None,
            explain: false,
        }
    }

    /// Every document in the collection (combine with `.sort()`/`.limit()`).
    pub fn find_all(&self) -> Find<T> {
        self.find(json!({}))
    }

    /// The first matching document.
    pub async fn find_one(&self, filter: impl Into<Value>) -> Result<Option<T>> {
        self.find(filter).one().await
    }

    /// Number of matching documents across the cluster.
    pub async fn count(&self, filter: impl Into<Value>) -> Result<usize> {
        self.find(filter).count().await
    }

    async fn matching_ids(&self, filter: Value) -> Result<Vec<String>> {
        let r = self.typed::<Value>().find(filter).send().await?;
        if r.partial {
            return Err(Error::Partial);
        }
        Ok(r.docs.iter().filter_map(|d| d["_id"].as_str().map(str::to_string)).collect())
    }

    /// Update every matching document; returns how many were updated. Not atomic: it finds
    /// the matching ids, then updates each one. Refuses with [`Error::Partial`] if some nodes
    /// are unreachable.
    pub async fn update_many(&self, filter: impl Into<Value>, update: impl Into<Value>) -> Result<usize> {
        let ids = self.matching_ids(filter.into()).await?;
        let update = update.into();
        let raw = self.typed::<Value>();
        let updated: Vec<bool> = futures::stream::iter(&ids)
            .map(|id| {
                let (raw, update) = (&raw, update.clone());
                async move { raw.update(id, update).await.map(|d| d.is_some()) }
            })
            .buffer_unordered(CONCURRENCY)
            .try_collect()
            .await?;
        Ok(updated.into_iter().filter(|u| *u).count())
    }

    /// Delete every matching document; returns how many were deleted. Same caveats as
    /// [`Collection::update_many`].
    pub async fn delete_many(&self, filter: impl Into<Value>) -> Result<usize> {
        let ids = self.matching_ids(filter.into()).await?;
        futures::stream::iter(&ids).map(|id| self.delete(id)).buffer_unordered(CONCURRENCY).try_collect::<Vec<()>>().await?;
        Ok(ids.len())
    }

    /// Create a secondary index on a (dotted) field. Returns `false` if it already
    /// existed. Nodes build it in the background; see [`Collection::indexes`].
    pub async fn create_index(&self, field: &str) -> Result<bool> {
        let path = format!("/collections/{}/indexes", enc(&self.name));
        let (status, body) = self.client.raw(Method::POST, &path, None, Some(&json!({ "field": field }))).await?;
        match status {
            201 => Ok(true),
            200 => Ok(false),
            s => Err(error_from(s, &body)),
        }
    }

    /// Drop a secondary index. Returns `false` if there was no such index.
    pub async fn drop_index(&self, field: &str) -> Result<bool> {
        let path = format!("/collections/{}/indexes/{}", enc(&self.name), enc(field));
        let (status, body) = self.client.raw(Method::DELETE, &path, None, None).await?;
        match status {
            204 => Ok(true),
            404 => Ok(false),
            s => Err(error_from(s, &body)),
        }
    }

    /// Indexes on this collection and their build state on the current node.
    pub async fn indexes(&self) -> Result<Vec<IndexInfo>> {
        let r = self.client.call(Method::GET, &format!("/collections/{}/indexes", enc(&self.name)), None, None).await?;
        Ok(serde_json::from_value(r["indexes"].clone())?)
    }

    /// Wait until an index is ready on every UP node, polling every 100 ms.
    pub async fn wait_for_index(&self, field: &str, timeout: std::time::Duration) -> Result<()> {
        let deadline = std::time::Instant::now() + timeout;
        let up: Vec<String> = self.client.cluster().await?.members.into_iter().filter(|m| m.is_up() && m.state == "NORMAL").map(|m| m.addr).collect();
        for addr in up {
            let node = Client::builder().host(&addr).build()?;
            loop {
                let ready = node.collection::<Value>(&self.name).indexes().await?.iter().any(|i| i.field == field && i.is_ready());
                if ready {
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    return Err(Error::Timeout(format!("index {}.{field} not ready on {addr}", self.name)));
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
        Ok(())
    }

    /// Which nodes store the document with this id.
    pub async fn owners(&self, id: &str) -> Result<Vec<Replica>> {
        let path = format!("/cluster/owners/{}/{}", enc(&self.name), enc(id));
        let r = self.client.call(Method::GET, &path, None, None).await?;
        Ok(serde_json::from_value(r["replicas"].clone())?)
    }
}

/// A query being built. Await it (or call [`Find::send`]) to run it.
#[must_use = "a Find does nothing until it is awaited"]
pub struct Find<T> {
    coll: Collection<T>,
    filter: Value,
    sort: Map<String, Value>,
    skip: Option<usize>,
    limit: Option<usize>,
    explain: bool,
}

impl<T> Find<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Sort by a spec such as `json!({"age": -1, "name": 1})`. Can be combined with `sort_by`.
    pub fn sort(mut self, spec: impl Into<Value>) -> Self {
        if let Value::Object(m) = spec.into() {
            self.sort.extend(m);
        }
        self
    }

    /// Sort by one field; call repeatedly for tie-breakers.
    pub fn sort_by(mut self, field: &str, order: Order) -> Self {
        self.sort.insert(field.to_string(), json!(if order == Order::Asc { 1 } else { -1 }));
        self
    }

    pub fn skip(mut self, n: usize) -> Self {
        self.skip = Some(n);
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Include each node's plan (index used, entries examined) in [`FindResult::explain`].
    pub fn explain(mut self) -> Self {
        self.explain = true;
        self
    }

    fn body(&self) -> Value {
        let mut b = Map::new();
        b.insert("filter".into(), self.filter.clone());
        if !self.sort.is_empty() {
            b.insert("sort".into(), Value::Object(self.sort.clone()));
        }
        if let Some(s) = self.skip {
            b.insert("skip".into(), json!(s));
        }
        if let Some(l) = self.limit {
            b.insert("limit".into(), json!(l));
        }
        if self.explain {
            b.insert("explain".into(), json!(true));
        }
        Value::Object(b)
    }

    /// Run the query.
    pub async fn send(self) -> Result<FindResult<T>> {
        let path = format!("/collections/{}/find", enc(&self.coll.name));
        let r = self.coll.client.call(Method::POST, &path, None, Some(&self.body())).await?;
        let docs = match r.get("docs") {
            Some(Value::Array(a)) => a.iter().cloned().map(serde_json::from_value).collect::<Result<Vec<T>, _>>()?,
            _ => Vec::new(),
        };
        Ok(FindResult {
            docs,
            matched: r["matched"].as_u64().unwrap_or(0) as usize,
            partial: r["partial"].as_bool().unwrap_or(false),
            explain: match r.get("explain") {
                Some(e) if !e.is_null() => Some(serde_json::from_value(e.clone())?),
                _ => None,
            },
        })
    }

    /// Run the query and return the first document.
    pub async fn one(mut self) -> Result<Option<T>> {
        self.limit = Some(1);
        Ok(self.send().await?.docs.into_iter().next())
    }

    /// Run the query and return only the number of matches.
    pub async fn count(mut self) -> Result<usize> {
        self.limit = Some(0);
        Ok(self.send().await?.matched)
    }
}

impl<T> IntoFuture for Find<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Output = Result<FindResult<T>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.send())
    }
}
