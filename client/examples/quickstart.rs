//! Quick tour of the cassango Rust driver.
//!
//!   scripts/cluster.sh start 3
//!   cargo run -p cassango-client --example quickstart
//!
//! Set CASSANGO_HOST to point at other nodes (comma-separated).

use cassango_client::{Client, Consistency, Error, Filter, Order, Update};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Product {
    #[serde(rename = "_id")]
    sku: String,
    name: String,
    price: f64,
    stock: u32,
    category: String,
    #[serde(default)]
    tags: Vec<String>,
}

fn product(sku: &str, name: &str, price: f64, stock: u32, category: &str) -> Product {
    Product { sku: sku.into(), name: name.into(), price, stock, category: category.into(), tags: vec![] }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let hosts = std::env::var("CASSANGO_HOST").unwrap_or_else(|_| "127.0.0.1:7001,127.0.0.1:7002".into());
    let client = Client::builder()
        .hosts(hosts.split(','))
        .consistency(Consistency::Quorum)
        .timeout(Duration::from_secs(10))
        .build()?;

    // Learn every node from the cluster, so any of them can be used if one goes down.
    let nodes = client.refresh_hosts().await?;
    println!("connected; nodes: {nodes:?}");

    // ---- typed collection
    let products = client.collection::<Product>("products_demo");
    products.delete_many(json!({})).await?; // start clean

    let ids = products
        .insert_many(&[
            product("p1", "Kettle", 24.99, 12, "kitchen"),
            product("p2", "Toaster", 39.50, 0, "kitchen"),
            product("p3", "Lamp", 18.00, 40, "living"),
            product("p4", "Rug", 120.00, 3, "living"),
            product("p5", "Mug", 6.50, 200, "kitchen"),
        ])
        .await?;
    println!("inserted {ids:?}");

    // Get by id.
    if let Some(p) = products.get("p1").await? {
        println!("p1 = {} at £{:.2}", p.name, p.price);
    }

    // Query with the Filter builder: in stock, under £30, cheapest first.
    let cheap = products
        .find(Filter::new().gt("stock", 0).lt("price", 30.0))
        .sort_by("price", Order::Asc)
        .await?;
    println!("in stock under £30 ({} of {}):", cheap.docs.len(), cheap.matched);
    for p in &cheap.docs {
        println!("  {:<8} £{:>6.2}  stock {}", p.name, p.price, p.stock);
    }

    // Or with plain JSON, MongoDB style.
    let kitchen = products.count(json!({"category": "kitchen"})).await?;
    println!("kitchen products: {kitchen}");

    // Update with the Update builder; returns the updated document.
    if let Some(p) = products.update("p2", Update::new().inc("stock", 25).inc("price", -5.5).push("tags", "restocked")).await? {
        println!("restocked {}: stock {}, price £{:.2}, tags {:?}", p.name, p.stock, p.price, p.tags);
    }
    let on_sale = products.update_many(json!({"category": "living"}), json!({"$set": {"sale": true}})).await?;
    println!("marked {on_sale} living products on sale");

    // ---- secondary index + explain
    products.create_index("price").await?;
    products.wait_for_index("price", Duration::from_secs(30)).await?;
    let r = products.find(Filter::new().gte("price", 20.0)).explain().await?;
    for (node, plan) in r.explain.unwrap_or_default() {
        println!("  {node}: {} examined {}, matched {}", plan.index.as_deref().unwrap_or("full scan"), plan.examined, plan.matched);
    }

    // ---- schemaless access with serde_json::Value
    let events = client.collection::<Value>("events_demo");
    let id = events.insert(&json!({"type": "signup", "user": "ann", "meta": {"source": "web"}})).await?;
    let ev = events.get(&id).await?.expect("just inserted");
    println!("event {id}: {}", ev["meta"]["source"]);

    // ---- consistency per collection handle
    let fast = products.with_consistency(Consistency::One);
    println!("read at ONE: {:?}", fast.get("p3").await?.map(|p| p.name));

    // ---- errors are typed
    match products.find(json!({"price": {"$nope": 1}})).await {
        Err(Error::BadRequest(msg)) => println!("bad query rejected: {msg}"),
        other => println!("unexpected: {other:?}"),
    }

    // ---- cluster info
    let info = client.cluster().await?;
    for m in &info.members {
        println!("  {} {} {} {:.1}%", m.node_id, m.status, m.state, m.primary_ownership_pct.unwrap_or(0.0));
    }
    println!("replicas of p1: {:?}", products.owners("p1").await?.iter().map(|r| &r.node_id).collect::<Vec<_>>());

    products.drop_index("price").await?;
    Ok(())
}
