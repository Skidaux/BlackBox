use std::sync::Arc;
use tokio::sync::RwLock;
use warp::Filter;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Serialize, Deserialize)]
struct Document {
    id: usize,
    #[serde(flatten)]
    data: Value,
}

type Documents = Arc<RwLock<Vec<Document>>>;

#[tokio::main]
async fn main() {
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3000);

    let docs: Documents = Arc::new(RwLock::new(Vec::new()));
    let docs_filter = warp::any().map(move || docs.clone());

    // Route that returns "Hello world" on the root path
    let hello = warp::path::end().map(|| "Hello world");

    let add_document = warp::path("documents")
        .and(warp::post())
        .and(warp::body::json())
        .and(docs_filter.clone())
        .and_then(add_document);

    let search = warp::path("search")
        .and(warp::get())
        .and(warp::query::<SearchQuery>())
        .and(docs_filter)
        .and_then(search_documents);

    let routes = hello.or(add_document).or(search);

    println!("Server running on port {}", port);
    warp::serve(routes).run(([0, 0, 0, 0], port)).await;
}

#[derive(Deserialize)]
struct SearchQuery {
    q: String,
}

async fn add_document(doc: Value, docs: Documents) -> Result<impl warp::Reply, warp::Rejection> {
    let mut list = docs.write().await;
    let id = list.len() + 1;
    list.push(Document { id, data: doc });
    Ok(warp::reply::json(&json!({ "id": id })))
}

async fn search_documents(params: SearchQuery, docs: Documents) -> Result<impl warp::Reply, warp::Rejection> {
    let list = docs.read().await;
    let query = params.q.to_lowercase();
    let results: Vec<_> = list
        .iter()
        .filter(|d| serialize_contains(&d.data, &query))
        .map(|d| json!({ "id": d.id, "document": d.data }))
        .collect();
    Ok(warp::reply::json(&results))
}

fn serialize_contains(value: &Value, query: &str) -> bool {
    value
        .to_string()
        .to_lowercase()
        .contains(query)
}
