mod index;
mod utils;
mod storage;

use crate::index::{Document, Mapping};
use crate::utils::{extract_vector, l2_distance, serialize_contains};
use crate::storage::{Indexes, load_indexes, persist_index, persist_mapping};
use env_logger;
use serde::Deserialize;
use serde_json::{json, Value};
use warp::{Filter, Rejection, Reply};
use std::cmp::Ordering;

#[derive(Deserialize)]
struct SearchQuery { q: String }

#[derive(Deserialize)]
struct VectorQuery {
    vector: Vec<f32>,
    #[serde(default)]
    k: Option<usize>,
    #[serde(default)]
    field: Option<String>,
}

#[derive(Deserialize)]
struct BulkDocs { documents: Vec<Value> }

#[derive(Deserialize)]
struct DslRange { #[serde(default)] gte: Option<f64>, #[serde(default)] lte: Option<f64> }

#[derive(Deserialize)]
struct DslSort { field: String, #[serde(default)] order: Option<String> }

#[derive(Deserialize)]
struct DslQuery {
    #[serde(default)]
    term: Option<std::collections::HashMap<String, String>>,
    #[serde(default)]
    range: Option<std::collections::HashMap<String, DslRange>>,
    #[serde(default)]
    sort: Option<DslSort>,
    #[serde(default)]
    aggs: Option<String>,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let port: u16 = std::env::var("PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(3000);

    let indexes = load_indexes().await;
    let indexes_filter = warp::any().map(move || indexes.clone());

    let hello = warp::path::end().map(|| "Hello world");

    let add_document = warp::path!("indexes" / String / "documents")
        .and(warp::post())
        .and(warp::body::json())
        .and(indexes_filter.clone())
        .and_then(add_document);

    let bulk_docs = warp::path!("indexes" / String / "bulk")
        .and(warp::post())
        .and(warp::body::json())
        .and(indexes_filter.clone())
        .and_then(bulk_documents);

    let set_mapping = warp::path!("indexes" / String / "mapping")
        .and(warp::put())
        .and(warp::body::json())
        .and(indexes_filter.clone())
        .and_then(set_mapping);

    let search = warp::path!("indexes" / String / "search")
        .and(warp::get())
        .and(warp::query::<SearchQuery>())
        .and(indexes_filter.clone())
        .and_then(search_documents);

    let search_dsl = warp::path!("indexes" / String / "query")
        .and(warp::post())
        .and(warp::body::json())
        .and(indexes_filter.clone())
        .and_then(search_dsl);

    let search_vector = warp::path!("indexes" / String / "search_vector")
        .and(warp::post())
        .and(warp::body::json())
        .and(indexes_filter.clone())
        .and_then(search_vector);

    let routes = hello
        .or(add_document)
        .or(bulk_docs)
        .or(set_mapping)
        .or(search)
        .or(search_dsl)
        .or(search_vector)
        .with(warp::compression::gzip())
        .with(warp::log("blackbox"));

    println!("Server running on port {}", port);
    warp::serve(routes).run(([0, 0, 0, 0], port)).await;
}

async fn add_document(index: String, doc: Value, indexes: Indexes) -> Result<impl Reply, Rejection> {
    let id = insert_doc(&index, doc, indexes, true).await?;
    Ok(warp::reply::json(&json!({"id": id})))
}

async fn bulk_documents(index: String, bulk: BulkDocs, indexes: Indexes) -> Result<impl Reply, Rejection> {
    let mut ids = Vec::new();
    for doc in bulk.documents {
        if let Ok(id) = insert_doc(&index, doc, indexes.clone(), false).await {
            ids.push(id);
        }
    }
    Ok(warp::reply::json(&json!({"ids": ids})))
}

async fn insert_doc(index: &str, doc: Value, indexes: Indexes, persist: bool) -> Result<usize, Rejection> {
    let mut map = indexes.write().await;
    let entry = map.entry(index.to_string()).or_default();
    let id = entry.docs.len() + 1;
    let vector = extract_vector(&doc, "vector");
    entry.docs.push(Document { id, vector, data: doc });
    if persist {
        if let Err(e) = persist_index(index, entry).await {
            eprintln!("failed to save index {index}: {e}");
            return Err(warp::reject());
        }
    }
    Ok(id)
}

async fn set_mapping(index: String, mapping: Mapping, indexes: Indexes) -> Result<impl Reply, Rejection> {
    let mut map = indexes.write().await;
    let entry = map.entry(index.clone()).or_default();
    entry.mapping = Some(mapping.clone());
    if let Err(e) = persist_mapping(&index, &mapping).await {
        eprintln!("failed to save mapping {index}: {e}");
    }
    Ok(warp::reply::json(&json!({"status": "ok"})))
}

async fn search_documents(index: String, params: SearchQuery, indexes: Indexes) -> Result<impl Reply, Rejection> {
    let map = indexes.read().await;
    if let Some(idx) = map.get(&index) {
        let query = params.q.to_lowercase();
        let results: Vec<_> = idx
            .docs
            .iter()
            .filter(|d| serialize_contains(&d.data, &query))
            .map(|d| json!({ "id": d.id, "document": d.data }))
            .collect();
        Ok(warp::reply::json(&results))
    } else {
        Ok(warp::reply::json(&json!({"error": "index not found"})))
    }
}

async fn search_dsl(index: String, req: DslQuery, indexes: Indexes) -> Result<impl Reply, Rejection> {
    use std::collections::HashMap;
    let map = indexes.read().await;
    if let Some(idx) = map.get(&index) {
        let mut results: Vec<&Document> = idx.docs.iter().collect();
        if let Some(term) = req.term {
            for (field, value) in term {
                results.retain(|d| d.data.get(&field).map(|v| v == &Value::String(value.clone())).unwrap_or(false));
            }
        }
        if let Some(range) = req.range {
            for (field, flt) in range {
                results.retain(|d| {
                    if let Some(v) = d.data.get(&field).and_then(|v| v.as_f64()) {
                        flt.gte.map_or(true, |g| v >= g) && flt.lte.map_or(true, |l| v <= l)
                    } else { false }
                });
            }
        }
        if let Some(sort) = req.sort {
            results.sort_by(|a, b| compare_vals(a.data.get(&sort.field), b.data.get(&sort.field)));
            if sort.order.as_deref() == Some("desc") { results.reverse(); }
        }
        let hits: Vec<_> = results.iter().map(|d| json!({"id": d.id, "document": d.data })).collect();
        let mut body = json!({"hits": hits});
        if let Some(field) = req.aggs {
            let mut counts: HashMap<String, usize> = HashMap::new();
            for d in &results {
                if let Some(val) = d.data.get(&field) {
                    *counts.entry(val.to_string()).or_insert(0) += 1;
                }
            }
            body["aggregations"] = json!(counts);
        }
        Ok(warp::reply::json(&body))
    } else {
        Ok(warp::reply::json(&json!({"error": "index not found"})))
    }
}

fn compare_vals(a: Option<&Value>, b: Option<&Value>) -> Ordering {
    match (a, b) {
        (Some(Value::Number(na)), Some(Value::Number(nb))) => na.as_f64().partial_cmp(&nb.as_f64()).unwrap_or(Ordering::Equal),
        (Some(Value::String(sa)), Some(Value::String(sb))) => sa.cmp(sb),
        _ => Ordering::Equal,
    }
}

async fn search_vector(index: String, query: VectorQuery, indexes: Indexes) -> Result<impl Reply, Rejection> {
    let map = indexes.read().await;
    if let Some(idx) = map.get(&index) {
        let k = query.k.unwrap_or(10);
        let field = query.field.unwrap_or_else(|| "vector".to_string());
        let qvec = query.vector;
        let mut scored: Vec<(usize, f32)> = idx
            .docs
            .iter()
            .filter_map(|d| {
                let extracted;
                let vec = if field == "vector" {
                    d.vector.as_ref()
                } else {
                    extracted = extract_vector(&d.data, &field);
                    extracted.as_ref()
                }?;
                Some((d.id, l2_distance(&qvec, vec)))
            })
            .collect();
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        let results: Vec<_> = scored.into_iter().take(k).filter_map(|(id, _)| {
            idx.docs.iter().find(|doc| doc.id == id).map(|doc| json!({ "id": doc.id, "document": doc.data }))
        }).collect();
        Ok(warp::reply::json(&results))
    } else {
        Ok(warp::reply::json(&json!({"error": "index not found"})))
    }
}
