mod index;
mod storage;
mod utils;

use crate::index::{Document, Mapping};
use crate::storage::{Indexes, load_indexes, persist_index, persist_mapping};
use crate::utils::{extract_vector, fuzzy_match, l2_distance, serialize_contains};
use env_logger;
use serde::Deserialize;
use serde_json::{Value, json};
use std::cmp::Ordering;
use warp::{Filter, Rejection, Reply};

#[derive(Deserialize)]
struct SearchQuery {
    q: String,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    fuzz: Option<usize>,
    #[serde(default)]
    scores: Option<bool>,
}

#[derive(Deserialize)]
struct VectorQuery {
    vector: Vec<f32>,
    #[serde(default, alias = "k")]
    limit: Option<usize>,
    #[serde(default)]
    field: Option<String>,
    #[serde(default)]
    scores: Option<bool>,
}

#[derive(Deserialize)]
struct BulkDocs {
    documents: Vec<Value>,
}

#[derive(Deserialize)]
struct DslRange {
    #[serde(default)]
    gte: Option<f64>,
    #[serde(default)]
    lte: Option<f64>,
}

#[derive(Deserialize)]
struct DslSort {
    field: String,
    #[serde(default)]
    order: Option<String>,
}

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
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    fuzz: Option<usize>,
    #[serde(default)]
    scores: Option<bool>,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3000);

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

    let metrics = warp::log::custom(|info| {
        let latency = info.elapsed().as_millis();
        let len = info
            .request_headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("0");
        log::info!(
            "{} {} -> {} {}ms req={}b",
            info.method(),
            info.path(),
            info.status().as_u16(),
            latency,
            len
        );
    });

    let routes = hello
        .or(add_document)
        .or(bulk_docs)
        .or(set_mapping)
        .or(search)
        .or(search_dsl)
        .or(search_vector)
        .with(warp::compression::gzip())
        .with(metrics);

    println!("Server running on port {}", port);
    warp::serve(routes).run(([0, 0, 0, 0], port)).await;
}

async fn add_document(
    index: String,
    doc: Value,
    indexes: Indexes,
) -> Result<impl Reply, Rejection> {
    let id = insert_doc(&index, doc, indexes, true).await?;
    Ok(warp::reply::json(&json!({"id": id})))
}

async fn bulk_documents(
    index: String,
    bulk: BulkDocs,
    indexes: Indexes,
) -> Result<impl Reply, Rejection> {
    let mut ids = Vec::new();
    for doc in bulk.documents {
        if let Ok(id) = insert_doc(&index, doc, indexes.clone(), false).await {
            ids.push(id);
        }
    }
    Ok(warp::reply::json(&json!({"ids": ids})))
}

async fn insert_doc(
    index: &str,
    doc: Value,
    indexes: Indexes,
    persist: bool,
) -> Result<usize, Rejection> {
    let mut map = indexes.write().await;
    let entry = map.entry(index.to_string()).or_default();
    let id = entry.docs.len() + 1;
    let vector = extract_vector(&doc, "vector");
    entry.docs.push(Document {
        id,
        vector,
        data: doc,
    });
    if persist {
        if let Err(e) = persist_index(index, entry).await {
            eprintln!("failed to save index {index}: {e}");
            return Err(warp::reject());
        }
    }
    Ok(id)
}

async fn set_mapping(
    index: String,
    mapping: Mapping,
    indexes: Indexes,
) -> Result<impl Reply, Rejection> {
    let mut map = indexes.write().await;
    let entry = map.entry(index.clone()).or_default();
    entry.mapping = Some(mapping.clone());
    if let Err(e) = persist_mapping(&index, &mapping).await {
        eprintln!("failed to save mapping {index}: {e}");
    }
    Ok(warp::reply::json(&json!({"status": "ok"})))
}

async fn search_documents(
    index: String,
    params: SearchQuery,
    indexes: Indexes,
) -> Result<impl Reply, Rejection> {
    let map = indexes.read().await;
    if let Some(idx) = map.get(&index) {
        let limit = params.limit.unwrap_or(10);
        let fuzz = params.fuzz.unwrap_or(0);
        let include_scores = params.scores.unwrap_or(false);
        let query = params.q.to_lowercase();
        let mut scored: Vec<(usize, f32)> = idx
            .docs
            .iter()
            .filter_map(|d| {
                if fuzz > 0 {
                    fuzzy_match(&d.data, &query, fuzz).map(|dist| (d.id, 1.0 / (dist as f32 + 1.0)))
                } else if serialize_contains(&d.data, &query) {
                    Some((d.id, 1.0))
                } else {
                    None
                }
            })
            .collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        let results: Vec<_> = scored
            .into_iter()
            .take(limit)
            .filter_map(|(id, score)| {
                idx.docs.iter().find(|doc| doc.id == id).map(|doc| {
                    if include_scores {
                        json!({ "id": doc.id, "document": doc.data, "score": score })
                    } else {
                        json!({ "id": doc.id, "document": doc.data })
                    }
                })
            })
            .collect();
        Ok(warp::reply::json(&json!({"hits": results})))
    } else {
        Ok(warp::reply::json(&json!({"error": "index not found"})))
    }
}

async fn search_dsl(
    index: String,
    req: DslQuery,
    indexes: Indexes,
) -> Result<impl Reply, Rejection> {
    use std::collections::HashMap;
    let map = indexes.read().await;
    if let Some(idx) = map.get(&index) {
        let mut results: Vec<&Document> = idx.docs.iter().collect();
        if let Some(ref term) = req.term {
            for (field, value) in term {
                results.retain(|d| {
                    d.data
                        .get(&field)
                        .map(|v| v == &Value::String(value.clone()))
                        .unwrap_or(false)
                });
            }
        }
        if let Some(range) = req.range {
            for (field, flt) in range {
                results.retain(|d| {
                    if let Some(v) = d.data.get(&field).and_then(|v| v.as_f64()) {
                        flt.gte.map_or(true, |g| v >= g) && flt.lte.map_or(true, |l| v <= l)
                    } else {
                        false
                    }
                });
            }
        }
        if let Some(sort) = req.sort {
            results.sort_by(|a, b| compare_vals(a.data.get(&sort.field), b.data.get(&sort.field)));
            if sort.order.as_deref() == Some("desc") {
                results.reverse();
            }
        }

        let limit = req.limit.unwrap_or(results.len());
        let fuzz = req.fuzz.unwrap_or(0);
        let include_scores = req.scores.unwrap_or(false);
        let mut hits = Vec::new();
        for d in results.iter().take(limit) {
            let score = if fuzz > 0 {
                req.term
                    .as_ref()
                    .and_then(|m| {
                        m.iter().next().and_then(|(field, val)| {
                            d.data
                                .get(field)
                                .and_then(|v| fuzzy_match(v, val, fuzz))
                                .map(|dist| 1.0 / (dist as f32 + 1.0))
                        })
                    })
                    .unwrap_or(1.0)
            } else {
                1.0
            };
            if include_scores {
                hits.push(json!({"id": d.id, "document": d.data, "score": score}));
            } else {
                hits.push(json!({"id": d.id, "document": d.data}));
            }
        }
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
        (Some(Value::Number(na)), Some(Value::Number(nb))) => na
            .as_f64()
            .partial_cmp(&nb.as_f64())
            .unwrap_or(Ordering::Equal),
        (Some(Value::String(sa)), Some(Value::String(sb))) => sa.cmp(sb),
        _ => Ordering::Equal,
    }
}

async fn search_vector(
    index: String,
    query: VectorQuery,
    indexes: Indexes,
) -> Result<impl Reply, Rejection> {
    let map = indexes.read().await;
    if let Some(idx) = map.get(&index) {
        let limit = query.limit.unwrap_or(10);
        let field = query.field.unwrap_or_else(|| "vector".to_string());
        let include_scores = query.scores.unwrap_or(false);
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
        let results: Vec<_> = scored
            .into_iter()
            .take(limit)
            .filter_map(|(id, distance)| {
                idx.docs.iter().find(|doc| doc.id == id).map(|doc| {
                    if include_scores {
                        json!({ "id": doc.id, "document": doc.data, "score": distance })
                    } else {
                        json!({ "id": doc.id, "document": doc.data })
                    }
                })
            })
            .collect();
        Ok(warp::reply::json(&json!({"hits": results})))
    } else {
        Ok(warp::reply::json(&json!({"error": "index not found"})))
    }
}
