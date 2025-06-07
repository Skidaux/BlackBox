use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;
use warp::{Filter, Rejection, Reply};

#[derive(Clone, Serialize, Deserialize)]
struct Document {
    id: usize,
    #[serde(skip)]
    vector: Option<Vec<f32>>,
    #[serde(flatten)]
    data: Value,
}

#[derive(Default, Clone, Serialize, Deserialize)]
struct Index {
    docs: Vec<Document>,
}

#[derive(Serialize, Deserialize)]
struct PersistedDocument {
    id: usize,
    data: Vec<u8>, // JSON-encoded
}

type Indexes = Arc<RwLock<HashMap<String, Index>>>;

#[tokio::main]
async fn main() {
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

    let search = warp::path!("indexes" / String / "search")
        .and(warp::get())
        .and(warp::query::<SearchQuery>())
        .and(indexes_filter.clone())
        .and_then(search_documents);

    let search_vector = warp::path!("indexes" / String / "search_vector")
        .and(warp::post())
        .and(warp::body::json())
        .and(indexes_filter.clone())
        .and_then(search_vector);

    let routes = hello
        .or(add_document)
        .or(search)
        .or(search_vector)
        .with(warp::compression::gzip());

    println!("Server running on port {}", port);
    warp::serve(routes).run(([0, 0, 0, 0], port)).await;
}

#[derive(Deserialize)]
struct SearchQuery {
    q: String,
}

#[derive(Deserialize)]
struct VectorQuery {
    vector: Vec<f32>,
    #[serde(default)]
    k: Option<usize>,
    #[serde(default)]
    field: Option<String>,
}

async fn add_document(
    index: String,
    doc: Value,
    indexes: Indexes,
) -> Result<impl Reply, Rejection> {
    let mut map = indexes.write().await;
    let entry = map.entry(index.clone()).or_default();
    let id = entry.docs.len() + 1;
    let vector = extract_vector(&doc, "vector");
    entry.docs.push(Document {
        id,
        vector,
        data: doc,
    });

    if let Err(e) = persist_index(&index, &entry.docs).await {
        eprintln!("failed to save index {index}: {e}");
        return Err(warp::reject());
    }

    Ok(warp::reply::json(&json!({ "id": id })))
}

async fn search_documents(
    index: String,
    params: SearchQuery,
    indexes: Indexes,
) -> Result<impl Reply, Rejection> {
    let map = indexes.read().await;
    if let Some(idx) = map.get(&index) {
        let query = params.q.to_lowercase();
        let results: Vec<_> = idx
            .docs
            .iter()
            .filter(|d| serialize_contains(&d.data, &query))
            .map(|d| json!({ "id": d.id, "document": d.data }))
            .collect();
        Ok(warp::reply::with_status(
            warp::reply::json(&results),
            warp::http::StatusCode::OK,
        ))
    } else {
        Ok(warp::reply::with_status(
            warp::reply::json(&json!({"error": "index not found"})),
            warp::http::StatusCode::NOT_FOUND,
        ))
    }
}

async fn search_vector(
    index: String,
    query: VectorQuery,
    indexes: Indexes,
) -> Result<impl Reply, Rejection> {
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

        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        let results: Vec<_> = scored
            .into_iter()
            .take(k)
            .filter_map(|(id, _)| {
                idx.docs
                    .iter()
                    .find(|doc| doc.id == id)
                    .map(|doc| json!({ "id": doc.id, "document": doc.data }))
            })
            .collect();
        Ok(warp::reply::with_status(
            warp::reply::json(&results),
            warp::http::StatusCode::OK,
        ))
    } else {
        Ok(warp::reply::with_status(
            warp::reply::json(&json!({"error": "index not found"})),
            warp::http::StatusCode::NOT_FOUND,
        ))
    }
}

fn serialize_contains(value: &Value, query: &str) -> bool {
    value.to_string().to_lowercase().contains(query)
}

fn extract_vector(data: &Value, field: &str) -> Option<Vec<f32>> {
    data.get(field)?.as_array().map(|arr| {
        arr.iter()
            .filter_map(|v| v.as_f64().map(|f| f as f32))
            .collect()
    })
}

fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        f32::INFINITY
    } else {
        a.iter()
            .zip(b)
            .map(|(x, y)| (x - y).powi(2))
            .sum::<f32>()
            .sqrt()
    }
}

async fn load_indexes() -> Indexes {
    let mut map = HashMap::new();
    let data_dir = PathBuf::from("data");
    if let Err(e) = fs::create_dir_all(&data_dir).await {
        eprintln!("failed to create data dir: {e}");
        return Arc::new(RwLock::new(map));
    }

    let mut entries = match fs::read_dir(&data_dir).await {
        Ok(e) => e,
        Err(_) => return Arc::new(RwLock::new(map)),
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("bin") {
            if let Ok(content) = fs::read(&path).await {
                if let Ok(raw_docs) = bincode::deserialize::<Vec<PersistedDocument>>(&content) {
                    if let Some(name) = path.file_stem().and_then(|s| s.to_str()) {
                        let docs = raw_docs
                            .into_iter()
                            .filter_map(|d| {
                                serde_json::from_slice(&d.data).ok().map(|value| {
                                    let vector = extract_vector(&value, "vector");
                                    Document {
                                        id: d.id,
                                        vector,
                                        data: value,
                                    }
                                })
                            })
                            .collect();
                        map.insert(name.to_string(), Index { docs });
                    }
                }
            }
        }
    }

    Arc::new(RwLock::new(map))
}

async fn persist_index(name: &str, docs: &Vec<Document>) -> Result<(), std::io::Error> {
    let path = PathBuf::from("data").join(format!("{name}.bin"));
    let raw: Vec<PersistedDocument> = docs
        .iter()
        .filter_map(|d| {
            serde_json::to_vec(&d.data)
                .ok()
                .map(|data| PersistedDocument { id: d.id, data })
        })
        .collect();
    let bytes =
        bincode::serialize(&raw).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    fs::write(path, bytes).await
}
