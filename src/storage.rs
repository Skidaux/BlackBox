use crate::index::{Document, Index, Mapping, PersistedDocument};
use crate::utils::extract_vector;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;

pub type Indexes = Arc<RwLock<HashMap<String, Index>>>;

pub async fn load_indexes() -> Indexes {
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
                        let mapping = load_mapping(name).await;
                        map.insert(name.to_string(), Index { docs, mapping });
                    }
                }
            }
        }
    }

    Arc::new(RwLock::new(map))
}

pub async fn persist_index(name: &str, index: &Index) -> Result<(), std::io::Error> {
    let path = PathBuf::from("data").join(format!("{name}.bin"));
    let raw: Vec<PersistedDocument> = index
        .docs
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

pub async fn load_mapping(name: &str) -> Option<Mapping> {
    let path = PathBuf::from("data").join(format!("{name}.mapping.json"));
    match fs::read(&path).await {
        Ok(bytes) => serde_json::from_slice(&bytes).ok(),
        Err(_) => None,
    }
}

pub async fn persist_mapping(name: &str, mapping: &Mapping) -> Result<(), std::io::Error> {
    let path = PathBuf::from("data").join(format!("{name}.mapping.json"));
    let bytes = serde_json::to_vec(mapping)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    fs::write(path, bytes).await
}
