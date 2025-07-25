use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Clone, Serialize, Deserialize)]
pub struct Document {
    pub id: usize,
    #[serde(skip)]
    pub vector: Option<Vec<f32>>,
    #[serde(flatten)]
    pub data: Value,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum FieldType {
    String,
    Numeric,
    Vector,
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Mapping {
    pub fields: HashMap<String, FieldType>,
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct Index {
    pub docs: Vec<Document>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mapping: Option<Mapping>,
}

#[derive(Serialize, Deserialize)]
pub struct PersistedDocument {
    pub id: usize,
    pub data: Vec<u8>,
}
