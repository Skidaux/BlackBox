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
    #[serde(skip)]
    pub inverted: HashMap<String, Vec<usize>>, 
}

#[derive(Serialize, Deserialize)]
pub struct PersistedDocument {
    pub id: usize,
    pub data: Vec<u8>,
}

impl Index {
    pub fn build_inverted(&mut self) {
        self.inverted.clear();
        let docs: Vec<(usize, Value)> = self
            .docs
            .iter()
            .map(|d| (d.id, d.data.clone()))
            .collect();
        for (id, data) in docs {
            self.add_to_index(id, &data);
        }
    }

    pub fn add_to_index(&mut self, id: usize, data: &Value) {
        fn index_value(map: &mut HashMap<String, Vec<usize>>, id: usize, v: &Value) {
            match v {
                Value::String(s) => {
                    for token in s.to_lowercase().split_whitespace() {
                        map.entry(token.to_string()).or_default().push(id);
                    }
                }
                Value::Object(obj) => {
                    for val in obj.values() {
                        index_value(map, id, val);
                    }
                }
                Value::Array(arr) => {
                    for val in arr {
                        index_value(map, id, val);
                    }
                }
                _ => {}
            }
        }

        index_value(&mut self.inverted, id, data);
    }
}
