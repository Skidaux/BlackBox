use serde_json::Value;
use strsim::levenshtein;

pub fn serialize_contains(value: &Value, query: &str) -> bool {
    value.to_string().to_lowercase().contains(query)
}

pub fn extract_vector(data: &Value, field: &str) -> Option<Vec<f32>> {
    data.get(field)?.as_array().map(|arr| {
        arr.iter()
            .filter_map(|v| v.as_f64().map(|f| f as f32))
            .collect()
    })
}

pub fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
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

pub fn fuzzy_match(value: &Value, query: &str, dist: usize) -> Option<usize> {
    match value {
        Value::String(s) => s
            .split_whitespace()
            .map(|token| levenshtein(&token.to_lowercase(), query))
            .filter(|&d| d <= dist)
            .min(),
        Value::Object(map) => map
            .values()
            .filter_map(|v| fuzzy_match(v, query, dist))
            .min(),
        Value::Array(arr) => arr.iter().filter_map(|v| fuzzy_match(v, query, dist)).min(),
        _ => {
            let d = levenshtein(&value.to_string().to_lowercase(), query);
            if d <= dist { Some(d) } else { None }
        }
    }
}
