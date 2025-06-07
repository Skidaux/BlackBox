use serde_json::Value;

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
