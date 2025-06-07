use reqwest::Client;
use serde_json::json;
use std::process::{Command, Stdio};
use tokio::time::{Duration, sleep};

fn spawn_server(port: u16) -> std::process::Child {
    let exe = env!("CARGO_BIN_EXE_blackbox");
    Command::new(exe)
        .env("PORT", port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn server")
}

fn cleanup() {
    let _ = std::fs::remove_dir_all("data");
}
async fn wait_for(port: u16) {
    let client = Client::new();
    for _ in 0..20u8 {
        if let Ok(resp) = client.get(&format!("http://localhost:{port}")).send().await {
            if resp.status().is_success() {
                break;
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_add_and_search() {
    cleanup();
    let port = 4101u16;
    let mut srv = spawn_server(port);
    wait_for(port).await;
    let client = Client::new();
    let index = "testidx1";
    let doc = json!({"title": "hello world", "vector": [1.0, 0.0]});
    client
        .post(&format!(
            "http://localhost:{port}/indexes/{index}/documents"
        ))
        .json(&doc)
        .send()
        .await
        .unwrap();

    let resp = client
        .get(&format!(
            "http://localhost:{port}/indexes/{index}/search?q=hello"
        ))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    println!("search body: {}", body);
    let res: Vec<serde_json::Value> = serde_json::from_str(&body).unwrap();
    assert_eq!(res.len(), 1);
    srv.kill().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_bulk_and_dsl() {
    cleanup();
    let port = 4102u16;
    let mut srv = spawn_server(port);
    wait_for(port).await;
    let client = Client::new();
    let index = "testidx2";
    let bulk = json!({"documents": [
        {"title": "a", "views": 10},
        {"title": "b", "views": 20},
        {"title": "c", "views": 15}
    ]});
    client
        .post(&format!("http://localhost:{port}/indexes/{index}/bulk"))
        .json(&bulk)
        .send()
        .await
        .unwrap();

    let query = json!({
        "range": {"views": {"gte": 15.0}},
        "sort": {"field": "views", "order": "desc"}
    });
    let resp = client
        .post(&format!("http://localhost:{port}/indexes/{index}/query"))
        .json(&query)
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    println!("dsl body: {}", body);
    let res: serde_json::Value = serde_json::from_str(&body).unwrap();
    let hits = res["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 2);
    assert!(
        hits[0]["document"]["views"].as_i64().unwrap()
            >= hits[1]["document"]["views"].as_i64().unwrap()
    );
    srv.kill().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_vector_search() {
    cleanup();
    let port = 4103u16;
    let mut srv = spawn_server(port);
    wait_for(port).await;
    let client = Client::new();
    let index = "testidx3";
    let docs = json!({"documents": [
        {"title": "a", "vector": [0.0, 1.0]},
        {"title": "b", "vector": [1.0, 0.0]}
    ]});
    client
        .post(&format!("http://localhost:{port}/indexes/{index}/bulk"))
        .json(&docs)
        .send()
        .await
        .unwrap();
    let body = json!({"vector": [0.9, 0.1], "k": 1});
    let resp = client
        .post(&format!(
            "http://localhost:{port}/indexes/{index}/search_vector"
        ))
        .json(&body)
        .send()
        .await
        .unwrap();
    let btext = resp.text().await.unwrap();
    println!("vector body: {}", btext);
    let res: Vec<serde_json::Value> = serde_json::from_str(&btext).unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0]["document"]["title"], "b");
    srv.kill().unwrap();
}
