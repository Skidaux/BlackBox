use reqwest::Client;
use serde_json::json;
use serial_test::serial;
use std::process::{Command, Stdio};
use tokio::time::{Duration, sleep};

fn spawn_server(port: u16) -> std::process::Child {
    let exe = env!("CARGO_BIN_EXE_blackbox");
    Command::new(exe)
        .env("PORT", port.to_string())
        .env("RUST_LOG", "info")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
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

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn test_add_and_search() {
    cleanup();
    let port = 4501u16;
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
            "http://localhost:{port}/indexes/{index}/search?q=hell&fuzz=1&scores=true"
        ))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    println!("search body: {}", body);
    let res: serde_json::Value = serde_json::from_str(&body).unwrap();
    let hits = res["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 1);
    assert!(hits[0]["score"].as_f64().unwrap() > 0.0);
    srv.kill().unwrap();
    let _ = srv.wait();
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn test_search_limit() {
    cleanup();
    let port = 4601u16;
    let mut srv = spawn_server(port);
    wait_for(port).await;
    let client = Client::new();
    let index = "limitidx";
    let bulk = json!({"documents": [
        {"title": "foo"},
        {"title": "bar"},
        {"title": "baz"}
    ]});
    client
        .post(&format!("http://localhost:{port}/indexes/{index}/bulk"))
        .json(&bulk)
        .send()
        .await
        .unwrap();

    let resp = client
        .get(&format!("http://localhost:{port}/indexes/{index}/search?q=a&limit=1"))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    println!("limit body: {}", body);
    let res: serde_json::Value = serde_json::from_str(&body).unwrap();
    let hits = res["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 1);
    srv.kill().unwrap();
    let _ = srv.wait();
}

#[serial]
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
        "sort": {"field": "views", "order": "desc"},
        "aggs": "views",
        "limit": 2,
        "scores": true
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
    assert!(hits[0]["score"].is_number());
    assert!(
        hits[0]["document"]["views"].as_i64().unwrap()
            >= hits[1]["document"]["views"].as_i64().unwrap()
    );
    let aggs = res["aggregations"].as_object().unwrap();
    assert_eq!(aggs.get("20").unwrap().as_i64().unwrap(), 1);
    srv.kill().unwrap();
    let _ = srv.wait();
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn test_dsl_fuzzy() {
    cleanup();
    let port = 4602u16;
    let mut srv = spawn_server(port);
    wait_for(port).await;
    let client = Client::new();
    let index = "fuzzyidx";
    let doc = json!({"title": "hello"});
    client
        .post(&format!("http://localhost:{port}/indexes/{index}/documents"))
        .json(&doc)
        .send()
        .await
        .unwrap();

    let q = json!({"term": {"title": "hello"}, "fuzz": 1, "scores": true});
    let resp = client
        .post(&format!("http://localhost:{port}/indexes/{index}/query"))
        .json(&q)
        .send()
        .await
        .unwrap();
    let text = resp.text().await.unwrap();
    println!("dsl fuzzy body: {}", text);
    let res: serde_json::Value = serde_json::from_str(&text).unwrap();
    let hits = res["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 1);
    assert!(hits[0]["score"].as_f64().unwrap() > 0.0);
    srv.kill().unwrap();
    let _ = srv.wait();
}

#[serial]
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
    let body = json!({"vector": [0.9, 0.1], "limit": 1, "scores": true});
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
    let res: serde_json::Value = serde_json::from_str(&btext).unwrap();
    let hits = res["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0]["document"]["title"], "b");
    assert!(hits[0]["score"].is_number());
    srv.kill().unwrap();
    let _ = srv.wait();
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn test_persistence() {
    cleanup();
    let port1 = 4304u16;
    let port2 = 4404u16;
    let mut srv = spawn_server(port1);
    wait_for(port1).await;
    let client = Client::new();
    let index = "persistidx";
    let doc = json!({"title": "persist"});
    client
        .post(&format!(
            "http://localhost:{port1}/indexes/{index}/documents"
        ))
        .json(&doc)
        .send()
        .await
        .unwrap();
    sleep(Duration::from_millis(100)).await;
    srv.kill().unwrap();
    let _ = srv.wait();

    let mut srv = spawn_server(port2);
    wait_for(port2).await;
    let resp = client
        .get(&format!(
            "http://localhost:{port2}/indexes/{index}/search?q=persist"
        ))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    println!("persist body: {}", body);
    let res: serde_json::Value = serde_json::from_str(&body).unwrap();
    let hits = res["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 1);
    srv.kill().unwrap();
    let _ = srv.wait();
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn test_vector_custom_field() {
    cleanup();
    let port = 4105u16;
    let mut srv = spawn_server(port);
    wait_for(port).await;
    let client = Client::new();
    let index = "vecidx";
    let mapping = json!({"fields": {"embedding": "vector"}});
    client
        .put(&format!("http://localhost:{port}/indexes/{index}/mapping"))
        .json(&mapping)
        .send()
        .await
        .unwrap();
    let docs = json!({"documents": [
        {"title": "foo", "embedding": [0.0, 1.0]},
        {"title": "bar", "embedding": [1.0, 0.0]}
    ]});
    client
        .post(&format!("http://localhost:{port}/indexes/{index}/bulk"))
        .json(&docs)
        .send()
        .await
        .unwrap();
    let q = json!({"vector": [0.9, 0.1], "limit": 1, "field": "embedding", "scores": true});
    let resp = client
        .post(&format!(
            "http://localhost:{port}/indexes/{index}/search_vector"
        ))
        .json(&q)
        .send()
        .await
        .unwrap();
    let text = resp.text().await.unwrap();
    println!("custom vector body: {}", text);
    let res: serde_json::Value = serde_json::from_str(&text).unwrap();
    let hits = res["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0]["document"]["title"], "bar");
    assert!(hits[0]["score"].is_number());
    srv.kill().unwrap();
    let _ = srv.wait();
}
