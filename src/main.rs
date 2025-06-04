use warp::Filter;

#[tokio::main]
async fn main() {
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3000);

    // Route that returns "Hello world" on the root path
    let hello = warp::path::end().map(|| "Hello world");

    println!("Server running on port {}", port);
    warp::serve(hello).run(([0, 0, 0, 0], port)).await;
}
