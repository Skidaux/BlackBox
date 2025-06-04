# BlackBox Search Server

This project provides a small HTTP server written in Rust using the [`warp`](https://crates.io/crates/warp) framework. It stores documents in named indexes on disk and supports basic full-text search.

## Building and Running

Ensure you have Rust installed, then run:

```bash
cargo run
```

The server listens on port `3000` by default. Set the `PORT` environment variable to change the port.

## API

### Add a document

```
POST /indexes/<index>/documents
Content-Type: application/json
{ "any": "json" }
```

Creates the index if it does not exist and returns the assigned document `id`.

### Search documents

```
GET /indexes/<index>/search?q=term
```

Returns an array of documents whose serialized JSON contains the query string.

## Data Storage

All indexes are saved under the `data/` directory. Each index has a JSON file containing the list of documents so data persists between server restarts.
