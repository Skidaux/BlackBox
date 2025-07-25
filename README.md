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

### Bulk indexing

```bash
POST /indexes/<index>/bulk
Content-Type: application/json
{
  "documents": [ {"title": "a"}, {"title": "b"} ]
}
```

Allows multiple documents to be indexed in a single request.

### Set field mapping

```bash
PUT /indexes/<index>/mapping
Content-Type: application/json
{
  "fields": { "title": "string", "views": "numeric", "vector": "vector" }
}
```

Defines simple field types for an index.

### Search documents

```
GET /indexes/<index>/search?q=term
```

Returns an array of documents whose serialized JSON contains the query string.

Optional query parameters:

```
GET /indexes/<index>/search?q=term&limit=5&fuzz=1&scores=true
```

`limit` controls the number of hits returned, `fuzz` applies a simple
Levenshtein distance for matches, and `scores` includes a `score` field in the
results.

### Query DSL

```bash
POST /indexes/<index>/query
Content-Type: application/json
{
  "term": { "title": "test" },
  "sort": { "field": "views", "order": "desc" },
  "aggs": "category"
}
```

Supports simple term and range filters with optional sorting and basic count aggregation.

### Search by vector

```bash
POST /indexes/<index>/search_vector
Content-Type: application/json
{
  "vector": [0.1, 0.2, 0.3],
  "limit": 5,
  "field": "vector"
}
```

Documents may include an optional array field storing a vector embedding. The
`/search_vector` endpoint returns the nearest documents based on L2 distance
using the specified vector field. Set `scores` to `true` to include the distance
value with each hit.

## Data Storage

All indexes are saved under the `data/` directory. Each index is stored as a binary file using [`bincode`](https://crates.io/crates/bincode), which loads faster and uses less space than JSON. Data persists between server restarts.

Responses are automatically compressed with gzip when supported by the client.
