# Axum http server
In this document, we'll look at how to add the Axum HTTP server to our Rust node.

## Table of Contents
1. [Overview](#1-overview)
2. [Dependencies](#2-dependencies)
3. [api.rs](#3-apirs)
    - [3.1 Shared state and channels](#31-shared-state-and-channels)
    - [3.2 GET /health](#32-get-health)
    - [3.3 GET /files](#33-get-files)
    - [3.4 POST /fetch](#34-post-fetch)
    - [3.5 serve()](#35-serve)
4. [Changes to node.rs](#4-changes-to-noders)
    - [4.1 Import api.rs](#41-import-apirs)
    - [4.2 Launch Axum and the fetch task](#42-launch-axum-and-the-fetch-task)
5. [Changes to main.rs](#5-changes-to-mainrs)
6. [Result](#6-result)
    - [6.1 Build and run](#61-build-and-run)
    - [6.2 Test the endpoints](#62-test-the-endpoints)


## 1. Overview
The Rust node currently runs entirely in the terminal: it servers files, broadcasts its manifest via gossip, and accepts interactive `fetch` commands. The Python client layer (coming next) needs a way to talk to the node without a terminal. The solution is a local HTTP API.

Three endpoint are exposed:
| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Returns the node status and institution name |
| `/files` | GET | Reads all manifests from `data/peers_manifest` and returns them merged |
| `/fetch` | POST | Triggers an iroh-blobs download by ticket, returns the local cache path |

**Why Axum?**   
Axum is developed by the Tokio team and runs on the same async runtime already use by iroh.

## 2. Dependencies
`axum = "0.8"` Lightweight HTTP framework build on Tokio. Handles routing, extractors, and JSON responses.

## 3. api.rs
> References: [axum repos](https://github.com/tokio-rs/axum), [axum extract](https://docs.rs/axum/latest/axum/extract/index.html), [tokio mpsc](https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html), [tokio oneshot](https://docs.rs/tokio/latest/tokio/sync/oneshot/index.html)

### 3.1 Shared state and channels
`AppState`
```Rust
pub struct AppState {
    pub institution: String,
    // Forward fetch requests to node.rs
    // The actual iroh download run there, this filed is the bridge
    pub fetch_tx: tokio::sync::mpsc::Sender<FetchRequest>,
}
```
Axum handlers are plain async functions. To give them access to node data, Axum use an extractor called `State`. Every handler that declares `State<Arc<AppState>>` in its arguments receives a clone of the `Arc`, not a copy of the data.

`FetchRequest`
```Rust
pub struct FetchRequest {
    pub ticket: String,
    // One-shot channel: node.rs sends back the result (filename or error)
    pub reply: tokio::sync::oneshot::Sender<Result<String, String>>,
}
```
A `oneshot` channel is  a single-use channel: the sender fires once, the receiver gets exactly one value. Here it is used to carry the download result from the fetch task back to the HTTP handler. Without it, the handler would have no way to know when the download finished.

### 3.2 GET /health
```Rust
// GET /health
// Returns the node status and institution name
async fn health(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(json!({"status": "ok", "institution": state.institution,}))
}
```
`Json(json!({...}))` serialize the value directly to an HTTP response with `Content-type: application/json`. The json! macro from `serde_json` builds the `Value` inline.

### 3.3 GET /files
```Rust
// GET /files
// Reads all JSON manifests in data/peers_manifest/ and returns them merged
// This section was created with the help of Claude chatbot
async fn files() -> Result<Json<Value>, StatusCode> {
    let peers_dir = PathBuf::from("data/peers_manifest");

    let mut manifests: Vec<Value> = Vec::new();

    let mut entries = tokio::fs::read_dir(&peers_dir).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    while let Some(entry) = entries.next_entry().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("json") {
            let content = tokio::fs::read_to_string(&path).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let manifest: Manifest = serde_json::from_str(&content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            manifests.push(json!(manifest));
        }
    }
    Ok(Json(json!({"manifests": manifests})))
}
```
`Result<Json<Value>, StatusCode` is the return type. Axum converts both variants into HTTP repsons. The `?` operator propagates errors directly to the `Err` variant.   
`tokio::fs::read_dir` and `tokio::fs::read_to_string` are the async versions of the standard library equivalents. Using them here keeps the Tokio runtime free to handle other requests while the disl I/O is in progress.  
`serde_json::from_str` deserializes the JSON file into a `Manifest` struct(defined in `node.rs`).

### 3.4 POST /fetch
```Rust
// POST /fetch
// Sends the request to node.rs via channel and waits for the result
async fn fetch(State(state): State<Arc<AppState>>, Json(body): Json<FetchBody>) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let (reply_br, reply_rx) = tokio::sync::oneshot::channel();

    let req = FetchRequest {
        ticket: body.ticket,
        reply: reply_br,
    };

    // Send fetch request to node.rs
    state.fetch_tx.send(req).await.map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "fetch channel closed"})),
        )
    })?;

    // Wait for node.rs to complete the download
    match reply_rx.await {
        Ok(Ok(filename)) => Ok(Json(json!({"path": format!("cache/{}", filename)}))),
        Ok(Err(e)) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e}))
        )),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "reply channel dropped"}))
        )),
    }
}
```
`Json(body): Json<FetchBody>` is the Axum body extractor. It reads the request body, deserializes it as JSON into `FetchBody`, and injects the result.  
`tokio::sync::oneshot::channel()` creates a matched sender/receiver pair. The sender goes into `FetchRequest` and is forwarder to the fetch task.   
`state.fetch_tx.send(req).await` puts the request into the `mpsc` channel.

### 3.5 serve()
```Rust
// Starts the Axum server on port 8080
// Called from node.rs as a background tokio::spawn task
pub async fn serve(state: Arc<AppState>) {
    let app = Router::new()
        .route("/health", get(health))
        .route("/files", get(files))
        .route("/fetch", post(fetch))
        .with_state(state);

    let listener = TcpListener::bind("0.0.0.0:8080").await.unwrap();
    println!("HTTP API listening on http://0.0.0.0:8080");
    axum::serve(listener, app).await.unwrap();
}
```
`Router::new()` builds the route table. `.route("/health", get(health))` register the `health` function as the handler for `GET /health`. `.with_state(state)` makes the `Arc<AppState>` available to every handler that declares a `State` extractor.  
`TcpListener::bind("0.0.0.0:8080")` binds to all interface on port 8080.    
`axum::serve(listener, app).await` runs the server indefinitely. Because `serve()` is called inside `tokio::spawn`, it runs as a background task and does not block the rest of `peer()`.

## 4. Changes to node.rs
Three additions are made to `peer()`. Nothing else changes.

### 4.1 Import api.rs
At the top of `node.rs` add:
```Rust
use crate::api::{AppState, FetchRequest, serve};
use std::sync::Arc;
```
`crate::api` refers to `api.rs` in the same `src/` directory. [Arc](https://doc.rust-lang.org/std/sync/struct.Arc.html) (Atomically Reference Counted) is the standard Rust mechanism for sharing ownership of data across multiple async tasks safely.

### 4.2 Launch Axum and the fetch task
This block was add in `peer()`, after the Router is started and `cache_dir` is created, and before the stdin loop.
```Rust
// Channel: Axum /fetch handler -> fetch task
// Capacity 32: up to 32 requests can be queued before the sender blocks
let (fetch_tx, mut fetch_rx) = tokio::sync::mpsc::channel::<FetchRequest>(32);

// Start the HTTP server as a background task
let api_state = Arc::new(AppState {
    institution: institution.clone(),
    fetch_tx,
});
tokio::spawn(serve(api_state));

// Fetch task: receives FetechRequests from the HTTP handler and runs the iroh download
// Runs concurrently with the Router and the gossip task
let fetch_downloader = store.downloader(&endpoint);
let fetch_cache_dir = cache_dir.clone();
let fetch_store = store.clone();
tokio::spawn(async move {
    while let Some(req) = fetch_rx.recv().await {
        let result = async {
            let ticket: iroh_blobs::ticket::BlobTicket = req.ticket
                .parse()
                .map_err(|e| format!("Invalid ticket: {e}"))?;

            fetch_downloader
                .download(ticket.hash(), Some(ticket.addr().id))
                .await
                .map_err(|e| format!("Download error: {e}"))?;

            let filename = format!("{}.parquet", &ticket.hash().to_string()[..16]);
            let dest = fetch_cache_dir
                .canonicalize()
                .map_err(|e| format!("Cache path error: {e}"))?
                .join(&filename);

            fetch_store
                .blobs()
                .export(ticket.hash(), dest)
                .await
                .map_err(|e| format!("Export error: {e}"))?;

            Ok::<String, String>(filename)
        }
        .await;

        // Send the result back to the HTTP handler via the oneshot channel
        let _ = req.reply.send(result);
    }
});
```
`mpsc::channel::<FetchRequest>(32)`     
`mpsc` stands for Multiple Producer Single Consumer. Here only one producer exists (the Axum handler), but `mpsc` is the right tool because it supports backpressure, if 32 requests are already queued, the 33rd `.send().await` will wait until a slot is free.

`tokio::spawn(serve(api_state))`    
`tokio::spawn` launches the Axum server as an independent Tokio task. It runs concurrently with everything else.

**The fetch task**  
`fetch_rx.recv().await` blocks asynchronously until a `fetchRequest` arrives. The download logic is identical to the existing stdin `fetch` command. The only difference is that the result is sent back over `req.reply` instead of being printed to stdout.

`let _ = req.reply.send(result)`    
The return value of `oneshot::Sender::send` is discarded with `let _`. If the HTTP client disconnected before the download finished, the receiver would have been dropped and `send` would return an error.

## 5. Changes to main.rs
One line is added at the top of `main.rs` to declare the new module:
```Rust
mod api;
```

## 6. Result
### 6.1 Build and run
Go to the `node` folder
```bash
cargo build
```
then run
```bash
# <institution> -> name of your peer
INSTITUTION=<institution> cargo run -- peer
```
You should see the HTTP server start alongside the rest of the node, example:
```bash
Peer: EndpointAddr { id: PublicKey(bf121c2bbb96a55b17b676db6de5428d11a63de88f7b21a3f8765933a9050a8f), addrs: {Ip(172.18.180.164:56444)} }
Institution: peer1
File hashing in data/
 [sample.parquet] ticket: blobac7rehblxolkkwyxwz3nw3pfikgrdjr55chxwind7b3fsm5jaufi6aaaaa5z3zcrq4xmjii4jdglhmk66gnraa4s57eyxnb77hn4frvrqfjmo
Joining gossip topic with 0 bootstrap peer(s)
Gossip: manifest broadcast for institution 'peer1'
Router started. Type 'fetch <ticket>' or 'quit'
HTTP API listening on http://0.0.0.0:8080
```

### 6.2 Test the endpoints
Open a second terminal and run the following commands:  
**GET /health**
```bash
curl http://localhost:8080/health
```
Expected response
```bash
{"institution":"<institution>","status":"ok"}
```

**GET files**
```bash
curl http://localhost:8080/files
```
Expected response
```json
{
  "manifests": [
    {
      "institution": "<institution>",
      "files": [
        { "file_name": "sample.parquet", "hash": "3b9de4...", "ticket": "blob..." }
      ]
    }
  ]
}
```

**POST /fetch**
```bash
curl -X POST http://localhost:8080/fetch \
     -H "Content-Type: application/json" \
     -d '{"ticket": "<ticket_from_files>"}'
```
Expected response
```bash
{"path":"cache/3b9de451872ec4a1.parquet"}
```