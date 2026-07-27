// HTTP API exposed to the Python client
// Three endpoints
//  GET /health -> node status and institution name
//  GET /files -> merged manifest
//  POST /fetch -> trigger an iroh-blobs download by ticket

use axum::{Router,extract::State,http::StatusCode,response::Json,routing::{get, post}};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};
use tokio::net::TcpListener;

use crate::node::Manifest;

// Shared state passed to every handler by Axum
pub struct AppState {
    pub institution: String,
    // Forward fetch requests to node.rs
    // The actual iroh download run there, this filed is the bridge
    pub fetch_tx: tokio::sync::mpsc::Sender<FetchRequest>,
}

// Body expected by POST /fetch
#[derive(Deserialize)]
pub struct FetchBody {
    pub ticket: String,
}

// What api.rs sends to node.rs to trigger a download
pub struct FetchRequest {
    pub ticket: String,
    // One-shot channel: node.rs sends back the result (filename or error)
    pub reply: tokio::sync::oneshot::Sender<Result<String, String>>,
}

// GET /health
// Returns the node status and institution name
async fn health(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(json!({"status": "ok", "institution": state.institution,}))
}


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