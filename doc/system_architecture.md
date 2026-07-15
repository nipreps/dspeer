# System Architecture
This document describes the components of the project, their responsibilities, and how they communicate. Every institution runs an identical software stack: one Rust **node** process and one Python **client** (used from a Jupyter notebook).

For network behavior under real-world conditions (NAT traversal, relay fallback, partial availability, error handling), see [evaluation.md](evaluation.md). This document focuses on structure, not behavior.

## Table of Contents

## 1. Global topology
Each institution runs the same two processes side by side:
- a **Rust node** (`node/`), which joins the iroh network, holds the institution's Parquet files, and exposes a local HTTP API
- a **Python client** (`client/`), used from a notebook, which talks only to its own local node over HTTP and never touches iroh directly
Nodes talk to each other over iroh (direct connections, NAT traversal, relay fallback, all delegated to iroh, see [evaluation.md](evaluation.md)). Python clients never talk to each other, from the researcher's point of view, the whole federation is reachable through their own local node.

## 2. The Rust node
The node is a single Tokio process (`peer()` in `node.rs`) that does four things concurrently: serve blobs, propagate manifests, run an HTTP API, and accept interactive commands.
| Component | Role | References |
|---|---|---|
| Rust + Tokio | Asynchronous runtime | [tokio.rs](https://tokio.rs/) |
| iroh | P2P network toolkit (endpoint, NAT traversal, relay) | [iroh](https://www.iroh.computer/) |
| iroh-blobs | Content-addressed blob storage and transfer | [iroh-blobs](https://docs.iroh.computer/protocols/blobs) |
| iroh-gossip | Manifest propagation between nodes | [iroh-gossip](https://docs.iroh.computer/connecting/gossip) |
| Axum | Local HTTP API consumed by the Python client | [axum](https://github.com/tokio-rs/axum) |

### 2.1 iroh objects
Four iroh objects are created once at startup and shared for the life of the process:
- `Endpoint:` the entry point of the iroh network (UDP socket, relay, hole-punching, QUIC). Everything else is built on top of it.
- `Router:` replaces a manual accept loop, dispatches incoming connections to the right protocol handler based on ALPN(`iroh_blobs::ALPN`. `iroh_gossip::ALPN`).
- `MemStore:` in-memory BLAKE3 blob store. Acts as a receive buffer during downloads and as the source when serving local files.
- `Downloader:` coordinates downloads and reuses existing QUIC connections across multiple files.

NAT traversal, relay fallback, and transfer resumability are entirely handled by iroh and are not reimplemented here, see [evaluation.md](evaluation.md) for detailes on that behavior.

### 2.2 Local file scanning
On startup (and on manual `refresh`), `build_local_manifest_files()` scans `data/*.parquet`:
1. `read_parquet_stats()` opens each file and reads **only the Parquet footer** (via the `parquet` crate) to get rown count, row-group count, file size, and column names/types, without reading the actual data.
2. `store.blobs().add_path(...)` hashes the file (BLAKE3) and registers it in `MemStore`, returning a `Tag` (hash + format) that also protects the blob from the garbage collector.
3. A `BlobTicket` is built from that hash and the node's own `EndpointId`. The ticket is the only information another peer needs to fetch the file.

### 2.3 Manifest propagation via gossip
This is the mechanism that lets every node learn what every other node has, without a central registry.
- **Topic**: every node derives the same `TopicId` from a fixed string (`manifest_topic_id()`), so all institutions running this software join the same topic automatically, no coordination needed beyond `BOOTSTRAP_PEERS`.
    > You can change this `TopicId` to create your network
- **Manifest content:** `{ institution, files: [{ file_name, hash, ticket, stats }] }`, one entry per local Parquet file.
- **On startup:** the node waits until it has joined at least one peer (if `BOOTSTRAP_PEERS` is set), then broadcasts its own manifest once.
- **On receive:** any manifest received from the gossip topic is written to `data/peers_manifest/<institution>.json`, overwriting the previous file for that institution. This is what make `/files` able to answer from disk without any live network call.
- **Manual refresh**: the `refresh` command in the onteractive loop re-scans `data/`, rewrites the local manifest file, and re-broadcasts, used when files are added/removed (there is no folder watcher yet)

### 2.4 HTTP API exposed to Python (Axum)

## 3. The Python client

## 4. End-to-end data flow

## 5. Where to look next