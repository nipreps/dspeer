# System Architecture
This document describes the components of the project, their responsibilities, and how they communicate. Every institution runs an identical software stack: one Rust **node** process and one Python **client** (used from a Jupyter notebook).

For network behavior under real-world conditions (NAT traversal, relay fallback, partial availability, error handling), see [evaluation.md](evaluation.md). This document focuses on structure, not behavior.

## Table of Contents

1. [Global topology](#1-global-topology)
2. [Rust node](#2-the-rust-node)
3. [Python client](#3-the-python-client)
4. [End-to-end data flow](#4-end-to-end-data-flow)
5. [Where to look next](#5-where-to-look-next)

## 1. Global topology
Each institution runs the same two processes side by side:
- a **Rust node** (`node/`), which joins the iroh network, holds the institution's Parquet files, and exposes a local HTTP API
- a **Python client** (`client/`), used from a notebook, which talks only to its own local node over HTTP and never touches iroh directly

Nodes talk to each other over iroh (direct connections, NAT traversal, relay fallback, all delegated to iroh, see [evaluation.md 2](evaluation.md#2-nat-traversal-and-relay-fallback)). Python clients never talk to each other; from the researcher's point of view, the whole federation is reachable through their own local node.

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
- `Endpoint`: the entry point of the iroh network (UDP socket, relay, hole-punching, QUIC). Everything else is built on top of it.
- `Router`: replaces a manual accept loop and dispatches incoming connections to the right protocol handler based on ALPN (`iroh_blobs::ALPN`, `iroh_gossip::ALPN`).
- `MemStore`: in-memory BLAKE3 blob store. Acts as a receive buffer during downloads and as the source when serving local files.
- `Downloader`: coordinates downloads and reuses existing QUIC connections across multiple files.

NAT traversal, relay fallback, and transfer resumability are entirely handled by iroh and are not reimplemented here - see [evaluation.md 2-4](evaluation.md#2-nat-traversal-and-relay-fallback) for details on that behavior.

### 2.2 Local file scanning
On startup (and on manual `refresh`), `build_local_manifest_files()` scans `data/*.parquet`:
1. `read_parquet_stats()` opens each file and reads **only the Parquet footer** (via the `parquet` crate) to get row count, row-group count, file size, and column names/types, without reading the actual data.
2. `store.blobs().add_path(...)` hashes the file (BLAKE3) and registers it in `MemStore`, returning a `Tag` (hash + format) that also protects the blob from the garbage collector.
3. A `BlobTicket` is built from that hash and the node's own `EndpointId`. The ticket is the only information another peer needs to fetch the file.

### 2.3 Manifest propagation via gossip
This is the mechanism that lets every node learn what every other node has, without a central registry.
- **Topic**: every node derives the same `TopicId` from a fixed string (`manifest_topic_id()`), so all institutions running this software join the same topic automatically; no coordination is needed beyond `BOOTSTRAP_PEERS`.
    > You can change this `TopicId` when creating your network
- **Manifest content:** `{ institution, files: [{ file_name, hash, ticket, stats }] }`, one entry per local Parquet file.
- **On startup:** the node waits until it has joined at least one peer (if `BOOTSTRAP_PEERS` is set), then broadcasts its own manifest once.
- **On receive:** any manifest received from the gossip topic is written to `data/peers_manifest/<institution>.json`, overwriting the previous file for that institution. This is what makes `/files` able to answer from disk without any live network call.
- **Manual refresh**: the `refresh` command in the interactive loop re-scans `data/`, rewrites the local manifest file, and re-broadcasts. Used when files are added/removed (there is no folder watcher yet).

### 2.4 HTTP API exposed to Python (Axum)
`api.rs` exposes three endpoints on `localhost:8080`, using channels (`fetch_tx`/`oneshot`) to hand off work to the async code running inside `node.rs`:
| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Node status and institution name |
| `/files` | GET | Reads every `data/peers_manifest/*.json` (including the node's own) and returns them merged - this is the federation's full catalog as seen by this node |
| `/fetch` | POST | Takes a `ticket`, forwards it to the node's fetch task, waits for the iroh download + export to `cache/` to finish, and returns the local path |

## 3. The Python client
Used from `demo.ipynb`. Two small modules, each with one job.

### 3.1 `P2PClient` HTTP wrapper
The **only** part of the Python side that knows the Rust node exists. Thin `requests` wrapper around `/health`, `/files`, `/fetch`: converts transport errors and non-200 responses into a single `P2PError`. See [evaluation.md 5.6](evaluation.md#56-logging-loggerinfo--loggerwarning) for the error-handling convention.

### 3.2 `P2PDataset` cache and dataset API
Everything a researcher calls from the notebook. It never talks HTTP directly; it goes through `P2PClient`.
| Method | Role |
|---|---|
| `files()` / `files_df()` | Flatten all peer manifests into a list / a display-friendly `pandas.DataFrame` (institution, rows, size, columns) |
| `get(file_name)` | Resolve a file to a local `Path`: check `cache/<hash[:16]>.parquet` first, call `P2PClient.fetch()` only on a cache miss |
| `load(file_name)` | `get()` + `pandas.read_parquet()`, one file, one DataFrame |
| `query(*names)` | `get()` + read for several files independently, returned as a `dict[name, DataFrame]`, no merging |
| `federate(*names)` | `get()` for several files, then creates a single DuckDB view `dataset` over all of them (`read_parquet([...])`), so they can be queried together with one SQL statement |

The cache key is derived directly from the BLAKE3 hash in the manifest (`hash[:16]`), which matches how the Rust node names files on export. This is what makes cache lookups a pure local check.

## 4. End-to-end data flow
Two flows exist for retrieving a remote file: the **CLI** path (typing `fetch <ticket>` directly in a node's terminal) and the **HTTP** path (what the notebook actually uses). Both end up calling the same `downloader.download()` + `store.blobs().export()` sequence in `node.rs`.

**Startup (per node):**
1. Scan `data/`, hash files, build local manifest
2. Start `Router` (blobs + gossip protocols)
3. Join the gossip topic, broadcast local manifest
4. Start the Axum HTTP server and the fetch task
5. Enter the interactive command loop

**Fetch, via notebook:**
1. `p2p.load("file.parquet")` -> `P2PDataset.get()`
2. `P2PClient.fetch(ticket)` -> `POST /fetch` on the local node
3. Axum hands the ticket to the fetch task via channel
4. `downloader.download(hash, peer_id)`: iroh connects to the peer named in the ticket and streams the blob, verifying it chunk by chunk against the BLAKE3 hash (see [evaluation.md 3](evaluation.md#3-behavior-under-realistic-conditions))
5. `store.blobs().export(hash, cache/...)` writes the verified blob to disk
6. The path is returned through the `oneshot` channel -> HTTP response -> `pandas.read_parquet()`

## 5. Where to look next
For behavior, see the corresponding section of [evaluation.md](evaluation.md):

| Topic | Section |
|---|---|
| NAT traversal, relay fallback, `iroh-doctor` | [2](evaluation.md#2-nat-traversal-and-relay-fallback)|
| Peers joining/leaving, partial availability, bandwidth | [3](evaluation.md#3-behavior-under-realistic-conditions) |
| Partial transfer, cache/tag behavior | [4](evaluation.md#4-reliability) |
| Error handling conventions (Rust, HTTP, Python) | [5](evaluation.md#5-error-handling) |
| Known limitations and future work | [6](evaluation.md#6-known-limitations) / [7](evaluation.md#7-future-improvement) |

> Claude chatbot was used only to correct spelling errors, when the document had been completed