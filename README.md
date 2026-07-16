# Peer-to-Peer (P2P) Dataset Federation
**Query Parquet files scattered across institutions from a single Jupyter notebook - no central server, no data upload.**

*Bachelor thesis (ISC, HES-SO Valais-Wallis) carried out at McGill University / Origami lab, Montréal, under the supervision of Prof. Oscar Esteban, Prof. Jean-Baptiste Poline and Dr. Nikhil Bhagwat.*

## Table of contents

1. [Overview](#1-overview)
2. [How it works](#2-how-it-works)
3. [Quickstart](#3-quickstart)
4. [Repository structure](#4-repository-structure)
5. [Documentation](#5-documentation)
6. [Results snapshot](#6-results-snapshot)
7. [Known limitations and future work](#7-known-limitations-and-future-work)
8. [Academic context](#8-academic-context)
9. [License](#9-license)

## 1. Overview

This project explores a different model for sharing tabular data across institutions: instead of a central server that every site uploads to, each institution keeps its Parquet files exactly where they already are, and a peer-to-peer network makes them discoverable and queryable as if they were local. A researcher opens a notebook, and the whole federation is queryable through it: no dataset has to move to a shared location, and no one has to run or maintain shared infrastructure.

The approach is not tied to any one dataset or field. The motivating use case for this thesis is sharing [MRIQC](https://mriqc.readthedocs.io/en/latest/) Image Quality Metrics across neuroimaging labs, but any group of peers willing to share Parquet files over a decentralized network can reuse this node/client pair as-is.

The peer-to-peer transport is provided by [iroh](https://www.iroh.computer/), an existing Rust networking toolkit (direct connections, NAT traversal, relay fallback). The work in this project is the layer built on top of it: a Rust node that advertises and serves files, and a Python client that makes the federation feel local from a notebook.

## 2. How it works

Every institution runs the same two processes side by side:
- a **Rust node** - joins the iroh network, hashes and serves its local Parquet files, propagates a manifest of what it has via gossip, and exposes a small local HTTP API
- a **Python client** - used from a Jupyter notebook, talks only to its own local node over HTTP, and turns the federation into a few method calls: `files()`, `load()`, `query()`, `federate()`

Full details (iroh objects, manifest propagation, HTTP API, end-to-end data flow) are in [doc/system_architecture.md](doc/system_architecture.md).

## 3. Quickstart

Full walkthrough (including prerequisites and troubleshooting) in [doc/installation.md](doc/installation.md). The essentials:
```bash
git clone https://github.com/VinceCor/vincent-cordola-bthesis-p2p_dataset_federation
cd vincent-cordola-bthesis-p2p_dataset_federation/peer-dataset-federation

# 1. Build and run the Rust node (first peer, no bootstrap needed)
cd node && cargo build
INSTITUTION=<institution> cargo run -- peer

# 2. In another terminal, set up and use the Python client
cd ../client
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
jupyter lab   # open demo.ipynb
```

## 4. Repository structure

```bash
.
├── README.md                       # you are here
├── doc/                            # design docs and write-ups
│   ├── system_architecture.md      # components, responsibilities, data flow
│   ├── installation.md             # step-by-step setup guide
│   ├── evaluation.md               # real-world test, robustness, known limitations
│   ├── axum_http_server.md         # the node's local HTTP API
│   ├── python_client_layer.md      # the Python client (client.py, dataset.py)
│   └── archive/                    # exploratory/early-stage material
│       ├── iroh_setup_guide.md     # from Rust MVP to the current iroh-gossip node
│       ├── PROJECT_MANAGEMENT.md   # work journal, planning
│       └── rust-mvp-iroh-network/  # first iroh prototypes (at the start of the project)
└── peer-dataset-federation/        # the actual project
    ├── node/                       # Rust: iroh node + Axum HTTP API
    │   ├── src/                    # main.rs, node.rs, api.rs
    │   └── data/                   # place your .parquet files here
    │       └── peers_manifest/     # manifest folder
    │           └── README.md       # explanation of the "data" folder
    └── client/                     # Python: HTTP client + notebook
        ├── p2p/                    # client.py, dataset.py
        └── demo.ipynb              # end-to-end demo notebook
```

## 5. Documentation

| Document | What it covers |
|---|---|
| [`system_architecture.md`](doc/system_architecture.md) | Components, responsibilities, end-to-end data flow |
| [`installation.md`](doc/installation.md) | Full setup guide, from a fresh machine to a running notebook |
| [`evaluation.md`](doc/evaluation.md) | Real-world test results, NAT traversal/relay behavior, reliability, error handling, known limitations |
| [`axum_http_server.md`](doc/axum_http_server.md) | The node's local HTTP API (`/health`, `/files`, `/fetch`) |
| [`python_client_layer.md`](doc/python_client_layer.md) | The Python client: `P2PClient`, `P2PDataset`, and the query layer (`load`, `query`, `federate`) |
| [`archive/iroh_setup_guide.md`](doc/archive/iroh_setup_guide.md) | Step-by-step log of how the Rust/iroh side evolved, from the first MVP to the current gossip-based node |
| [`archive/PROJECT_MANAGEMENT.md`](doc/archive/PROJECT_MANAGEMENT.md) | Work journal, planning, and meeting notes |

## 6. Results snapshot

A real-world test was run between two peers on the public internet, one in Montréal, Canada and one in Sion, Switzerland (no shared LAN, no VPN):
| Measurement | Result |
| --- | --- |
| Project setup (clone -> peer launched) | < 10 minutes |
| Manifest (gossip) propagation between the two peers | < 3 seconds |
| Transferring a 60MB Parquet file (3 million rows, 18 columns) | 6.5 seconds |

Full test conditions and additional measurements in [`evaluation.md - section 1`](doc/evaluation.md#1-real-world-test-conducted).

## 7. Known limitations and future work

This is a proof of concept: the goal was a working end-to-end demonstration, not a production-ready system. Notably, manifest refresh is manual (no folder watcher yet), there's no application-level retry on failed downloads, and the local cache has no size limit. None of these are fundamental blockers - see [`evaluation.md - section 6-7`](doc/evaluation.md#6-known-limitations).

## 8. Academic context

This project is the Bachelor thesis of Vincent Cordola (ISC, HES-SO Valais-Wallis, 2025-26), carried out at McGill University / Origami lab, Montréal, under the supervision of Prof. Oscar Esteban, Prof. Jean-Baptiste Poline and Dr. Nikhil Bhagwat. Thanks to [Nathan Antonietti](https://github.com/NathanAnto) for taking part in the real-world Montréal (Canada) and Sion (Switzerland) test.

## 9. License

No license has been set yet - this section will be updated once one is chosen.

> Claude chatbot was used to correct spelling errors and reformat the file, once the document had been completed a first time.