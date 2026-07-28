# dspeer

**Self-hostable, peer-to-peer federation of tabular datasets.**

*dspeer* lets a group of independent parties share collections of
[Apache Parquet](https://parquet.apache.org/) files across an ad-hoc
peer-to-peer network and query the federated collection from standard
dataframe tooling ([PyArrow](https://arrow.apache.org/docs/python/),
[pandas](https://pandas.pydata.org/), [DuckDB](https://duckdb.org/)) as if
it were a single local dataset. There is no central server and no data
hand-over: every peer hosts its own data and remains in full control of it.

## Why

Communities that produce structured data, such as scientific consortia,
monitoring networks and distributed teams, usually share it by uploading
everything to a central platform. That single point of aggregation is also a
single point of failure, cost, policy, and control. *dspeer* explores the
alternative: keep the data where it is produced, advertise it to a network of
peers, and let consumers assemble the federated collection on demand.

The motivating use case comes from neuroimaging: quality-control metrics
produced by [MRIQC](https://mriqc.readthedocs.io/) at thousands of sites
worldwide, currently pooled through a centralized web API. A peer-to-peer
federation lets institutions exchange these metrics directly, including
between sites that cannot ship data to third-party infrastructure, while the
same design serves any community with tabular data to share. Nothing in
*dspeer* is specific to neuroimaging.

## How it works

*dspeer* deliberately builds on existing, well-tested primitives rather than
inventing new protocol machinery. The stack has three layers.

### 1. Transport: [iroh](https://www.iroh.computer/)

Peer-to-peer connectivity is provided by iroh, a Rust networking toolkit:

- **Content-addressed transfer** (`iroh-blobs`): files are addressed and
  verified by the hash of their content, so any peer can serve any file it
  holds and receivers verify integrity by construction.
- **Encrypted QUIC connections** between peers, with **NAT traversal** (hole
  punching) and **relay fallback** so peers behind home or institutional
  routers can still participate.
- **Gossip-based discovery** (`iroh-gossip`): peers announce which datasets
  and files they hold to the swarm; there is no tracker or registry to
  operate.

### 2. Node agent

A thin Rust agent runs on each peer. It joins the ad-hoc network, scans its
local `data/` folder, reads each Parquet footer to build a manifest, serves
those files to (and fetches them from) other peers, propagates its manifest by
gossip, and exposes a small local HTTP API. The agent is intentionally
minimal: the intelligence lives in the dataset conventions and the client
layer, not in a bespoke protocol.

### 3. Python client

The user-facing layer is a Python library designed for interactive use, for
example from a Jupyter notebook. It talks only to its own local node over
HTTP, and turns the federation into four calls:

- `files()` discovers what is available across all reachable peers,
- `load()` fetches a single file into a `pandas.DataFrame`,
- `query()` fetches several files independently,
- `federate()` exposes several files as one DuckDB view, so a query spanning
  files hosted by different peers looks exactly like a query on a local
  directory of Parquet files.

Datasets follow a documented layout convention: a manifest describes the
schema, the partition inventory, and version tags, so clients can reason about
what exists, what is local, and what still needs to be retrieved.

## Design principles

- **Self-hostable by anyone.** A peer is a process you run on your own
  machine, not an account on someone's platform. Deployment should be a
  single, declarative step.
- **Working end-to-end over protocol novelty.** The project composes proven
  building blocks (iroh, Parquet, Arrow) and focuses effort on the seams:
  discovery, caching, and the dataframe-facing API.
- **Tolerant of real networks.** Peers join and leave; availability is
  partial; bandwidth varies. The federation must degrade gracefully: what is
  reachable is queryable, transfers resume rather than restart, and the cache
  keeps previously fetched data usable offline.
- **Integrity by construction.** Content addressing means a file fetched
  from an untrusted peer is exactly the file that was advertised, or it is
  rejected.

## Quickstart

Full walkthrough, including prerequisites and troubleshooting, in
[`doc/installation.md`](doc/installation.md). The essentials:

```bash
git clone https://github.com/nipreps/dspeer
cd dspeer/peer-dataset-federation

# 1. Build and run the Rust node (first peer, no bootstrap needed)
cd node && cargo build
INSTITUTION=<institution> cargo run -- peer

# 2. In another terminal, set up and use the Python client
cd ../client
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
jupyter lab   # open demo.ipynb
```

Drop your own `.parquet` files into `peer-dataset-federation/node/data/`; see
[that folder's README](peer-dataset-federation/node/data/README.md) for the
conventions and for how to reproduce the measurements below.

## Demo notebook

Once your peer is running and the Python environment is set up,
[`peer-dataset-federation/client/demo.ipynb`](peer-dataset-federation/client/demo.ipynb)
is the fastest way to see the federation in action. Cell by cell it walks
through the whole client API:

- `dataset.files()` / `dataset.files_df()`: discover what is available across
  all connected peers
- `dataset.load(file_name)`: fetch a single file into a `pandas.DataFrame`
- `dataset.query(*file_names)`: fetch several files independently
- `dataset.federate(*file_names)`: expose several files as one DuckDB view and
  query them together with SQL

No prior knowledge of iroh, the Rust node, or the HTTP API is needed to follow
it. From the notebook's point of view, the federation is just data.

## Repository structure

```bash
.
├── README.md                       # you are here
├── LICENSE                         # Apache License 2.0
├── NOTICE                          # attribution
├── doc/                            # design docs and write-ups
│   ├── system_architecture.md      # components, responsibilities, data flow
│   ├── installation.md             # step-by-step setup guide
│   ├── evaluation.md               # real-world test, robustness, known limitations
│   ├── axum_http_server.md         # the node's local HTTP API
│   ├── python_client_layer.md      # the Python client (client.py, dataset.py)
│   └── archive/                    # exploratory and early-stage material
│       ├── iroh_setup_guide.md     # from Rust MVP to the current iroh-gossip node
│       ├── PROJECT_MANAGEMENT.md   # work journal, planning
│       └── rust-mvp-iroh-network/  # first iroh prototypes
└── peer-dataset-federation/
    ├── node/                       # Rust: iroh node + Axum HTTP API
    │   ├── src/                    # main.rs, node.rs, api.rs
    │   └── data/                   # place your .parquet files here
    │       └── peers_manifest/     # manifests, one JSON per institution
    └── client/                     # Python: HTTP client + notebook
        ├── p2p/                    # client.py, dataset.py
        └── demo.ipynb              # end-to-end demo notebook
```

## Documentation

| Document | What it covers |
|---|---|
| [`system_architecture.md`](doc/system_architecture.md) | Components, responsibilities, end-to-end data flow |
| [`installation.md`](doc/installation.md) | Full setup guide, from a fresh machine to a running notebook |
| [`evaluation.md`](doc/evaluation.md) | Real-world test results, NAT traversal and relay behaviour, reliability, error handling, known limitations |
| [`axum_http_server.md`](doc/axum_http_server.md) | The node's local HTTP API (`/health`, `/files`, `/fetch`) |
| [`python_client_layer.md`](doc/python_client_layer.md) | The Python client: `P2PClient`, `P2PDataset`, and the query layer |
| [`archive/iroh_setup_guide.md`](doc/archive/iroh_setup_guide.md) | Step-by-step log of how the Rust/iroh side evolved, from the first MVP to the current gossip-based node |
| [`archive/PROJECT_MANAGEMENT.md`](doc/archive/PROJECT_MANAGEMENT.md) | Work journal, planning, and meeting notes |

## Status: what works today

This is a working proof of concept, not a production system. What runs:

- a Rust node that joins an ad-hoc iroh network, scans a local folder, reads
  each Parquet footer for row count, row groups and column schema, and serves
  the files content-addressed over `iroh-blobs`;
- manifest propagation between peers over `iroh-gossip`, one JSON manifest per
  institution;
- a local Axum HTTP API (`/health`, `/files`, `/fetch`) that the client talks
  to, so the client never speaks the peer-to-peer protocol itself;
- a Python client with `files()`, `load()`, `query()` and `federate()`, the
  last exposing several peers' files as a single DuckDB view;
- an end-to-end demo notebook.

Measured between two peers on the public internet, one in Montréal, Canada and
one in Sion, Switzerland, with no shared LAN and no VPN:

| Measurement | Result |
| --- | --- |
| Project setup, clone to peer launched | < 10 minutes |
| Manifest (gossip) propagation between the two peers | < 3 seconds |
| Transferring a 60 MB Parquet file (3 million rows, 18 columns) | 6.5 seconds |

Test conditions, NAT-traversal and relay-fallback behaviour, and the
reliability and error-handling analysis are in
[`doc/evaluation.md`](doc/evaluation.md).

Known limitations, none of them fundamental: manifest refresh is manual, with
no folder watcher; there is no application-level retry on a failed download;
the local cache has no size limit; and partial (byte-range) reads of Parquet
files are not implemented, so a query fetches whole files.

## Roadmap

The work planned on top of this prototype is tracked in the
[issue tracker](https://github.com/nipreps/dspeer/issues), grouped into three
directions: making the **node** durable, describable and access-controlled;
making it **deployable** declaratively and free of any mandatory third party;
and making it **operable** by someone who did not write it.

## Origins and credits

The codebase in this repository began as the Bachelor thesis of
**Vincent Cordola** (ISC, HES-SO Valais-Wallis, 2025-26), carried out at
McGill University / Origami lab, Montréal, under the supervision of
Oscar Esteban, Jean-Baptiste Poline and Nikhil Bhagwat. Thanks to
[Nathan Antonietti](https://github.com/NathanAnto) for taking part in the
Montréal to Sion test.

The full development history is preserved here commit by commit. The original
repository is
[VinceCor/vincent-cordola-bthesis-p2p_dataset_federation](https://github.com/VinceCor/vincent-cordola-bthesis-p2p_dataset_federation);
the state submitted as the thesis is tagged `official_submit` there, at commit
`c65ec18`, and the same point is tagged `official_submit` here. The code was
released by its author under the Apache License 2.0 before being imported.

One difference from the source repository: two 60 MB NYC TLC test fixtures were
removed from this repository's history, because they were most of its download
weight and none of its code. Every commit, author, date and message is intact,
but commit ids from 2026-07-13 onward therefore differ from the originals. The
source repository above remains the byte-exact archive of the thesis.

Parts of the original documentation were language-edited with an AI assistant
after being written; the same applies to portions of this README.

## Relationship to NiPreps

*dspeer* is developed within the [NiPreps](https://www.nipreps.org/)
community, where its first deployment target is the federation of MRIQC
image-quality metrics across institutions. It is developed as general-purpose
data infrastructure, independent of any particular pipeline.

## License

[Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for attribution.
