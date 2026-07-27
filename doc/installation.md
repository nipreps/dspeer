# Installation and usage guide

## Table of Contents
1. [Overview](#1-overview)
2. [Prerequisites](#2-prerequisites)
3. [Preparing the environment](#3-preparing-the-environment)
4. [Getting the project](#4-getting-the-project)
5. [Building the Rust node](#5-building-the-rust-node)
6. [Running your first peer](#6-running-your-first-peer)
    - [6.1 Start peer (if it is the first peer of the network)](#61-start-peer-if-it-is-the-first-peer-of-the-network)
    - [6.2 Joining an existing network](#62-joining-an-existing-network)
7. [Verifying the node (HTTP API)](#7-verifying-the-node-http-api)
8. [Setting up the Python client](#8-setting-up-the-python-client)
9. [Running a notebook](#9-running-a-notebook)

## 1. Overview
This guide helps you set up the project from a fresh Linux machine (or VM) to running a first end-to-end demo in a Jupyter notebook: build the Rust node, start it as a peer, set up the Python client, and query the federated dataset.

The deployment model is one peer per machine: each institution runs a single peer process on its own machine/VM. This guide follows that model throughout.

## 2. Prerequisites
| Component | Why | Reference |
|---|---|---|
| Linux distribution | Target OS for this guide (any recent distro, tested assumptions: Debian/Ubuntu like) | - |
| Rust | Builds and runs the node | [rustup](https://rust-lang.org/tools/install/) |
| Python 3.10+ | Runs the client layer and Jupyter | [python.org download](https://www.python.org/downloads/) |
| `pip` / `venv` | Python dependency and environment management | included with Python 3 |
| `git` | Cloning the repository | your distro's package manager |

The Rust dependencies (iroh, iroh-blobs, iroh-gossip, Axum, ...) are fetched automatically by `cargo build`, and the Python dependencies by `pip install -r requirements.txt`.


## 3. Preparing the environment
`tmux` is used to keep the P2P node and the Jupyter server running independently of the terminal session, allowing you to simulate realistic disconnection/reconnection conditions without interrupting the processes currently being evaluated. It's not mandatory but very useful.  
`tmux` does not replace a real system service (`systemd`): if the VM restarts, everything stops; there is no automatic restart.
```bash
# build-essential: Compile and link native code (C, and Rust binaries)
# git: Retrieve repository and Cargo's Git dependencies
# tmux: Keep the process (node, jupyter) running
# pkg-config + libssl-dev: Securing the build against Rust dependencies that rely on C system libraries

sudo apt install -y build-essential git tmux pkg-config libssl-dev
```
Follow the [official instructions](https://rust-lang.org/tools/install/).
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
```
Once installed, open a new terminal and check:
```bash
rustc --version
cargo --version
```
Both should print a version number.

## 4. Getting the project
```bash
git clone https://github.com/VinceCor/vincent-cordola-bthesis-p2p_dataset_federation
cd vincent-cordola-bthesis-p2p_dataset_federation
```
The project tree is described in [README.md](../README.md). The two parts you will work with are `peer-dataset-federation/node/` and `peer-dataset-federation/client/`. The `doc/` folder holds the design documents. `rust-mvp-iroh-network` lets you see how the Rust implementation (iroh) was carried out; it is not needed to run the final notebook.

## 5. Building the Rust node
```bash
cd peer-dataset-federation/node
cargo build
```
The first build will take a while: `cargo` compiles iroh and its dependencies from scratch. Subsequent builds are much faster.

## 6. Running your first peer
A "peer" is a single process that both serves your local Parquet files to the network and lets you fetch files from other peers. Configuration is passed via environment variables.

| Variable | Required | Meaning |
|---|---|---|
| `INSTITUTION` | yes | Name advertised in this node's manifest (e.g. `mcgill`, `hes-so`) |
| `BOOTSTRAP_PEERS` | no | Comma-separated `EndpointId` to join an existing network. Leave unset if you are the first peer |

Place any `.parquet` file you want to share in `peer-dataset-federation/node/data/`.

### 6.1 Start peer (if it is the first peer of the network)
Then in your terminal:
```bash
# new tmux
tmux new -s node
# New peer
INSTITUTION=<INSTITUTION> cargo run -- peer
```
You should see something like:
```bash
Peer: EndpointAddr { id: PublicKey(c097e791bb83c730da69cc8c421c17b5e30909659037276ab5fe85cb6071eab5), addrs: {Ip(172.18.180.164:53468)} }
Institution: peer1
File hashing in data/
 [sample.parquet] ticket: blobadajpz4rxob4omg2nhgiyqq4c626gcijmwidoj3kwx7ils3aohvlkaaaads4zm2lu4tk7vmffi7iw2tmobht43yhjstgv4rr7xc4r3e76nkf6
Joining gossip topic with 0 bootstrap peer(s)
Gossip: manifest broadcast for institution 'peer1'
Router started. Type 'fetch <ticket>' or 'quit'
HTTP API listening on http://0.0.0.0:8080
```
The `PublicKey(...)` value in the first line is this peer's `EndpointId`; another peer will need it to join your network via `BOOTSTRAP_PEERS`.

### 6.2 Joining an existing network
If someone else already started a peer and gave you their `EndpointId`:
```bash
INSTITUTION=<INSTITUTION> BOOTSTRAP_PEERS=<their_endpoint_id> cargo run -- peer
```

Leave this terminal running: the Router, the gossip listener, and the HTTP API all live in this process.

## 7. Verifying the node (HTTP API)
With the peer still running, open a second terminal:
```bash
curl http://localhost:8080/health
# {"institution":"peer1","status":"ok"}

curl http://localhost:8080/files
# {"manifests":[{"files":[{"file_name":"sample.parquet","hash":"e5ccb34ba72...","ticket":"blobacjg2uak3..."}],"institution":"peer1"}]}
```
`/fetch` is normally called by the Python client, but you can test it directly by copying a `ticket` value from the `/files` response above:
```bash
curl -X POST http://localhost:8080/fetch \
     -H "Content-Type: application/json" \
     -d '{"ticket": "<ticket_from_files>"}'
# {"path":"cache/3b9de451872ec4a1.parquet"}
```

## 8. Setting up the Python client
```bash
cd peer-dataset-federation/client
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
`requirements.txt` covers the client library itself (`requests`, `pandas`, `duckdb`, `jupyter`, `jupyterlab`, `pyarrow`).
To leave the virtual environment later: `deactivate`.

## 9. Running a notebook
Launch Jupyter in another tmux session (so as not to block the terminal):
```bash
tmux new -s jupyter
# Make sure you're in the venv (source .venv/bin/activate)
jupyter lab
```
A minimal walkthrough, in case you want to reproduce it cell by cell:
```Python
import logging
import pandas as pd
from p2p.client import P2PClient # HTTP wrapper: talks to the Rust node
from p2p.dataset import P2PDataset # Cache + query layer: files(), load(), query(), federate()

# Show INFO-level logs (cache hits/misses, fetch calls) directly in the notebook
logging.basicConfig(level=logging.INFO)

# Connect to the local Rust node
client = P2PClient("http://localhost:8080")
# High-level entry point: everything below goes through this object
dataset = P2PDataset(client)

# 1. Discover what is available on the network
dataset.files()

# Discover in a more readable way
dataset.files_df()
# You can change the number of visible columns
dataset.files_df(2)

# 2. load a single file (fetches it into cache/ if not already there)
df = dataset.load("sample.parquet")
df.head()

# 3. Query several files independently
results = dataset.query("sample.parquet", "other_sample.parquet")
for name, df in results.items():
    print(name, df.shape)

# 4. Federate several files into one DuckDB view and run SQL across them
con = dataset.federate("sample.parquet", "other_sample.parquet")

df = con.sql("""SELECT * FROM dataset WHERE "passenger_count" > 2 """).df()

```