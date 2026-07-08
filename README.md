# dspeer

**Self-hostable, peer-to-peer federation of tabular datasets.**

*dspeer* lets a group of independent parties share collections of
[Apache Parquet](https://parquet.apache.org/) files across an ad-hoc
peer-to-peer network and query the federated collection from standard
dataframe tooling — [PyArrow](https://arrow.apache.org/docs/python/),
[pandas](https://pandas.pydata.org/), [DuckDB](https://duckdb.org/) — as if
it were a single local dataset. There is no central server and no data
hand-over: every peer hosts its own data and remains in full control of it.

## Why

Communities that produce structured data — scientific consortia, monitoring
networks, distributed teams — usually share it by uploading everything to a
central platform. That single point of aggregation is also a single point of
failure, cost, policy, and control. *dspeer* explores the alternative: keep
the data where it is produced, advertise it to a network of peers, and let
consumers assemble the federated collection on demand.

The motivating use case comes from neuroimaging: quality-control metrics
produced by [MRIQC](https://mriqc.readthedocs.io/) at thousands of sites
worldwide, currently pooled through a centralized web API. A peer-to-peer
federation lets institutions exchange these metrics directly — including
between sites that cannot ship data to third-party infrastructure — while the
same design serves any community with tabular data to share. Nothing in
*dspeer* is specific to neuroimaging.

## How it works

*dspeer* deliberately builds on existing, well-tested primitives rather than
inventing new protocol machinery. The stack has three layers:

### 1. Transport — [iroh](https://www.iroh.computer/)

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

A thin agent runs on each peer. It joins the ad-hoc network, advertises the
Parquet files available locally, and serves them to (and fetches them from)
other peers. The agent is intentionally minimal — the intelligence lives in
the dataset conventions and the client layer, not in a bespoke protocol.

### 3. Python client

The user-facing layer is a Python library designed for interactive use
(e.g., from a Jupyter notebook):

- **discovers** the files that make up a federated dataset across all
  reachable peers,
- **fetches** missing files on demand into a local cache,
- **exposes** the assembled collection as a single dataset to PyArrow,
  pandas, or DuckDB — queries spanning files hosted by multiple peers look
  exactly like queries on a local directory of Parquet files.

Datasets follow a documented **layout convention**: a manifest describes the
schema, the partition inventory, and version tags, so clients can reason
about what exists, what is local, and what still needs to be retrieved.

## Design principles

- **Self-hostable by anyone.** A peer is a process you run on your own
  machine, not an account on someone's platform. Deployment should be a
  single, declarative step.
- **Working end-to-end over protocol novelty.** The project composes proven
  building blocks (iroh, Parquet, Arrow) and focuses effort on the seams —
  discovery, caching, and the dataframe-facing API.
- **Tolerant of real networks.** Peers join and leave; availability is
  partial; bandwidth varies. The federation must degrade gracefully:
  what is reachable is queryable, transfers resume rather than restart, and
  the cache keeps previously fetched data usable offline.
- **Integrity by construction.** Content addressing means a file fetched
  from an untrusted peer is exactly the file that was advertised, or it is
  rejected.

## Status

Early development — a proof of concept is being built and evaluated. The
current milestone plan:

1. **Core sharing prototype** — join an ad-hoc iroh network, advertise local
   Parquet files, fetch files from peers; validated on a Docker Compose
   testbed of 3–5 containers.
2. **Python/Jupyter integration** — the unified client API with on-demand
   retrieval and local caching; testbed moved to virtual machines with
   separate network stacks to exercise NAT traversal and relay fallback.
3. **Robustness under churn** — peers joining/leaving, partial availability,
   varying bandwidth; resumable transfers, cache behaviour, error handling.
4. **Reproducible demonstration** — a notebook that discovers a federated
   dataset and runs multi-peer queries, plus setup scripts for reproducing
   the demo on 2–5 physical machines.

A known open question is partial reads: Parquet readers use a footer-first
access pattern, and matching it to `iroh-blobs`' verified range requests has
not been validated yet. The baseline design therefore fetches whole files
into the cache — sufficient at proof-of-concept scale — and treats byte-range
optimization as a later refinement.

## Relationship to NiPreps

*dspeer* is developed within the [NiPreps](https://www.nipreps.org/)
community, where its first deployment target is the federation of MRIQC
image-quality metrics across institutions. It is developed as
general-purpose data infrastructure, independent of any particular pipeline.

## License

[Apache License 2.0](LICENSE).
