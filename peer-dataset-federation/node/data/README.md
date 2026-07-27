# `data/`

This is where you place the `.parquet` files this node shares with the network.
On startup, and on every `refresh`, the node scans this folder, reads the footer of
each file, and builds its local manifest from what it finds.

A file must be a valid Parquet file (readable footer: row count, row groups, column
schema) to be picked up. There is no schema constraint: any dataset that follows the
Parquet standard can be dropped here and shared.

The folder is deliberately empty in version control. `*.parquet` is gitignored so an
operator's data never lands in a commit by accident.

See [`peers_manifest/README.md`](peers_manifest/README.md) for how the manifests
written alongside your data are structured.

## Reproducing the original evaluation

The two-peer Montréal to Sion measurements in [`doc/evaluation.md`](../../../doc/evaluation.md)
were taken with two months of the NYC Taxi and Limousine Commission yellow-taxi trip
records, roughly 60 MB each, 3 million rows and 18 columns. To reproduce them, fetch
the same files into this folder:

```bash
curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet
curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-02.parquet
```

Any two reasonably sized Parquet files will do; nothing in the node is specific to
this dataset. The TLC records are convenient because they are public, large enough to
make transfer time observable, and stable.
