# data/ and data/peers_manifest/

This document explains the role of the two folders used by the node to store the files being shared and the manifests that describe them.

## `data/`

This is where you place the `.parquet` files that you want to share with the network. On startup (and on every `refresh`), the node scanas this folder, reads the footer of each file, and builds its local manifest from what it finds there.

A file must be a **valid Parquet file** (readable footer: row count, row groups, column schema) to be picked up. An invalid or corrupted file will simply not be added to the manfiest.

The project is deliberately flexible about content: there is **no schema constraint**. As long as the file follows the Parquet standard, any dataset can be dropped here and shared on the network.

## `data/peers_manifest`

This is where the JSON manifests are written, one file per institution (`<institution.json>`), including the node's own manifest. Each file is fully rewritten every time a new version is received (via gossip) or generated (local scan) for that institution.

### Manifest structure
```JSON
{
  "institution": "mtl",
  "files": [
    {
      "file_name": "yellow_tripdata_2026-02.parquet",
      "hash": "106c59bb36ce157dc4249f5713db982b217a5e514b0ac4e7be817d7cad2aa245",
      "ticket": "blobad53au7k...",
      "stats": {
        "num_rows": 3399866,
        "num_row_groups": 4,
        "file_size_bytes": 58683353,
        "columns": [
          {
            "name": "VendorID",
            "c_type": "INT32"
          },
          {
            "name": "tpep_pickup_datetime",
            "c_type": "INT64"
          },
          {
            "name": "tpep_dropoff_datetime",
            "c_type": "INT64"
          }
        ]
      }
    },
    {
      "file_name": "yellow_tripdata_2026-01.parquet",
      "hash": "aec0905b9748d9ec664e05c3cf5992a3fd6e8ae40a6878f05e7f269a919dbf45",
    // ...
```

| Field | Role |
|---|---|
| `institution` | Name of the institution that generated this manifest |
| `files[].file_name` | File name as it appears in `data/` |
| `files[].hash` | BLAKE3 hash of the file, used as a content identifier |
| `files[].ticket` | iroh ticket (hash + node address) needed to download the file |
| `files[].stats` | Metadata read from the Parquet footer (no reading of the actual data) |
| `files[].stats.columns` | Name and type of each column in the schema |

These file should never be edited by hand: they are generated automatically and overwritten on every scan or gossip reception.