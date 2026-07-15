# P2PDataset
# Fetch on demand access to distributed Parquet dataset

import logging
from pathlib import Path
import pandas as pd
import duckdb

from .client import P2PClient, P2PError

# Convention from https://stackoverflow.com/questions/50714316/how-to-use-logging-getlogger-name-in-multiple-modules
logger = logging.getLogger(__name__)

cache_dir = (Path(__file__).resolve().parent / "../../node/cache").resolve()

def format_size(num_bytes: int) -> str:
    # Format in readable units (KB/MB/GB)
    size = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"

# Fetch on demand access to the distributed Parquet dataset
class P2PDataset:

    def __init__(self, client: P2PClient):
        self._client = client

    # List every file visible across all peer manifests (raw data)
    def files(self) -> list[dict]:
        response = self._client.files()
        result = []
        for manifest in response.get("manifests", []):
            institution = manifest.get("institution", "unknown")
            for f in manifest.get("files", []):
                stats = f.get("stats", {})
                result.append({
                    "file_name": f["file_name"],
                    "institution": institution,
                    "num_rows": stats.get("num_rows"),
                    "num_row_groups": stats.get("num_row_groups"),
                    "file_size_bytes": stats.get("file_size_bytes"),
                    "columns": stats.get("columns", []),
                })
        return result
    
    # A human-readable version of 'files()', designed for display in a notebook
    # Use 'files()' if you need the raw data
    # Claude chatbot was use as an help for this function
    def files_df(self, max_columns_shown: int = 8) -> pd.DataFrame:
        rows = self.files()
        if not rows:
            return pd.DataFrame(columns=["file_name", "institution", "num_rows", "num_row_groups", "size", "columns"])
        
        def format_columns(cols):
            names = [c["name"] for c in cols]
            if len(names) > max_columns_shown:
                shown = ", ".join(names[:max_columns_shown])
                return f"{shown}, ... (+{len(names) - max_columns_shown} more)"
            return ", ".join(names)

        df = pd.DataFrame(rows)
        df["size"] = df["file_size_bytes"].apply(format_size)
        df["columns"] = df["columns"].apply(format_columns)
        df = df.drop(columns=["file_size_bytes"])
        return df[["file_name", "institution", "num_rows", "num_row_groups", "size", "columns"]]



    # Return a local Path to a *file_name*, fetching it from the network if it is not already cached.
    def get(self, file_name: str) -> Path | None:
        # Walk all peer manifests to find the file
        response = self._client.files()
        entry = None
        for manifest in response.get("manifests", []):
            for f in manifest.get("files", []):
                if f["file_name"] == file_name:
                    entry = f | {"institution": manifest.get("institution", "unknown")}
                    break
            if entry is not None:
                break
            
        if entry is None:
            logger.warning("'%s' not found in any peer manifest", file_name)
            return None
        
        # Derive the expected cache path from the BLAKE3 hash
        cached = cache_dir / f"{entry['hash'][:16]}.parquet"

        if cached.exists():
            logger.info("cache hit: %s", file_name)
            return cached
        
        # Not on disk: fetch via the Rust node
        logger.info("fetching '%s' from %s ...", file_name, entry["institution"])
        self._client.fetch(entry["ticket"])
        logger.info("saved -> %s", cached)

        return cached
    
    # Load a single file from the network into pandas DataFrame
    def load(self, file_name: str) -> pd.DataFrame:
        path = self.get(file_name)
        if path is None:
            raise P2PError(f"'{file_name}' not found on the network")
        logger.info("loading '%s' into dataframe", file_name)
        return pd.read_parquet(path)
    
    # Fetch a specific set of files, chosen by name, from the network
    # Each file is resolved and fetchted independently (no merging)
    def query(self, *file_names: str) -> dict[str, pd.DataFrame]:
        if not file_names:
            raise P2PError("query() requires at least one file name, e.g. query('sample.parquet')")
        
        results: dict[str, pd.DataFrame] = {}
        for name in file_names:
            path = self.get(name)
            if path is None:
                logger.warning("'%s' not found on the network, skipping", name)
                continue
            results[name] = pd.read_parquet(path)
        return results
    
    # Fetch a specific set of files, chosen by name, and expose them together 
    # as a single DuckDB view called 'dataset', so they can be queried with one SQL statement across peers.
    def federate(self, *file_names: str) -> duckdb.DuckDBPyConnection:
        if not file_names:
            raise P2PError("federate() requires at least one file name, e.g. federate('sample.parquet')")
        
        paths = []
        for name in file_names:
            path = self.get(name)
            if path is None:
                logger.warning("'%s' not found on the network, skipping,", name)
                continue

            paths.append(str(path))

        if not paths:
            raise P2PError("none of the requested files could be found on the network")
        
        con = duckdb.connect()
        paths_str = [str(p) for p in paths]
        con.execute(f"CREATE VIEW dataset AS SELECT * FROM read_parquet({paths_str!r})")
        logger.info("federated view 'dataset' created from %d file(s)", len(paths))
        return con

    # Convenience function built on top of federate(): runs the exact same SQL a researcher would write by hand,
    # but hides it behind a plain method call.
    # Shows how project specific shortcuts can ba added on top of the federated view without requiring the researcher
    # to know SQL.
    def filter_passenger(self, con: duckdb.DuckDBPyConnection, min_passengers: int) -> pd.DataFrame:
        return con.sql(f'SELECT * FROM dataset WHERE "passenger_count" > {min_passengers}').df()