# Python HTTP client

## 1. Overview
> References: [Requests](https://requests.readthedocs.io/en/latest/)

The Rust node exposes three HTTP endpoints (`/health`, `/files`, `/fetch`), documented in the Axum HTTP server guide. `P2PClient` is the only piece of Python code that knows this node exists. It does not interpret the data it receives, it does not decide whteher a file should be downloade, in only translates Python method calls into HTTP requests and HTTP responses into Python values.

## 2. Dependencies
`requests` the standard synchronous HTTP library for Python. Chosen over `AIOHTTP` or `HTTPX` because the project doesn't need async HTTP calls on the Python side.
```bash
pip install requests
```

## 3. client.py
### 3.1 P2PError
```Python
class P2PError(Exception):
    # Raised when the Rust node returns an error or is unreachable
    pass
```
The caller (`P2PDataset`, or the researcher directly in a notebook) only needs to know "the node call failed", not whether it was a connection error, a timeout, or an HTTP 500. Catching one exception type is simple than catching three.

### 3.2 The constructor
```Python
def __init__(self, base_url: str = "http://localhost:8080", timeout: int = 30):
    # base_url -> base url of the Rust node
    self.base_url = base_url.rstrip("/")
    # timeout -> Request timeout in seconds
    self.timeout = timeout
```
`base_url.rstrip("/")` removes a trailing slash if the user passes `http://localhost:8080/`. This avoids accidentally producing `http://localhost:8080//` when concatenating with `"/health"` later.    
`timeout` defaults to 30 seconds. `GET /health` and `GET /files` are fast (local disk reads), but `POST /fetch` triggers an actual iroh-blobs download, which depends on file size and network conditions (direct connection vs relay fallback). A single shared timeout keeps the class simple for the PoC, if `/fetch` later needs a longer timeout than `/health`, this can be split.

### 3.3 health(), files(), fetch()
These three public methods are the entire surface of the class. Each one calls a private helper (`get` or `post`) and shapes the return value
```Python
# GET /health
# Check that the Rust node is running
def health(self) -> dict:
    return self.get("/health")

# GET /files
# Return the merged manifest: local files + all peers manifests
def files(self) -> dict:
    return self.get("/files")

# POST /fetch
# Ask the Rust node to download the blob identified by <ticket>
def fetch(self, ticket: str) -> str:
    data = self.post("/fetch", {"ticket": ticket})
    return data["path"]
```
`health()` and `files()` return the JSON body.  
`fetch()` is the only method that unwraps the response. The Rust node returns `{"path": "cache/<hash>.parquet"}`, returning `data["path"]` directly instead of the whole dict.

### 3.4 Private helpers
> References: [requests errors and exceptions](https://docs.python-requests.org/en/latest/user/quickstart/#errors-and-exceptions), [requests timeout](https://docs.python-requests.org/en/latest/user/advanced/#timeouts)

This is the part of the file doing the actual work, `get` and `post` are nearly identical, which is intentional.
```Python
def get(self, path: str) -> dict:
    url = self.base_url + path

    try:
        response = requests.get(url, timeout=self.timeout)
    except requests.exceptions.ConnectionError as e:
        raise P2PError(f"Cannot reach the Rust node at {self.base_url}") from e
    except requests.exceptions.Timeout:
        raise P2PError(f"Request timed out after {self.timeout}s: GET {url}")

    self.raise_for_status(response)
    return response.json()
```
```Python
def post(self, path: str, body: dict) -> dict:
    url = self.base_url + path

    try:
        response = requests.post(url, json=body, timeout=self.timeout)
    except requests.exceptions.ConnectionError as e:
        raise P2PError(f"Cannot reach the Rust node at {self.base_url}") from e
    except requests.exceptions.Timeout:
        raise P2PError(f"Request timed out after {self.timeout}s: GET {url}")
    
    self.raise_for_status(response)
    return response.json()
```

For two call sites with identical error handling, the duplication here is small enough that splitting by verb keeps each method readable without an extra layer of indirection. If a third HTTP verb were needed, merging into one `request()` would become the better trade-off.   
**Two layers of error handling** The `try/except` block catches transport-level failures.
- `ConnectionError`: the node isn't listening on that port (not started, wrong port, firewall)
- `Timeout`: the node is reachable but didn't answer within `self.timeout` seconds

`self.raise_for_status(response)`, called after the `try/except`, catches application-level failures, the node did repond, but with a non 200 status (ex. the Axum `/fetch` handler returning `500` because the ticket was invalid)

### 3.5 raise for status
```Python
@staticmethod
def raise_for_status(response: requests.Response) -> None:
    if response.status_code != 200:
        try:
            detail = response.json().get("error", response.text)
        except Exception:
            detail = response.text
        raise P2PError(f"Rust node returned HTTP {response.status_code}: {detail}")
```
It inspects a `Response` object and converts a non 200 status into a `P2PError` with a useful message. It tries to extract the "error" filed the Rust API puts in its JSON error bodies, and falls back to the raw response text if the body isn't JSON, or doesn't contain that field.


### 3.6 Result
#### 3.61 Install python library
If you haven't alreadym install the requests library.
```bash
# Example of how to reate a .venv
python3 -m venv .venv

source .venv/bin/activate

pip install requests
# leave
deactivate
```

#### 3.62 Manual test
With the Rust node running (`INSTITUTION=peer1 cargo run --peer`)   
`python3 test_client.py`
```Python
# Test client
from p2p.client import P2PClient

client = P2PClient("http://localhost:8080")
print(client.health())

print(client.files())
```

#### 3.63 Testing the error path
With the node stopped:
```Python
try:
    client.health()
except P2PError as e:
    print(e)
```

## 4. dataset.py

### 4.1 Dependencies

| Import | Role |
|---|---|
| `P2PClient` | HTTP calls to the Rust node |
| `P2PError` | Exception propagated on network failure |
| `logging` | Standard Python logging |
| `pathlib.Path` | Filesystem path manipulation |

### 4.2 Constants
> References: [python loggin getLogger](https://docs.python.org/3/library/logging.html#logging.getLogger)

`logging.getLogger(__name__)` creates a logger named `p2p.dataset`
```Python
# Convention from https://stackoverflow.com/questions/50714316/how-to-use-logging-getlogger-name-in-multiple-modules
logger = logging.getLogger(__name__)

cache_dir = (Path(__file__).resolve().parent / "../../node/cache").resolve()
```

`cache_dir` locates `node/cache/` from `__file__` (the absolute path of `dataset.py` at import time), navigating `../../node/cache` relative to `client/p2p`. This works regardless of the directory from which the notebook is launched.

### 4.3 `files()`

Calls `GET /files` once and flattens the nested manifest structure into a simple list.
```Python
def files(self) -> list[dict]:
    response = self._client.files()
    return [
        {"file_name": f["file_name"], "institution": manifest.get("institution", "unknown")}
        for manifest in response.get("manifests", [])
        for f in manifest.get("files", [])
    ]
```

### 4.4 `get()`
**Why an explicit loop here instead of a list comprehension?**  
`get()` needs to stop as soon as a match is found (`break`). List comprehension always walk the entire list. An explicit loop with `break` is more efficient and clearer when early exit is the goal.
```Python
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
```
`entry = f | {"institution": manifest.get("institution", "unknown")}`   
`f` is the file dict from the manifest. The `|` operator merges two dicts into a new one. The result is a flat dict with all four fileds, go `get()` can wrote `entry["hash"]` and `entry["ticket"]` directly.

```Python
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
```
`entry['hash'][:16]` takes the first 16 character of the BLAKE3 hash. The Rust node uses this exact slice to name the exported file. `cached.exists()` checks the disk, if the file is there, it is already a verified Parquet file and can be returned immediately with no network call.   
`self._client.fetch(entry["ticket"])` triggers `POST /fetch` on the Rust node. The return value (the relative path string) is not used here: `chached` was already computed from the hash and points to the same file. If the fetch fails, `P2PClient` raises `P2PError`, which propagates to the notebook, the user sees the full error rather than a silent `None`.

### 4.5 Result
Example of use
```Python
import logging
import pandas as pd
from p2p.client import P2PClient
from p2p.dataset import P2PDataset

logging.basicConfig(level=logging.INFO)

client = P2PClient("http://localhost:8080")
dataset = P2PDataset(client)

print(dataset.files())

path = dataset.get("sample.parquet")

df = pd.read_parquet(path)

df.head()

```

## 5. Python query layer: load() and query()

### 5.1 load()
`load()` is a thin wrapper around `get()`. If `get()` returns `None`, the file does not exist in any peer manifest, `load()` turns this into `P2PError` rather than silently returning `None`, so the failure is visible immediately in the notebook. `pd.read_parquet(path)` ready the file directoy from `cache/`, no network call happens here.
```Python
# Load a single file from the network into pandas DataFrame
def load(self, file_name: str) -> pd.DataFrame:
    path = self.get(file_name)
    if path is None:
        raise P2PError(f"'{file_name}' not found on the network")
    logger.info("loading '%s' into dataframe", file_name)
    return pd.read_parquet(path)
```
Use in notebook
```Python
# load(): single file as a DataFrame
df = dataset.load("iris.parquet")
df.head()
```

### 5.2 query()
`query()` takes one or more file names as arguments, for example `data.query("sample.parquer", "orther_sample.parquet")`. The researcher is expected to call `dataset.files()` first to see what's available on the network, then pass the name they actually want.
```Python
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
```
Use in notebook
```Python
results = dataset.query("iris.parquet","titanic.parquet","test.parquet")

for name, df in results.items():
    print(name, df.shape)
```

### 5.3 federate()
`federate()` mirrors `query()`s argument handling on purpose: at leat one file name required, each name resolved through `get()`, missing files logged as a warning and skipped rather tan failing the whole call. The difference is only in what happens after the files are on disk.  
`read_parquet()` accepts a list of paths and unions them by column name into one relation, exposed as the view `dataset`.

```Python
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
```
Use in notebook
```Python
con = dataset.federate("yellow_tripdata_2026-01.parquet","yellow_tripdata_2026-02.parquet")

df2 = con.sql("""SELECT * FROM dataset WHERE "passenger_count" > 2 """).df()
```

### 5.4 `filter_passenger()` example of a custom function
`filter_passenger()` illustrates the value of `federate()`: once the dataset view has been created, you can write Python methods that encapsulate a specific SQL query, so that the user doesn't need to know SQL for their recurring use cases. It's the same principle as `load()`/`query()` compared to `get()`: a convenience layer built on top of a more general building block.
```Python
# Convenience function built on top of federate(): runs the exact same SQL a researcher would write by hand,
# but hides it behind a plain method call.
# Shows how project specific shortcuts can ba added on top of the federated view without requiring the researcher
# to know SQL.
def filter_passenger(self, con: duckdb.DuckDBPyConnection, min_passengers: int) -> pd.DataFrame:
    return con.sql(f'SELECT * FROM dataset WHERE "passenger_count" > {min_passengers}').df()
```
`con` is the connection returned by `federate()`, so the dataset view must already exist on that connection. `filter_passenger()` does nothing more than construct the same as the one used manually in the demo notebook. It simply saves the researcher from having to write SQL for the specific filter.     
use in the notebook:
```Python
con = dataset.federate("yellow_tripdata_2026-01.parquet","yellow_tripdata_2026-02.parquet")

df = dataset.filter_passenger(con,3 )
```