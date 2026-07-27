# P2PClient
# Thin HTTP wrapper around the 3 Rust node endpoints
# This is the only part of the Python layer that knows the Rust node exists

# References
# https://docs.python-requests.org/en/latest/ | https://realpython.com/python-requests/
# 
# GET /health   -> node status and institution name
# GET /files    -> merged manifest (local + peers)
# POST /fetch   -> trigger an iroh-blobs download by ticket

import requests

class P2PError(Exception):
    # Raised when the Rust node returns an error or is unreachable
    pass

# Minimal HTTP client for the local Rust node API
class P2PClient:

    def __init__(self, base_url: str = "http://localhost:8080", timeout: int = 30):
        # base_url -> base url of the Rust node
        self.base_url = base_url.rstrip("/")
        # timeout -> Request timeout in seconds
        self.timeout = timeout

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

    # Private helpers

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

    @staticmethod
    def raise_for_status(response: requests.Response) -> None:
        if response.status_code != 200:
            try:
                detail = response.json().get("error", response.text)
            except Exception:
                detail = response.text
            raise P2PError(f"Rust node returned HTTP {response.status_code}: {detail}")