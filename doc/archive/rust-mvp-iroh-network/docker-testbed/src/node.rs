// Code taken from rust-mvp (peer function)
use iroh::{Endpoint, EndpointId, SecretKey, endpoint::presets, protocol::Router};
use iroh_blobs::{BlobsProtocol, store::mem::MemStore, ticket::BlobTicket};
use iroh_gossip::{api::Event, net::Gossip, proto::TopicId};
use n0_error::{Result, StdResultExt};
use sha2::{Digest, Sha256};
use serde::{Deserialize, Serialize};
use std::{env, path::PathBuf};

// Manifest
// The manifest describes what this node possesses: its institution name and,
// for each local .parquet file, its real filename, its BLAKE3 hash, and the
// BlobTicket needed to fetch it. It is broadcast on a shared gossip topic so every peer ends up
// with a copy of every other peer's manifest

// One entry of the manifest, single Parquet file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestFile {
    pub file_name: String,
    pub hash: String,
    pub ticket: String,
}

// The manifest broadcast by a peer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub institution: String,
    pub files: Vec<ManifestFile>,
}

// Derives a stable 32-byte gossip TopicId
// https://docs.iroh.computer/connecting/gossip#picking-a-topic-id
fn manifest_topic_id() -> TopicId {
    let mut hasher = Sha256::new();
    hasher.update(b"p2p-parquet/manifest/v1");
    let hash = hasher.finalize();
    TopicId::from_bytes(hash.into())
}

// For bootstrap_peers_from_env() and build_local_manifest_files claude chatbot was used to help me create these functions

// Reads BOOTSTRAP_PEERS environment variable
// An empty Vec means first peer, nobody to bootstrap from
fn bootstrap_peers_from_env() -> Result<Vec<EndpointId>> {
    let raw = match env::var("BOOTSTRAP_PEERS") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => return Ok(Vec::new()),
    };

    let mut peers = Vec::new();
    for id_str in raw.split(',') {
        let id_str = id_str.trim();
        if id_str.is_empty() {
            continue;
        }
        let id: EndpointId = id_str.parse().std_context("Invalid BOOTSTRAP_PEERS entry")?;
        peers.push(id);
    }
    Ok(peers)
}

// Scans /data for .parquet files, hashes each one into `store`, and returns the resulting manifest entries.
// Same scan/hash as `peer()`, just collected into a Vec<ManifestFile> instead of only printing
async fn build_local_manifest_files(store: &MemStore, endpoint_id: EndpointId) -> Result<Vec<ManifestFile>> {
    let data_dir = PathBuf::from("data");
    let mut entries = tokio::fs::read_dir(&data_dir)
        .await
        .std_context("Unable to read /data folder")?;

    let mut files = Vec::new();

    while let Some(entry) = entries.next_entry().await.std_context("Error reading entry")? {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
            let abs_path = path.canonicalize().std_context("Absolute path not found")?;
            let filename = path.file_name().unwrap().to_string_lossy().to_string();

            // add_path hashes the file and returns a tag (hash + format)
            // The "tag" prevents the store's garbage collector from deleting the blob
            let tag = store
                .blobs()
                .add_path(abs_path)
                .await
                .std_context("Error during hashing")?;

            // BlobTicket -> Blake 3 hash of the file + listener's EndpointId
            let ticket = BlobTicket::new(endpoint_id.into(), tag.hash, tag.format);

            files.push(ManifestFile {
                file_name: filename,
                hash: tag.hash.to_string(),
                ticket: ticket.to_string(),
            });
        }
    }
    Ok(files)
}

// Writes a received manifest to data/peers_manifest/<institution>.json so that the Cache layer
// can read it as a plain local file, same as the local manifest.
// Overwrites the previous file for that institution, which keeps the folder updated.
async fn save_peer_manifest(manifest: &Manifest) -> Result<()> {
    let peers_dir = PathBuf::from("data/peers_manifest");
    tokio::fs::create_dir_all(&peers_dir)
        .await
        .std_context("Unable to create data/peers_manifest/")?;

    let dest = peers_dir.join(format!("{}.json", manifest.institution));
    let json = serde_json::to_string_pretty(manifest).std_context("Error serializing manifest")?;

    tokio::fs::write(&dest,json)
        .await
        .std_context("Unable to write peer manifest")?;

    Ok(())
}



// peer: single process that simultaneously serves local files and accepts interactive fetch commands.
// 
// Configuration is read from the environment
// INSTITUTION  | required, the name advertised in this node's manifest
// BOOTSTRAP_PEERS | optional, EndpointIds already in the gossip swarm, leave empty for the first peer
//
// Commands:
//      - fetch <ticket> | download the file identified by the ticket
//      - quit           | shutdown
pub async fn peer() -> Result<()> {
    let institution = env::var("INSTITUTION").std_context("INSTITUTION environment variable is required")?;
    let bootstrap = bootstrap_peers_from_env()?;

    // Shared endpoint and store
    let endpoint = Endpoint::bind(presets::N0).await?;
    println!("Peer: {:?}", endpoint.addr());
    println!("Institution: {institution}");

    let store = MemStore::new();

    // hash local files
    let blobs = BlobsProtocol::new(&store, None);

    
    println!("File hashing in data/");
    let local_files = build_local_manifest_files(&store, endpoint.id()).await?;
    for f in &local_files {
        println!(" [{}] ticket: {}", f.file_name, f.ticket);
    }

    // Build the gossip protocol and register it on the same Router as iroh-blobs
    let gossip = Gossip::builder().spawn(endpoint.clone());

    // Start the Router
    // .spawn() launches the accept loop as a background Tokio task
    // The Router stays alive as long as this variable is in scope
    let router = Router::builder(endpoint.clone())
        .accept(iroh_blobs::ALPN, blobs)
        .accept(iroh_gossip::ALPN, gossip.clone())
        .spawn();

    // All peer derive the same TopicId from a fixed, shared string
    let topic_id = manifest_topic_id();

    println!("Joining gossip topic with {} bootstrap peer(s)",bootstrap.len());

    let (sender, mut receiver) = gossip
        .subscribe(topic_id, bootstrap.clone())
        .await
        .std_context("Error subscibing to gossip topic")?
        .split();

    let local_manifest = Manifest {
        institution: institution.clone(),
        files: local_files, 
    };

    // Also persist our own manifest locally
    save_peer_manifest(&local_manifest).await?;

    // Spawn a background task that:
    // 1. waits until at least one peer has joined the topic
    // 2. broadcasts our manifest once connected
    // 3. then listens for manifests broadcast by other peers and saves them to data/peers_manifest
    let gossip_task = tokio::spawn(async move {
        if !bootstrap.is_empty() {
            if let Err(e) = receiver.joined().await {
                eprintln!("Gossip: error waiting for peers to join: {e}");
            }
        }

        let payload = match serde_json::to_vec(&local_manifest) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("Gossip: error serializing local manifest: {e}");
                return;
            }
        };

        if let Err(e) = sender.broadcast(payload.into()).await {
            eprintln!("Gossip: error broadcasting manifest: {e}");
        } else {
            println!("Gossip: manifest broadcast for institution '{}'", local_manifest.institution);
        }

        // n0_future::StreamExt is required for '.next()' on the gossip receiver.
        // For the while let Some(event) claude chatbot was used to help
        use n0_future::StreamExt;
        while let Some(event) = receiver.next().await {
            match event {
                Ok(Event::Received(message)) => {
                    match serde_json::from_slice::<Manifest>(&message.content) {
                        Ok(manifest) => {
                            println!(
                                "Gossip: received manifest from '{}' ({} file(s))",
                                manifest.institution,
                                manifest.files.len()
                            );
                            if let Err(e) = save_peer_manifest(&manifest).await {
                                eprintln!("Gossip: error saving peer manifest: {e}");
                            }
                        }
                        Err(e) => {
                            eprintln!("Gossip: received message is not a valid manifest: {e}");
                        }
                    }
                }
                Ok(_) => {
                    // Other event kinds (neighbor up/down, etc.) are ignored for this PoC.
                }
                Err(e) => {
                    eprintln!("Gossip: stream error: {e}");
                    break;
                }
            }
        }
    });


    // Prepare cache/ and the downloader once, reused for every fetch command
    let cache_dir = PathBuf::from("cache");
    tokio::fs::create_dir_all(&cache_dir)
        .await
        .std_context("Unable to crate cache/")?;

    let downloader = store.downloader(&endpoint);

    // Interactive command loop
    // tokio::io::BufReader wraps stdin so that read_line().await yields control back to the Tokio runtime while waiting for input
    // instead of blocking the thread. This is what allow the Router to keep serving incoming connections
    println!("Router started. Type 'fetch <ticket>' or 'quit'");

    let stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let mut lines = tokio::io::AsyncBufReadExt::lines(stdin);

    // lines.next_line().await block asynchronously until the user presses Enter
    // The loop exits when next_line() returns Non (stdin closed) or on "quit"
    // This section was created with the help of Claude chatbot
    while let Some(line) = lines.next_line().await.std_context("Error reading stdin")? {
        let line = line.trim().to_string();

        match line.split_once(' ') {
            Some(("fetch", raw_ticket)) => {
                let ticket: BlobTicket = match raw_ticket.trim().parse() {
                    Ok(t) => t,
                    Err(e) => {
                        println!("Invalid ticket: {e}");
                        print!("> ");
                        continue;
                    }
                };

                // Download the blob from the provider identified in the ticket
                match downloader
                    .download(ticket.hash(),Some(ticket.addr().id))
                    .await {
                        Ok(_) => {
                            //
                            let filename = format!("{}.parquet", &ticket.hash().to_string()[..16]);
                            let dest = cache_dir
                                .canonicalize()
                                .std_context("Cannot resolve cache/")?
                                .join(&filename);
                            match store.blobs().export(ticket.hash(), dest).await {
                                Ok(_) => println!("-> cache/{filename}"),
                                Err(e) => println!("Export error: {e}"),
                            }
                        }
                        Err(e) => println!("Download error: {e}"),
                }
            }
            // quit
            _ if line == "quit" => {
                break;
            }

            // unkown command
            _ if line.is_empty() => {}
            _ => println!("Unkown command")
        }

        print!("> ")
    }


    // Shutdown
    gossip_task.abort();
    router.shutdown().await.std_context("Error shutdown")?;
    endpoint.close().await;

    Ok(())
}