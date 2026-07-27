mod node;
use iroh::{EndpointAddr, PublicKey};
use std::{env};

// Create a Tokio runtime and uses it to run `main` as an async function
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initializing log
    tracing_subscriber::fmt::init();

    // Reading the arguments and dispatching
    let args: Vec<String> = env::args().collect();

    match args.get(1).map(String::as_str) {
        Some("listen") => {
            node::listen().await?;
        }
        Some("connect") => {
            // Preparing the address in connect mode
            let id_str = args.get(2).expect("Usage: connect <EndpointId>");
            let public_key: PublicKey = id_str.parse()?;
            let addr = EndpointAddr::new(public_key);
            node::connect(addr).await?;
        }

        // file transfer
        Some("listen-blobs") => {
            // Process all .parquet files in the data/ directory. Displaying one ticket per file
            // and then waits indefinitely for incoming connections
            node::listen_blobs().await?;
        }

        Some("fetch-blobs") => {
            let tickets: Vec<String> = args[2..].to_vec();
            if tickets.is_empty() {
                eprintln!("Usage: cargo run -- fetch-blobs <ticket1> ...");
                std::process::exit(1);
            }
            node::fetch_blobs(tickets).await?;
        }

        Some("peer") => {
            node::peer().await?;
        }

        _ => {
            eprintln!("Usage:");
            eprintln!("cargo run -- listen");
            eprintln!("cargo run -- connect <EndpointId>");
            eprintln!("cargo run -- listen-blobs");
            eprintln!("cargo run -- fetch-blobs <ticket1> ...");
            eprintln!("cargo run -- peer")
        }
    }
    Ok(())

}