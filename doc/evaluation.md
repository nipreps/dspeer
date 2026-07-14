# Evaluation, robustness and real-world conditions
This document outlines the various features and currently known limitations.

## 1. Real-world test conducted
To start with, here is a test conducted between two peers, one in Canada Montréal and one in Switzerland Sion (Thanks to [Nathan Antonietti](https://github.com/NathanAnto) for his time)

| Parameters | Values |
| --- | --- |
| OS / environment | WSL (Montréal), Arch linux (Switzerland) |
| Network link | Public internet (no shared LAN / No VPN) |

**Result obtained**
| Measurement | Result |
| --- | --- |
| Project setup (clone -> peer launched) | < 10 minutes |
| Manifest (gossip) propagation between the two peers | < 3 seconds |
| Transferring a 60MB Parquet file (3 million rows, 18 columns) | 6.5 seconds |
> Note that installation is a one time process, after that, launching the peer takes less than 2 seconds as long as the terminal is open (and it continues to run until the machine is shut down).

The goal of this test was to see if two distant peers could connect and send data to each other.    
In the following screenshots, you can first see the internet connection for the two peers. And in the terminal, we also have confirmation that the two peers were able to send an receive manifests.
![mtl_speed](media/mtl_speed.png)
![swiss_speed](media/swiss_speed.png)
From the peer in Montreal, I retrieved one of the files that the Switzerland peer had (the name, ticket etc... were included in the manifest exchanged earlier).    
And you can see that retrieving and displaying the file took only 6.5 seconds (60 MB, 3 milion lines, and 18 columns)
![demo_fetch](media/demo_fetch.png)


## 2. NAT traversal and relay fallback
> References: [Gossip broadcast](https://docs.iroh.computer/connecting/gossip), [iroh-gossip crate](https://docs.rs/iroh-gossip/latest/iroh_gossip/), [NeighborUp, NeighborDown](https://docs.rs/iroh-gossip/latest/iroh_gossip/net/type.ProtoEvent.html), [protocole proto hyParView, PlumTree](https://docs.rs/iroh-gossip/latest/iroh_gossip/proto/index.html), [iroh Relays](https://docs.iroh.computer/concepts/relays), [iroh Troubleshooting](https://docs.iroh.computer/troubleshooting)

The behavior is entirely delegated to iroh (`Endpoint::bind(presets::N0)`). No NAT/relay logic is written directly into this project. The procedure is as follows: During setup, each endpoint connects to its nearest relay ([home relay](https://docs.iroh.computer/concepts/relays)) and registers itself as reachable there, it is through this relay that two endpoints establish initial contact, before attempting a direct hole-punch in parallel. If the hole-punch fails (for example, due to overly restrictive NAT on both sides), traffic continues to pass through the relay in a way that is transparent to the application, no error is visible in the code, just higher latency.

To see which route your peers are using, you can use [iroh-doctor](https://github.com/n0-computer/iroh-doctor). `iroh-doctor` is a diagnostic tool, it transfers data between the two machines and reports in real time whether the connetion is direct, relayed, or a combination of the two as the transfer progess


## 3. Behavior under realistic conditions
> References: [iroh blobs](https://docs.iroh.computer/protocols/blobs), [Blob store design](https://www.iroh.computer/blog/blob-store-design-challenges), [blobs protocol](https://docs.iroh.computer/protocols/blobs)

`iroh-gossip` separate two layers: a membership layer (HyParView) that maintains a partial view of the swarm and detects direct neighbors that appear or disapperar, and a broadcast layer (PlumTree) that propagates messages redundantly so that they reach peers even in the event of join, leave, or message loss. This is explicitly documented as the rationale behind the protocol: the broadcast layer accepts that each node has only a partial view of the network and uses probabilistic relaying, since peers can joine, leave, change addresses, or lose messages at any time.

**Peers joining / leaving**     
`bootstrap_peers_from_env()` allows a new peer to join via `BOOTSTRAP_PEERS`. A peer that exits (Ctrl+C / quit) simply calls `router.shutdown()`. One addition to this project would be the ability to check whether remote peers are actually online.

**Partial availability**    
Currently, manifest sharing is not automated. The manifest is updated only at startup, or via the manual `refresh` command. If a peers adds or remove a `.parquet` file from its `data/` folder at any point, the other peers won't know about it until that peer performs a `refresh`. An addition to this project would be the automatic detection of these changes so that the user no longer has to do it.

**Variable bandwith**   
The transfer of a Parquet file uses BLAKE3-verified streaming: the data is divided into chunks, and each chunk is verified on the fly during reception rather than afterward. This means that available bandwidth directly and linearly translates to transfer time (there is no separate verification phase that would be added after the download), but it also means that there is no automatic adaptation (no compression, no quality negotiation), the project simply operates at the available bandwidth and does not adapt to it.

## 4. Reliability
> References: [iroh blobs](https://docs.iroh.computer/protocols/blobs), [blob store design](https://www.iroh.computer/blog/blob-store-design-challenges)
**Partial transfer reinstatement**  
This capability comes entirely from iroh-blobs, not from any logic written in this project.

## 5. Known limitations