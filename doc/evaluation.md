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
> References: [Gossip broadcast](https://docs.iroh.computer/connecting/gossip), [iroh-gossip crate](https://docs.rs/iroh-gossip/latest/iroh_gossip/), [NeighborUp, NeighborDown](https://docs.rs/iroh-gossip/latest/iroh_gossip/net/type.ProtoEvent.html), [protocole proto hyParView, PlumTree](https://docs.rs/iroh-gossip/latest/iroh_gossip/proto/index.html)

> References: [iroh blobs](https://docs.iroh.computer/protocols/blobs), [Blob store design](https://www.iroh.computer/blog/blob-store-design-challenges)
## 3. Behavior under realistic conditions


## 4. Reliability

## 5. Known limitations