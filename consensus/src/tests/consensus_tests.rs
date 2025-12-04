use super::*;
use crate::common::{committee_with_base_port, keys};
use crate::config::Parameters;
use crypto::{Digest, SecretKey};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use futures::future::try_join_all;
use std::convert::TryInto as _;
use std::fs;
use tokio::sync::mpsc::channel;
use tokio::task::JoinHandle;

struct NodeSetup {
    handle: JoinHandle<Digest>,
    tx_mempool: tokio::sync::mpsc::Sender<Vec<Digest>>,
    store: Store,
}

fn spawn_nodes(
    keys: Vec<(PublicKey, SecretKey)>,
    committee: Committee,
    store_path: &str,
) -> Vec<NodeSetup> {
    keys.into_iter()
        .enumerate()
        .map(|(i, (name, secret))| {
            let committee = committee.clone();
            let parameters = Parameters {
                timeout_delay: 100,
                ..Parameters::default()
            };
            let store_path = format!("{}_{}", store_path, i);
            let _ = fs::remove_dir_all(&store_path);
            let store = Store::new(&store_path).unwrap();
            let signature_service = SignatureService::new(secret);
            let (tx_consensus_to_mempool, mut rx_consensus_to_mempool) = channel(10);
            let (tx_mempool_to_consensus, rx_mempool_to_consensus) = channel(1);
            let (tx_commit, mut rx_commit) = channel(1);

            // Keep a reference to the store and sender for later use
            let store_clone = store.clone();
            let tx_mempool_clone = tx_mempool_to_consensus.clone();

            // Sink the mempool channel.
            tokio::spawn(async move {
                loop {
                    rx_consensus_to_mempool.recv().await;
                }
            });

            // Spawn the consensus engine.
            let handle = tokio::spawn(async move {
                Consensus::spawn(
                    name,
                    committee,
                    parameters,
                    signature_service,
                    store,
                    rx_mempool_to_consensus,
                    tx_consensus_to_mempool,
                    tx_commit,
                );

                rx_commit.recv().await.unwrap()
            });

            NodeSetup {
                handle,
                tx_mempool: tx_mempool_clone,
                store: store_clone,
            }
        })
        .collect()
}

#[tokio::test]
async fn end_to_end() {
    let committee = committee_with_base_port(15_000);

    // Run all nodes.
    let store_path = ".db_test_end_to_end";
    let nodes = spawn_nodes(keys(), committee, store_path);

    // Give nodes time to start up and bind to ports
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Create a test transaction
    let tx: Vec<u8> = vec![1, 2, 3, 4, 5];

    // Compute the digest
    let digest = Digest(Sha512::digest(&tx).as_slice()[..32].try_into().unwrap());

    // Store the transaction in ALL nodes' stores (required for vote verification)
    let mut stores: Vec<_> = nodes.iter().map(|n| n.store.clone()).collect();
    for store in &mut stores {
        store.write(digest.to_vec(), tx.clone()).await;
    }

    // Send the digest to ALL nodes' mempool channels to trigger consensus.
    // Each node needs to vote independently, and we need 3 out of 4 votes for quorum.
    for node in &nodes {
        node.tx_mempool
            .send(vec![digest.clone()])
            .await
            .expect("Failed to send digest to mempool");
    }

    // Wait for all nodes to receive commits
    let handles: Vec<_> = nodes.into_iter().map(|n| n.handle).collect();
    let blocks = try_join_all(handles).await.unwrap();

    // All nodes should have committed the same digest
    assert!(blocks.windows(2).all(|w| w[0] == w[1]));
}
