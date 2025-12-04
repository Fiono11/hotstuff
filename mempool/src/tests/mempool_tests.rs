use super::*;
use crate::common::{committee_with_base_port, keys, listener, transaction};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use network::SimpleSender;
use std::convert::TryInto as _;
use std::fs;
use tokio::sync::mpsc::channel;
use types::Digest;

#[tokio::test]
async fn handle_clients_transactions() {
    let (name, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(11_000);
    let parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

    // Create a new test store.
    let path = ".db_test_handle_clients_transactions";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Spawn a `Mempool` instance.
    let (_tx_consensus_to_mempool, rx_consensus_to_mempool) = channel(1);
    let (tx_mempool_to_consensus, mut rx_mempool_to_consensus) = channel(1);
    Mempool::spawn(
        name,
        committee.clone(),
        parameters,
        store,
        rx_consensus_to_mempool,
        tx_mempool_to_consensus,
    );

    // Spawn enough mempools' listeners to acknowledge our batches.
    for (_, address) in committee.broadcast_addresses(&name) {
        let _ = listener(address, /* expected */ None);
    }

    // Send enough transactions to create a batch.
    let mut network = SimpleSender::new();
    let address = committee.transactions_address(&name).unwrap();
    network.send(address, Bytes::from(transaction())).await;
    network.send(address, Bytes::from(transaction())).await;

    // Ensure the consensus got the batch digests.
    let received_digests = rx_mempool_to_consensus.recv().await.unwrap();
    assert_eq!(received_digests.len(), 2);

    // Verify each digest matches the expected transaction digest.
    let tx1 = transaction();
    let tx2 = transaction();
    let expected_digest1 = Digest(Sha512::digest(&tx1).as_slice()[..32].try_into().unwrap());
    let expected_digest2 = Digest(Sha512::digest(&tx2).as_slice()[..32].try_into().unwrap());
    assert_eq!(received_digests[0], expected_digest1);
    assert_eq!(received_digests[1], expected_digest2);
}
