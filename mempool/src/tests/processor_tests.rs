use super::*;
use crate::batch_maker::Batch;
use crate::common::batch;
use crate::processor::Processor;
use ed25519_dalek::Sha512;
use std::convert::TryInto;
use std::fs;
use store::Store;
use tokio::sync::mpsc::channel;
use types::{Digest, Transaction};

#[tokio::test]
async fn hash_and_store() {
    let (tx_batch, rx_batch) = channel(1);
    let (tx_digest, mut rx_digest) = channel(1);

    // Create a new test store.
    let path = ".db_test_hash_and_store";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Spawn a new `Processor` instance.
    Processor::spawn(store.clone(), rx_batch, tx_digest);

    // Send a batch to the `Processor`.
    let test_batch: Batch = batch();
    tx_batch.send(test_batch.clone()).await.unwrap();

    // Ensure the `Processor` outputs the batch's digests.
    let received_digests = rx_digest.recv().await.unwrap();
    assert_eq!(received_digests.len(), test_batch.len());

    // Ensure the `Processor` correctly stored each transaction.
    for (i, tx) in test_batch.iter().enumerate() {
        let tx_bytes = tx.to_bytes();
        let expected_digest = Digest(Sha512::digest(&tx_bytes).as_slice()[..32].try_into().unwrap());
        assert_eq!(received_digests[i], expected_digest);

        let stored_tx_bytes = store.read(expected_digest.to_vec()).await.unwrap();
        assert!(stored_tx_bytes.is_some(), "Transaction {} is not in the store", i);
        // Deserialize the stored bytes to compare
        let config = bincode::config::standard();
        let stored_tx: Transaction = bincode::serde::decode_from_slice(&stored_tx_bytes.unwrap(), config).unwrap().0;
        assert_eq!(stored_tx, *tx);
    }
}
