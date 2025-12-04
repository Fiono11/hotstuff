use super::*;
use crate::batch_maker::Batch;
use crate::common::batch;
use crate::processor::Processor;
use ed25519_dalek::Sha512;
use std::fs;
use store::Store;
use tokio::sync::mpsc::channel;
use types::Digest;

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
        let expected_digest = Digest(Sha512::digest(tx).as_slice()[..32].try_into().unwrap());
        assert_eq!(received_digests[i], expected_digest);

        let stored_tx = store.read(expected_digest.to_vec()).await.unwrap();
        assert!(stored_tx.is_some(), "Transaction {} is not in the store", i);
        assert_eq!(stored_tx.unwrap(), *tx);
    }
}
