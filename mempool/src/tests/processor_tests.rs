use super::*;
use crate::batch_maker::Batch;
use crate::common::batch;
use crate::processor::Processor;
use ed25519_dalek::Sha512;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fs;
use std::sync::Arc;
use tokio::sync::mpsc::channel;
use tokio::sync::Mutex;
use types::{Digest, Transaction};

#[tokio::test]
async fn hash_and_store() {
    let (tx_batch, rx_batch) = channel(1);
    let (tx_digest, mut rx_digest) = channel(1);

    // Create a transaction cache.
    let tx_cache = Arc::new(Mutex::new(HashMap::new()));

    // Spawn a new `Processor` instance.
    Processor::spawn(rx_batch, tx_digest, tx_cache.clone());

    // Send a batch to the `Processor`.
    let test_batch: Batch = batch();
    tx_batch.send(test_batch.clone()).await.unwrap();

    // Ensure the `Processor` outputs the batch's digests.
    let received_digests = rx_digest.recv().await.unwrap();
    assert_eq!(received_digests.len(), test_batch.len());

    // Ensure the `Processor` correctly stored each transaction in the cache.
    for (i, tx) in test_batch.iter().enumerate() {
        let tx_bytes = tx.to_bytes();
        let expected_digest = Digest(
            Sha512::digest(&tx_bytes).as_slice()[..32]
                .try_into()
                .unwrap(),
        );
        assert_eq!(received_digests[i], expected_digest);

        // Check that the transaction is in the cache
        let cache = tx_cache.lock().await;
        let stored_tx_bytes = cache.get(&expected_digest);
        assert!(
            stored_tx_bytes.is_some(),
            "Transaction {} is not in the cache",
            i
        );
        // Deserialize the stored bytes to compare
        let config = bincode::config::standard();
        let stored_tx: Transaction =
            bincode::serde::decode_from_slice(stored_tx_bytes.unwrap(), config)
                .unwrap()
                .0;
        assert_eq!(stored_tx, *tx);
    }
}
