use super::*;
use crate::batch_maker::BatchMaker;
use crate::common::transaction;
use ed25519_dalek::Sha512;
use std::collections::HashMap;
use std::convert::TryInto as _;
use std::fs;
use std::sync::Arc;
use tokio::sync::mpsc::channel;
use tokio::sync::Mutex;
use types::Digest;

#[tokio::test]
async fn make_batch() {
    let (tx_transaction, rx_transaction) = channel(1);
    let (tx_digest, mut rx_digest) = channel(1);

    // Create a transaction cache.
    let tx_cache = Arc::new(Mutex::new(HashMap::new()));

    // Spawn a `BatchMaker` instance.
    BatchMaker::spawn(
        /* batch_size */ 200,
        /* max_batch_delay */ 1_000_000, // Ensure the timer is not triggered.
        rx_transaction,
        tx_digest,
        tx_cache,
    );

    // Send enough transactions to seal a batch.
    let tx1 = transaction();
    let tx2 = transaction();
    tx_transaction.send(tx1.clone()).await.unwrap();
    tx_transaction.send(tx2.clone()).await.unwrap();

    // Ensure the batch digests are as expected.
    let received_digests = rx_digest.recv().await.unwrap();
    assert_eq!(received_digests.len(), 2);

    let tx1_bytes = tx1.to_bytes();
    let tx2_bytes = tx2.to_bytes();
    let expected_digest1 = Digest(
        Sha512::digest(&tx1_bytes).as_slice()[..32]
            .try_into()
            .unwrap(),
    );
    let expected_digest2 = Digest(
        Sha512::digest(&tx2_bytes).as_slice()[..32]
            .try_into()
            .unwrap(),
    );
    assert_eq!(received_digests[0], expected_digest1);
    assert_eq!(received_digests[1], expected_digest2);
}

#[tokio::test]
async fn batch_timeout() {
    let (tx_transaction, rx_transaction) = channel(1);
    let (tx_digest, mut rx_digest) = channel(1);

    // Create a transaction cache.
    let tx_cache = Arc::new(Mutex::new(HashMap::new()));

    // Spawn a `BatchMaker` instance.
    BatchMaker::spawn(
        /* batch_size */ 200,
        /* max_batch_delay */ 50, // Ensure the timer is triggered.
        rx_transaction,
        tx_digest,
        tx_cache,
    );

    // Do not send enough transactions to seal a batch.
    let tx1 = transaction();
    tx_transaction.send(tx1.clone()).await.unwrap();

    // Ensure the batch digest is as expected.
    let received_digests = rx_digest.recv().await.unwrap();
    assert_eq!(received_digests.len(), 1);

    let tx1_bytes = tx1.to_bytes();
    let expected_digest1 = Digest(
        Sha512::digest(&tx1_bytes).as_slice()[..32]
            .try_into()
            .unwrap(),
    );
    assert_eq!(received_digests[0], expected_digest1);
}
