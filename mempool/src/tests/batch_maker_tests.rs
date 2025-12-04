use super::*;
use crate::batch_maker::BatchMaker;
use crate::common::transaction;
use crypto::Digest;
use ed25519_dalek::Sha512;
use std::fs;
use store::Store;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn make_batch() {
    let (tx_transaction, rx_transaction) = channel(1);
    let (tx_digest, mut rx_digest) = channel(1);

    // Create a new test store.
    let path = ".db_test_make_batch";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Spawn a `BatchMaker` instance.
    BatchMaker::spawn(
        /* batch_size */ 200,
        /* max_batch_delay */ 1_000_000, // Ensure the timer is not triggered.
        rx_transaction,
        store,
        tx_digest,
    );

    // Send enough transactions to seal a batch.
    let tx1 = transaction();
    let tx2 = transaction();
    tx_transaction.send(tx1.clone()).await.unwrap();
    tx_transaction.send(tx2.clone()).await.unwrap();

    // Ensure the batch digests are as expected.
    let received_digests = rx_digest.recv().await.unwrap();
    assert_eq!(received_digests.len(), 2);

    let expected_digest1 = Digest(Sha512::digest(&tx1).as_slice()[..32].try_into().unwrap());
    let expected_digest2 = Digest(Sha512::digest(&tx2).as_slice()[..32].try_into().unwrap());
    assert_eq!(received_digests[0], expected_digest1);
    assert_eq!(received_digests[1], expected_digest2);
}

#[tokio::test]
async fn batch_timeout() {
    let (tx_transaction, rx_transaction) = channel(1);
    let (tx_digest, mut rx_digest) = channel(1);

    // Create a new test store.
    let path = ".db_test_batch_timeout";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Spawn a `BatchMaker` instance.
    BatchMaker::spawn(
        /* batch_size */ 200,
        /* max_batch_delay */ 50, // Ensure the timer is triggered.
        rx_transaction,
        store,
        tx_digest,
    );

    // Do not send enough transactions to seal a batch.
    let tx1 = transaction();
    tx_transaction.send(tx1.clone()).await.unwrap();

    // Ensure the batch digest is as expected.
    let received_digests = rx_digest.recv().await.unwrap();
    assert_eq!(received_digests.len(), 1);

    let expected_digest1 = Digest(Sha512::digest(&tx1).as_slice()[..32].try_into().unwrap());
    assert_eq!(received_digests[0], expected_digest1);
}
