use super::*;
use crate::common::{batch, committee_with_base_port, keys, listener, serialized_batch};
use crypto::Digest;
use ed25519_dalek::{Digest as _, Sha512};
use std::convert::TryInto as _;
use std::fs;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn batch_reply() {
    let (tx_request, rx_request) = channel(1);
    let (requestor, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(8_000);

    // Create a new test store.
    let path = ".db_test_batch_reply";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Create a batch and store each transaction in the store using its digest.
    let test_batch = batch();
    let mut digests = Vec::new();
    for tx in &test_batch {
        let digest = Digest(Sha512::digest(tx).as_slice()[..32].try_into().unwrap());
        store.write(digest.to_vec(), tx.clone()).await;
        digests.push(digest);
    }

    // Spawn an `Helper` instance.
    Helper::spawn(committee.clone(), store, rx_request);

    // Spawn a listener to receive the batch reply.
    let address = committee.mempool_address(&requestor).unwrap();
    let expected = Bytes::from(serialized_batch());
    let handle = listener(address, Some(expected));

    // Send a batch request with transaction digests.
    tx_request.send((digests, requestor)).await.unwrap();

    // Ensure the requestor received the batch (ie. it did not panic).
    assert!(handle.await.is_ok());
}
