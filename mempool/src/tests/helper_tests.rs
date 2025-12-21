use super::*;
use crate::common::{batch, committee_with_base_port, keys, listener, serialized_batch};
use crate::helper::Helper;
use bytes::Bytes;
use ed25519_dalek::{Digest as _, Sha512};
use std::collections::HashMap;
use std::convert::TryInto as _;
use std::fs;
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::channel;
use tokio::sync::Mutex;
use types::Digest;

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
        let tx_bytes = tx.to_bytes();
        let digest = Digest(
            Sha512::digest(&tx_bytes).as_slice()[..32]
                .try_into()
                .unwrap(),
        );
        store.write(digest.to_vec(), tx_bytes).await;
        digests.push(digest);
    }

    // Create a transaction cache.
    let tx_cache = Arc::new(Mutex::new(HashMap::new()));

    // Spawn an `Helper` instance.
    Helper::spawn(committee.clone(), store, tx_cache, rx_request);

    // Spawn a listener to receive the batch reply.
    let address = committee.mempool_address(&requestor).unwrap();
    let expected = Bytes::from(serialized_batch());
    let handle = listener(address, Some(expected));

    // Send a batch request with transaction digests.
    tx_request.send((digests, requestor)).await.unwrap();

    // Ensure the requestor received the batch (ie. it did not panic).
    assert!(handle.await.is_ok());
}
