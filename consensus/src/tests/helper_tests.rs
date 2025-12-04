use super::*;
use crate::common::{block, committee_with_base_port, keys, listener};
use crypto::Hash as _;
use std::fs;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn sync_reply() {
    let (tx_request, rx_request) = channel(1);
    let (requestor, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(13_000);

    // Create a new test store.
    let path = ".db_test_sync_reply";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Add a batch to the store.
    let digest = block().digest();
    let config = bincode::config::standard();
    let serialized = bincode::serde::encode_to_vec(&block(), config).unwrap();
    store.write(digest.to_vec(), serialized.clone()).await;

    // Spawn an `Helper` instance.
    Helper::spawn(committee.clone(), store, rx_request);

    // Spawn a listener to receive the sync reply.
    let address = committee.address(&requestor).unwrap();
    let message = ConsensusMessage::Propose(block());
    let expected = Bytes::from(bincode::serde::encode_to_vec(&message, config).unwrap());
    let handle = listener(address, Some(expected));

    // Send a sync request.
    tx_request.send((digest, requestor)).await.unwrap();

    // Ensure the requestor received the batch (ie. it did not panic).
    assert!(handle.await.is_ok());
}
