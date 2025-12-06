use crate::batch_maker::Batch;
use crate::config::Committee;
use crate::mempool::MempoolMessage;
use bytes::Bytes;
use log::{error, warn};
use network::SimpleSender;
use std::collections::HashMap;
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
use types::{Digest, PublicKey, Transaction};

#[cfg(test)]
#[path = "tests/helper_tests.rs"]
pub mod helper_tests;

/// A task dedicated to help other authorities by replying to their batch requests.
pub struct Helper {
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// In-memory transaction cache.
    tx_cache: Arc<Mutex<HashMap<Digest, Vec<u8>>>>,
    /// Input channel to receive batch requests.
    rx_request: Receiver<(Vec<Digest>, PublicKey)>,
    /// A network sender to send the batches to the other mempools.
    network: SimpleSender,
}

impl Helper {
    pub fn spawn(
        committee: Committee,
        store: Store,
        tx_cache: Arc<Mutex<HashMap<Digest, Vec<u8>>>>,
        rx_request: Receiver<(Vec<Digest>, PublicKey)>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                store,
                tx_cache,
                rx_request,
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some((digests, origin)) = self.rx_request.recv().await {
            // TODO [issue #7]: Do some accounting to prevent bad nodes from monopolizing our resources.

            // Get the requestor's address.
            let address = match self.committee.mempool_address(&origin) {
                Some(x) => x,
                None => {
                    warn!("Received batch request from unknown authority: {}", origin);
                    continue;
                }
            };

            // Collect all available transactions from cache or store.
            let mut batch: Batch = Vec::new();
            for digest in digests {
                // First check the in-memory cache
                let tx_bytes = {
                    let cache = self.tx_cache.lock().await;
                    cache.get(&digest).cloned()
                };

                let tx_bytes = match tx_bytes {
                    Some(bytes) => Some(bytes),
                    None => {
                        // Fall back to store if not in cache
                        match self.store.read(digest.to_vec()).await {
                            Ok(Some(data)) => Some(data),
                            Ok(None) => None,
                            Err(e) => {
                                error!("Failed to read transaction {} from store: {}", digest, e);
                                None
                            }
                        }
                    }
                };

                if let Some(data) = tx_bytes {
                    // Deserialize the stored transaction bytes into a Transaction struct.
                    let config = bincode::config::standard();
                    match bincode::serde::decode_from_slice(&data, config) {
                        Ok((transaction, _)) => {
                            let transaction: Transaction = transaction;
                            batch.push(transaction);
                        }
                        Err(e) => {
                            error!("Failed to deserialize transaction {}: {}", digest, e);
                        }
                    }
                }
            }

            // Send the batch as a MempoolMessage::Batch if we have any transactions.
            if !batch.is_empty() {
                let message = MempoolMessage::Batch(batch);
                let config = bincode::config::standard();
                match bincode::serde::encode_to_vec(&message, config) {
                    Ok(serialized) => {
                        self.network.send(address, Bytes::from(serialized)).await;
                    }
                    Err(e) => {
                        error!("Failed to serialize batch message: {}", e);
                    }
                }
            }
        }
    }
}
