use crate::batch_maker::Batch;
use crate::config::Committee;
use crate::mempool::MempoolMessage;
use bytes::Bytes;
use crypto::{Digest, PublicKey};
use log::{error, warn};
use network::SimpleSender;
use store::Store;
use tokio::sync::mpsc::Receiver;

#[cfg(test)]
#[path = "tests/helper_tests.rs"]
pub mod helper_tests;

/// A task dedicated to help other authorities by replying to their batch requests.
pub struct Helper {
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Input channel to receive batch requests.
    rx_request: Receiver<(Vec<Digest>, PublicKey)>,
    /// A network sender to send the batches to the other mempools.
    network: SimpleSender,
}

impl Helper {
    pub fn spawn(
        committee: Committee,
        store: Store,
        rx_request: Receiver<(Vec<Digest>, PublicKey)>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                store,
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

            // Collect all available transactions from the store.
            let mut batch: Batch = Vec::new();
            for digest in digests {
                match self.store.read(digest.to_vec()).await {
                    Ok(Some(data)) => {
                        // The data stored is the raw transaction bytes.
                        batch.push(data);
                    }
                    Ok(None) => {
                        // Transaction not found in store, skip it.
                    }
                    Err(e) => {
                        error!("Failed to read transaction {} from store: {}", digest, e);
                    }
                }
            }

            // Send the batch as a MempoolMessage::Batch if we have any transactions.
            if !batch.is_empty() {
                let message = MempoolMessage::Batch(batch);
                match bincode::serialize(&message) {
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
