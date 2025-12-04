use crate::batch_maker::Batch;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use log::info;
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use types::Digest;

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

/// Indicates a serialized `MempoolMessage::Batch` message.
/// This type is kept for compatibility with other modules (e.g., quorum_waiter).
#[allow(dead_code)]
pub type SerializedBatchMessage = Vec<u8>;

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor;

impl Processor {
    pub fn spawn(
        // The persistent storage.
        mut store: Store,
        // Input channel to receive batches.
        mut rx_batch: Receiver<Batch>,
        // Output channel to send out batches' digests.
        tx_digest: Sender<Vec<Digest>>,
    ) {
        tokio::spawn(async move {
            while let Some(batch) = rx_batch.recv().await {
                // Process each transaction in the batch.
                let mut digests = Vec::with_capacity(batch.len());
                for tx in batch.iter() {
                    // Hash each transaction.
                    let tx_bytes = tx.to_bytes();
                    let digest = Digest(Sha512::digest(&tx_bytes).as_slice()[..32].try_into().unwrap());

                    // Store the raw transaction bytes under its digest.
                    store.write(digest.to_vec(), tx_bytes).await;

                    // NOTE: This log entry is used to compute performance.
                    info!("Received tx {}", digest);

                    // Collect the digest for batch sending.
                    digests.push(digest);
                }

                // Send all transaction digests as a batch to consensus.
                if !digests.is_empty() {
                    tx_digest
                        .send(digests)
                        .await
                        .expect("Failed to send transaction digests to consensus");
                }
            }
        });
    }
}
