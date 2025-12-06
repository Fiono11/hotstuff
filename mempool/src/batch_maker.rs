use ed25519_dalek::{Digest as _, Sha512};
use log::info;
use std::collections::HashMap;
use std::convert::TryInto as _;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration, Instant};
use types::{Digest, Transaction};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

pub type Batch = Vec<Transaction>;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,
    /// Output channel to deliver sealed batches' digests directly to consensus.
    tx_digest: Sender<Vec<Digest>>,
    /// In-memory transaction cache.
    tx_cache: Arc<Mutex<HashMap<Digest, Vec<u8>>>>,
    /// Holds the current batch.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
}

impl BatchMaker {
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        tx_digest: Sender<Vec<Digest>>,
        tx_cache: Arc<Mutex<HashMap<Digest, Vec<u8>>>>,
    ) {
        tokio::spawn(async move {
            Self {
                batch_size,
                max_batch_delay,
                rx_transaction,
                tx_digest,
                tx_cache,
                current_batch: Batch::with_capacity(batch_size * 2),
                current_batch_size: 0,
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    self.current_batch_size += transaction.to_bytes().len();
                    self.current_batch.push(transaction);
                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal the current batch: store each transaction and send its digests as a batch.
    async fn seal(&mut self) {
        let size = self.current_batch_size;
        self.current_batch_size = 0;

        // Drain the current batch so we can process and store each transaction individually.
        let batch: Vec<_> = self.current_batch.drain(..).collect();

        // Collect all digests for this batch.
        let mut digests = Vec::with_capacity(batch.len());

        for tx in batch.iter() {
            // Hash each transaction.
            let tx_bytes = tx.to_bytes();
            let digest = Digest(
                Sha512::digest(&tx_bytes).as_slice()[..32]
                    .try_into()
                    .unwrap(),
            );

            // Store the raw transaction bytes in memory cache instead of writing to store.
            {
                let mut cache = self.tx_cache.lock().await;
                cache.insert(digest.clone(), tx_bytes);
            }

            // NOTE: This log entry is used to compute performance.
            info!("Received tx {}", digest);

            // Collect the digest for batch sending.
            digests.push(digest);
        }

        // Send all digests as a batch to consensus.
        if !digests.is_empty() {
            self.tx_digest
                .send(digests)
                .await
                .expect("Failed to deliver transaction digests to consensus");
        }

        // NOTE: This log entry is used to compute performance.
        info!(
            "Sealed batch of {} B containing {} transactions",
            size,
            batch.len()
        );
    }
}
