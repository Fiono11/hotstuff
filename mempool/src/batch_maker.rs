use crypto::Digest;
use ed25519_dalek::{Digest as _, Sha512};
use log::info;
use std::convert::TryInto as _;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

pub type Transaction = Vec<u8>;
pub type Batch = Vec<Transaction>;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,
    /// The persistent storage.
    store: Store,
    /// Output channel to deliver sealed batches' digests directly to consensus.
    tx_digest: Sender<Digest>,
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
        store: Store,
        tx_digest: Sender<Digest>,
    ) {
        tokio::spawn(async move {
            Self {
                batch_size,
                max_batch_delay,
                rx_transaction,
                store,
                tx_digest,
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
                    self.current_batch_size += transaction.len();
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

    /// Seal the current batch: store each transaction and send its digest.
    async fn seal(&mut self) {
        let size = self.current_batch_size;
        self.current_batch_size = 0;

        // Drain the current batch so we can process and store each transaction individually.
        let batch: Vec<_> = self.current_batch.drain(..).collect();

        for tx in batch.iter() {
            // Hash each transaction.
            let digest = Digest(Sha512::digest(&tx).as_slice()[..32].try_into().unwrap());

            // Store the raw transaction bytes under its digest.
            self.store.write(digest.to_vec(), tx.clone()).await;

            // NOTE: This log entry is used to compute performance.
            info!("Received tx {}", digest);

            // Send the transaction's digest directly to consensus.
            self.tx_digest
                .send(digest)
                .await
                .expect("Failed to deliver transaction digest to consensus");
        }

        // NOTE: This log entry is used to compute performance.
        info!(
            "Sealed batch of {} B containing {} transactions",
            size,
            batch.len()
        );
    }
}
