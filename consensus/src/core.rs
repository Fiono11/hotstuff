use crate::aggregator::Aggregator;
use crate::config::Committee;
use crate::consensus::ConsensusMessage;
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::Vote;
use bytes::Bytes;
use ledger::Ledger;
use log::{debug, error, info, warn};
use mempool::ConsensusMempoolMessage;
use network::SimpleSender;
use std::collections::HashMap;
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use types::{Digest, PublicKey, SignatureService, Transaction};

pub struct Core {
    name: PublicKey,
    committee: Committee,
    signature_service: SignatureService,
    rx_message: Receiver<ConsensusMessage>,
    /// Receive batches of transaction digests directly from the mempool.
    rx_mempool: Receiver<Vec<Digest>>,
    /// Send committed transaction digests to the application layer.
    tx_commit: Sender<Digest>,
    /// Send synchronization requests to the mempool.
    tx_mempool: Sender<ConsensusMempoolMessage>,
    /// The persistent storage to check for missing transactions.
    store: Store,
    /// The ledger for executing transactions after voting.
    ledger: Ledger,
    aggregator: Aggregator,
    network: SimpleSender,
    /// In-memory transaction cache.
    tx_cache: Arc<Mutex<HashMap<Digest, Vec<u8>>>>,
    /// Pending transactions to be committed to store (digest -> bytes).
    pending_commits: HashMap<Digest, Vec<u8>>,
    /// Pending transactions to be executed on ledger (digest -> transaction).
    pending_ledger_executions: HashMap<Digest, Transaction>,
    /// Count of confirmed transactions since last commit.
    confirmed_count: usize,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        rx_message: Receiver<ConsensusMessage>,
        rx_mempool: Receiver<Vec<Digest>>,
        tx_commit: Sender<Digest>,
        tx_mempool: Sender<ConsensusMempoolMessage>,
        store: Store,
        ledger: Ledger,
        tx_cache: Arc<Mutex<HashMap<Digest, Vec<u8>>>>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee: committee.clone(),
                signature_service,
                rx_message,
                rx_mempool,
                tx_commit,
                tx_mempool,
                store,
                ledger,
                aggregator: Aggregator::new(committee),
                network: SimpleSender::new(),
                tx_cache,
                pending_commits: HashMap::new(),
                pending_ledger_executions: HashMap::new(),
                confirmed_count: 0,
            }
            .run()
            .await
        });
    }

    /// Handle a new vote coming from the network.
    async fn handle_vote(&mut self, vote: Vote) -> ConsensusResult<()> {
        debug!("Processing {:?}", vote);

        // Ensure the vote is well formed.
        vote.verify(&self.committee)?;

        // Save the vote author before processing (vote gets consumed by aggregator).
        let vote_author = vote.author;

        // Process the vote first to determine which transactions reach quorum.
        // Only request transactions that are confirmed (reach quorum) and we don't have.
        let mut confirmed_missing = Vec::new();

        // Process the vote: if it has multiple digests, use batch vote handler;
        // if it has one digest, use single vote handler.
        if vote.payload.len() > 1 {
            // Batch vote: extract each digest and vote for it individually
            // (quorum is computed per transaction).
            let results = self.aggregator.add_batch_vote(vote)?;
            for (digest, qc_opt) in results {
                if let Some(qc) = qc_opt {
                    debug!("Assembled {:?}", qc);

                    // Check if we have this confirmed transaction (in cache or store).
                    let has_tx = {
                        let cache = self.tx_cache.lock().await;
                        cache.contains_key(&digest)
                    } || self.store.read(digest.to_vec()).await?.is_some();
                    if !has_tx {
                        confirmed_missing.push(digest.clone());
                    }

                    // Execute the transaction after voting (reaching quorum)
                    self.execute_transaction_after_vote(&digest).await;

                    // Notify the application layer of the committed transaction digest.
                    info!("Committed tx {}", digest);
                    if let Err(e) = self.tx_commit.send(digest.clone()).await {
                        warn!("Failed to send digest through the commit channel: {}", e);
                    }

                    // Clean up the aggregator after the transaction is committed.
                    self.aggregator.cleanup(&digest);
                }
            }
        } else {
            // Single transaction vote (payload contains one digest).
            // Add the new vote to our aggregator and see if we have a quorum.
            if let Some(qc) = self.aggregator.add_vote(vote)? {
                debug!("Assembled {:?}", qc);

                let digest = qc.hash.clone();
                // Check if we have this confirmed transaction (in cache or store).
                let has_tx = {
                    let cache = self.tx_cache.lock().await;
                    cache.contains_key(&digest)
                } || self.store.read(digest.to_vec()).await?.is_some();
                if !has_tx {
                    confirmed_missing.push(digest.clone());
                }

                // Execute the transaction after voting (reaching quorum)
                self.execute_transaction_after_vote(&digest).await;

                // Notify the application layer of the committed transaction digest.
                info!("Committed tx {}", digest);
                if let Err(e) = self.tx_commit.send(digest.clone()).await {
                    warn!("Failed to send digest through the commit channel: {}", e);
                }

                // Clean up the aggregator after the transaction is committed.
                self.aggregator.cleanup(&digest);
            }
        }

        // Only request transactions that are confirmed (reach quorum) and we don't have.
        if !confirmed_missing.is_empty() {
            debug!(
                "Missing {} confirmed transactions from vote by {}, requesting from mempool",
                confirmed_missing.len(),
                vote_author
            );
            let message = ConsensusMempoolMessage::Synchronize(confirmed_missing, vote_author);
            if let Err(e) = self.tx_mempool.send(message).await {
                warn!("Failed to send sync message to mempool: {}", e);
            }
        }
        Ok(())
    }

    /// Handle a batch of digests coming from the mempool: create a single vote containing all digests.
    async fn handle_digest_batch(&mut self, digests: Vec<Digest>) -> ConsensusResult<()> {
        debug!("Received batch of {} digests", digests.len());

        if digests.is_empty() {
            return Ok(());
        }

        // Create a single vote containing all digests in the payload.
        let sig_service = self.signature_service.clone();
        let vote = Vote::new(digests.clone(), self.name, sig_service).await;

        // Process the batch vote locally: add our vote for each digest in the batch.
        // The aggregator will handle adding the author and signature for each digest
        // without creating individual Vote structures.
        let results = self.aggregator.add_batch_vote(vote.clone())?;
        for (digest, qc_opt) in results {
            if let Some(qc) = qc_opt {
                debug!("Assembled {:?}", qc);

                // Execute the transaction after voting (reaching quorum)
                self.execute_transaction_after_vote(&digest).await;

                // Notify the application layer of the committed transaction digest.
                info!("Committed tx {}", digest);
                if let Err(e) = self.tx_commit.send(digest.clone()).await {
                    warn!("Failed to send digest through the commit channel: {}", e);
                }

                // Clean up the aggregator after the transaction is committed.
                self.aggregator.cleanup(&digest);
            }
        }

        // Broadcast the single batch vote to all other authorities.
        debug!(
            "Broadcasting batch vote {:?} with {} digests",
            vote,
            vote.payload.len()
        );
        let addresses = self
            .committee
            .broadcast_addresses(&self.name)
            .into_iter()
            .map(|(_, x)| x)
            .collect();
        let config = bincode::config::standard();
        let message = bincode::serde::encode_to_vec(&ConsensusMessage::Vote(vote), config)
            .expect("Failed to serialize vote");
        self.network
            .broadcast(addresses, Bytes::from(message))
            .await;

        Ok(())
    }

    /// Queue a transaction for deferred execution after it reaches quorum (after voting).
    /// Transactions are only executed on the ledger when committed in batches of 10000.
    async fn execute_transaction_after_vote(&mut self, digest: &Digest) {
        // Get transaction bytes from cache or store
        let tx_bytes = match self.get_transaction_bytes(digest).await {
            Some(bytes) => bytes,
            None => {
                error!("Transaction {} not found in cache or store", digest);
                return;
            }
        };

        // Add to pending commits if not already there (for batch commit to store)
        if !self.pending_commits.contains_key(digest) {
            self.pending_commits
                .insert(digest.clone(), tx_bytes.clone());
        }

        // Deserialize transaction for deferred ledger execution
        let config = bincode::config::standard();
        let (tx, _): (Transaction, usize) =
            match bincode::serde::decode_from_slice(&tx_bytes, config) {
                Ok(result) => result,
                Err(e) => {
                    error!("Failed to deserialize transaction {}: {}", digest, e);
                    return;
                }
            };

        // Get transaction details for logging
        let sender = tx.sender;
        let amount = tx.amount;

        // Queue transaction for deferred ledger execution (don't execute immediately)
        // Transactions will only be executed on the ledger when committed in batches of 10000
        if !self.pending_ledger_executions.contains_key(digest) {
            self.pending_ledger_executions.insert(digest.clone(), tx);
        }

        // Increment confirmed count
        self.confirmed_count += 1;

        // Log that transaction is queued (not yet executed on ledger)
        info!(
            "Queued transaction {} for deferred ledger execution: sender {} amount {} (confirmed_count: {})",
            digest, sender, amount, self.confirmed_count
        );

        // Check if we should batch commit (and execute on ledger)
        self.maybe_batch_commit().await;
    }

    /// Get transaction bytes from cache or store.
    async fn get_transaction_bytes(&self, digest: &Digest) -> Option<Vec<u8>> {
        // First check the in-memory cache
        {
            let cache = self.tx_cache.lock().await;
            if let Some(tx_bytes) = cache.get(digest) {
                return Some(tx_bytes.clone());
            }
        }

        // Fall back to store if not in cache
        let mut store_clone = self.store.clone();
        store_clone.read(digest.to_vec()).await.ok().flatten()
    }

    /// Batch commit pending transactions to store every 10000 confirmed transactions.
    /// This also executes all pending transactions on the ledger at this point.
    async fn maybe_batch_commit(&mut self) {
        const COMMIT_BATCH_SIZE: usize = 10000;

        if self.confirmed_count >= COMMIT_BATCH_SIZE {
            info!(
                "Batch committing {} pending transactions to store and executing on ledger (confirmed_count: {})",
                self.pending_commits.len(),
                self.confirmed_count
            );

            // Write all pending transactions to store
            for (digest, tx_bytes) in self.pending_commits.drain() {
                self.store.write(digest.to_vec(), tx_bytes).await;
            }

            // Execute all pending transactions on the ledger (this is when balances actually change)
            for (digest, tx) in self.pending_ledger_executions.drain() {
                match self.ledger.execute_transaction_with_tx(&tx).await {
                    Ok(new_balance) => {
                        info!(
                            "Executed transaction {} on ledger: sender {} balance after subtracting {}: {}",
                            digest, tx.sender, tx.amount, new_balance
                        );
                    }
                    Err(e) => {
                        error!(
                            "Failed to execute transaction {} on ledger: {}",
                            digest, e
                        );
                    }
                }
            }

            // Reset confirmed count
            self.confirmed_count = 0;
        }
    }

    pub async fn run(&mut self) {
        // This is the main loop: it processes incoming votes and digests.
        loop {
            let result = tokio::select! {
                Some(message) = self.rx_message.recv() => match message {
                    ConsensusMessage::Vote(vote) => self.handle_vote(vote).await,
                    // Ignore all other consensus messages in this simplified core.
                    _ => Ok(()),
                },
                Some(digests) = self.rx_mempool.recv() => self.handle_digest_batch(digests).await,
            };
            match result {
                Ok(()) => (),
                Err(ConsensusError::StoreError(e)) => error!("{}", e),
                Err(ConsensusError::SerializationError(e)) => error!("Store corrupted. {}", e),
                Err(e) => warn!("{}", e),
            }
        }
    }
}
