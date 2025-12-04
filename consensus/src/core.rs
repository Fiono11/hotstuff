use crate::aggregator::Aggregator;
use crate::config::Committee;
use crate::consensus::ConsensusMessage;
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::Vote;
use bytes::Bytes;
use types::{Digest, PublicKey, SignatureService};
use log::{debug, error, info, warn};
use mempool::ConsensusMempoolMessage;
use network::SimpleSender;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

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
    aggregator: Aggregator,
    network: SimpleSender,
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
                aggregator: Aggregator::new(committee),
                network: SimpleSender::new(),
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

        // Check if we have all transactions referenced in the vote.
        // Check each digest in the payload.
        let mut missing = Vec::new();
        for digest in &vote.payload {
            if self.store.read(digest.to_vec()).await?.is_none() {
                missing.push(digest.clone());
            }
        }

        // If we're missing transactions, request them from the voter via mempool.
        if !missing.is_empty() {
            debug!(
                "Missing {} transactions from vote by {}, requesting from mempool",
                missing.len(),
                vote.author
            );
            let message = ConsensusMempoolMessage::Synchronize(missing, vote.author);
            if let Err(e) = self.tx_mempool.send(message).await {
                warn!("Failed to send sync message to mempool: {}", e);
            }
        }

        // Process the vote: if it has multiple digests, use batch vote handler;
        // if it has one digest, use single vote handler.
        if vote.payload.len() > 1 {
            // Batch vote: extract each digest and vote for it individually
            // (quorum is computed per transaction).
            let results = self.aggregator.add_batch_vote(vote)?;
            for (digest, qc_opt) in results {
                if let Some(qc) = qc_opt {
                    debug!("Assembled {:?}", qc);

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

                // Notify the application layer of the committed transaction digest.
                let digest = qc.hash.clone();
                info!("Committed tx {}", digest);
                if let Err(e) = self.tx_commit.send(digest.clone()).await {
                    warn!("Failed to send digest through the commit channel: {}", e);
                }

                // Clean up the aggregator after the transaction is committed.
                self.aggregator.cleanup(&digest);
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
