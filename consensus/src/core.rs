use crate::aggregator::Aggregator;
use crate::config::Committee;
use crate::consensus::ConsensusMessage;
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::Vote;
use bytes::Bytes;
use crypto::Hash as _;
use crypto::{Digest, PublicKey, Signature, SignatureService};
use log::{debug, error, info, warn};
use network::SimpleSender;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

pub struct Core {
    name: PublicKey,
    committee: Committee,
    signature_service: SignatureService,
    rx_message: Receiver<ConsensusMessage>,
    /// Receive transaction digests directly from the mempool.
    rx_mempool: Receiver<Digest>,
    /// Send committed transaction digests to the application layer.
    tx_commit: Sender<Digest>,
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
        rx_mempool: Receiver<Digest>,
        tx_commit: Sender<Digest>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee: committee.clone(),
                signature_service,
                rx_message,
                rx_mempool,
                tx_commit,
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

        // Add the new vote to our aggregator and see if we have a quorum.
        if let Some(qc) = self.aggregator.add_vote(vote)? {
            debug!("Assembled {:?}", qc);

            // Notify the application layer of the committed transaction digest.
            let digest = qc.hash.clone();
            info!("Committed tx {}", digest);
            if let Err(e) = self.tx_commit.send(digest).await {
                warn!("Failed to send digest through the commit channel: {}", e);
            }
        }
        Ok(())
    }

    /// Handle a new digest coming from the mempool: vote for it and broadcast our vote.
    async fn handle_digest(&mut self, digest: Digest) -> ConsensusResult<()> {
        debug!("Received digest {:?}", digest);

        // Create a vote directly for this transaction digest.
        let base_vote = Vote {
            hash: digest.clone(),
            author: self.name,
            signature: Signature::default(),
        };
        let mut sig_service = self.signature_service.clone();
        let signature = sig_service.request_signature(base_vote.digest()).await;
        let vote = Vote {
            signature,
            ..base_vote
        };

        // Process our own vote locally.
        self.handle_vote(vote.clone()).await?;

        // Broadcast the vote to all other authorities.
        debug!("Broadcasting {:?}", vote);
        let addresses = self
            .committee
            .broadcast_addresses(&self.name)
            .into_iter()
            .map(|(_, x)| x)
            .collect();
        let message =
            bincode::serialize(&ConsensusMessage::Vote(vote)).expect("Failed to serialize vote");
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
                Some(digest) = self.rx_mempool.recv() => self.handle_digest(digest).await,
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
