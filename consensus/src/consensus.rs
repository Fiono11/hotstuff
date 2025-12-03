use crate::config::{Committee, Parameters};
use crate::core::Core;
use crate::error::ConsensusError;
use crate::helper::Helper;
use crate::messages::{Block, Vote};
use async_trait::async_trait;
use bytes::Bytes;
use crypto::{Digest, PublicKey, SignatureService};
use futures::SinkExt as _;
use log::info;
use mempool::ConsensusMempoolMessage;
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use serde::{Deserialize, Serialize};
use std::error::Error;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

/// The default channel capacity for each channel of the consensus.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The consensus round number.
pub type Round = u64;

#[derive(Serialize, Deserialize, Debug)]
pub enum ConsensusMessage {
    Propose(Block),
    Vote(Vote),
    SyncRequest(Digest, PublicKey),
}

pub struct Consensus;

impl Consensus {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        signature_service: SignatureService,
        store: Store,
        rx_mempool: Receiver<Vec<Digest>>,
        _tx_mempool: Sender<ConsensusMempoolMessage>,
        tx_commit: Sender<Digest>,
    ) {
        // NOTE: This log entry is used to compute performance.
        parameters.log();

        let (tx_consensus, rx_consensus) = channel(CHANNEL_CAPACITY);
        let (tx_helper, rx_helper) = channel(CHANNEL_CAPACITY);

        // Spawn the network receiver.
        let mut address = committee
            .address(&name)
            .expect("Our public key is not in the committee");
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */
            ConsensusReceiverHandler {
                tx_consensus,
                tx_helper,
            },
        );
        info!(
            "Node {} listening to consensus messages on {}",
            name, address
        );

        // Spawn the consensus core.
        Core::spawn(
            name,
            committee.clone(),
            signature_service.clone(),
            /* rx_message */ rx_consensus,
            rx_mempool,
            tx_commit,
        );

        // Spawn the helper module.
        Helper::spawn(committee, store, /* rx_requests */ rx_helper);
    }
}

/// Defines how the network receiver handles incoming primary messages.
#[derive(Clone)]
struct ConsensusReceiverHandler {
    tx_consensus: Sender<ConsensusMessage>,
    tx_helper: Sender<(Digest, PublicKey)>,
}

#[async_trait]
impl MessageHandler for ConsensusReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Deserialize and parse the message.
        match bincode::deserialize(&serialized).map_err(ConsensusError::SerializationError)? {
            ConsensusMessage::SyncRequest(missing, origin) => self
                .tx_helper
                .send((missing, origin))
                .await
                .expect("Failed to send consensus message"),
            message @ ConsensusMessage::Propose(..) => {
                // Reply with an ACK.
                let _ = writer.send(Bytes::from("Ack")).await;

                // Pass the message to the consensus core.
                self.tx_consensus
                    .send(message)
                    .await
                    .expect("Failed to consensus message")
            }
            message => self
                .tx_consensus
                .send(message)
                .await
                .expect("Failed to consensus message"),
        }
        Ok(())
    }
}
