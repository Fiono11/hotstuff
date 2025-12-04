use crate::config::Export as _;
use crate::config::{Committee, ConfigError, Parameters, Secret};
use consensus::Consensus;
use types::{Digest, SignatureService};
use log::{info, warn, error};
use mempool::Mempool;
use store::Store;
use ledger::Ledger;
use tokio::sync::mpsc::{channel, Receiver};

/// The default channel capacity for this module.
pub const CHANNEL_CAPACITY: usize = 1_000;

pub struct Node {
    pub commit: Receiver<Digest>,
    ledger: Ledger,
    store: Store,
}

impl Node {
    pub async fn new(
        committee_file: &str,
        key_file: &str,
        store_path: &str,
        parameters: Option<String>,
    ) -> Result<Self, ConfigError> {
        let (tx_commit, rx_commit) = channel(CHANNEL_CAPACITY);
        let (tx_consensus_to_mempool, rx_consensus_to_mempool) = channel(CHANNEL_CAPACITY);
        let (tx_mempool_to_consensus, rx_mempool_to_consensus) = channel(CHANNEL_CAPACITY);

        // Read the committee and secret key from file.
        let committee = Committee::read(committee_file)?;
        let secret = Secret::read(key_file)?;
        let name = secret.name;
        let secret_key = secret.secret;

        // Load default parameters if none are specified.
        let parameters = match parameters {
            Some(filename) => Parameters::read(&filename)?,
            None => Parameters::default(),
        };

        // Make the data store.
        let store = Store::new(store_path).expect("Failed to create store");

        // Create the ledger for managing account balances.
        // We need a separate store instance for the ledger since it will be used
        // in analyze_block while the main store is used by consensus.
        let ledger_store = store.clone();
        let ledger = Ledger::new(ledger_store.clone());

        // Run the signature service.
        let signature_service = SignatureService::new(secret_key);

        // Make a new mempool.
        Mempool::spawn(
            name,
            committee.mempool,
            parameters.mempool,
            store.clone(),
            rx_consensus_to_mempool,
            tx_mempool_to_consensus,
        );

        // Run the consensus core.
        Consensus::spawn(
            name,
            committee.consensus,
            parameters.consensus,
            signature_service,
            store,
            rx_mempool_to_consensus,
            tx_consensus_to_mempool,
            tx_commit,
        );

        info!("Node {} successfully booted", name);
        Ok(Self {
            commit: rx_commit,
            ledger,
            store: ledger_store,
        })
    }

    pub fn print_key_file(filename: &str) -> Result<(), ConfigError> {
        Secret::new().write(filename)
    }

    pub async fn analyze_block(&mut self) {
        while let Some(digest) = self.commit.recv().await {
            // Execute the committed transaction in the ledger.
            match self.ledger.execute_transaction(&digest, &self.store).await {
                Ok(()) => {
                    info!("Successfully executed transaction {}", digest);
                }
                Err(e) => {
                    error!("Failed to execute transaction {}: {}", digest, e);
                    // Continue processing other transactions even if one fails
                }
            }
        }
    }
}
