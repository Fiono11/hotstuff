use crypto::{Digest, PublicKey};
use log::{debug, info, warn};
use mempool::{MempoolMessage, TransactionData};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use store::Store;

pub mod error;

use error::{LedgerError, LedgerResult};

/// Represents the state of an account with its balance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Account {
    pub public_key: PublicKey,
    pub balance: u64,
}

/// The ledger that tracks account balances.
pub struct Ledger {
    /// In-memory account balances (PublicKey -> balance)
    accounts: HashMap<PublicKey, u64>,
    /// Reference to the store for retrieving transaction batches
    store: Store,
}

impl Ledger {
    /// Create a new ledger with the given store.
    pub fn new(store: Store) -> Self {
        Self {
            accounts: HashMap::new(),
            store,
        }
    }

    /// Initialize an account with a starting balance.
    /// If the account already exists, this will overwrite its balance.
    pub fn initialize_account(&mut self, public_key: PublicKey, balance: u64) {
        info!(
            "Initializing account {} with balance {}",
            public_key, balance
        );
        self.accounts.insert(public_key, balance);
    }

    /// Initialize multiple accounts at once.
    pub fn initialize_accounts(&mut self, accounts: Vec<(PublicKey, u64)>) {
        for (public_key, balance) in accounts {
            self.initialize_account(public_key, balance);
        }
    }

    /// Get the balance of an account.
    /// Returns 0 if the account doesn't exist.
    pub fn get_balance(&self, public_key: &PublicKey) -> u64 {
        self.accounts.get(public_key).copied().unwrap_or(0)
    }

    /// Get all accounts and their balances.
    pub fn get_all_accounts(&self) -> Vec<Account> {
        self.accounts
            .iter()
            .map(|(public_key, balance)| Account {
                public_key: *public_key,
                balance: *balance,
            })
            .collect()
    }

    /// Apply a single transaction to update account balances.
    /// Returns an error if the sender has insufficient balance.
    pub fn apply_transaction(&mut self, tx: &TransactionData) -> LedgerResult<()> {
        let sender_balance = self.get_balance(&tx.sender);

        if sender_balance < tx.amount {
            return Err(LedgerError::InsufficientBalance {
                account: tx.sender,
                balance: sender_balance,
                requested: tx.amount,
            });
        }

        // Deduct from sender
        let new_sender_balance = sender_balance - tx.amount;
        self.accounts.insert(tx.sender, new_sender_balance);

        // Add to destination
        let dest_balance = self.get_balance(&tx.destination);
        self.accounts
            .insert(tx.destination, dest_balance + tx.amount);

        debug!(
            "Applied transaction: {} -> {} (amount: {})",
            tx.sender, tx.destination, tx.amount
        );
        debug!(
            "  Sender balance: {} -> {}",
            sender_balance, new_sender_balance
        );
        debug!(
            "  Destination balance: {} -> {}",
            dest_balance,
            dest_balance + tx.amount
        );

        Ok(())
    }

    /// Apply transactions from a batch stored in the store.
    /// This retrieves the batch using the digest and applies all transactions.
    pub async fn apply_batch(&mut self, batch_digest: &Digest) -> LedgerResult<()> {
        // Retrieve the batch from the store
        let batch_data = self
            .store
            .read(batch_digest.to_vec())
            .await
            .map_err(|e| LedgerError::StoreError(format!("Failed to read batch: {}", e)))?;

        let batch_data = batch_data.ok_or_else(|| {
            LedgerError::BatchNotFound(format!("Batch not found: {:?}", batch_digest))
        })?;

        // Deserialize the batch
        let message: MempoolMessage = bincode::deserialize(&batch_data).map_err(|e| {
            LedgerError::DeserializationError(format!("Failed to deserialize batch: {}", e))
        })?;

        let transactions = match message {
            MempoolMessage::Batch(batch) => batch,
            _ => {
                return Err(LedgerError::InvalidMessage(
                    "Expected MempoolMessage::Batch".to_string(),
                ));
            }
        };

        // Apply each transaction
        for tx_bytes in transactions {
            let tx_data = TransactionData::from_transaction(&tx_bytes).map_err(|e| {
                LedgerError::DeserializationError(format!(
                    "Failed to deserialize transaction: {}",
                    e
                ))
            })?;

            if let Err(e) = self.apply_transaction(&tx_data) {
                warn!("Failed to apply transaction: {}", e);
                // Continue processing other transactions even if one fails
            }
        }

        Ok(())
    }

    /// Apply transactions from a committed block.
    /// This processes all batch digests in the block's payload.
    pub async fn apply_block(&mut self, block: &consensus::Block) -> LedgerResult<()> {
        info!(
            "Applying block {} with {} batches",
            block.round,
            block.payload.len()
        );

        for batch_digest in &block.payload {
            if let Err(e) = self.apply_batch(batch_digest).await {
                warn!("Failed to apply batch {:?}: {}", batch_digest, e);
                // Continue processing other batches even if one fails
            }
        }

        Ok(())
    }

    /// Print a summary of all accounts and their balances.
    pub fn print_summary(&self) {
        info!("=== Ledger Summary ===");
        info!("Total accounts: {}", self.accounts.len());
        let total_balance: u64 = self.accounts.values().sum();
        info!("Total balance: {}", total_balance);
        info!("Accounts:");
        for (public_key, balance) in &self.accounts {
            info!("  {}: {}", public_key, balance);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crypto::generate_keypair;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::fs;

    fn create_test_store() -> Store {
        let path = ".db_test_ledger";
        let _ = fs::remove_dir_all(path);
        Store::new(path).expect("Failed to create test store")
    }

    #[tokio::test]
    async fn test_initialize_account() {
        let store = create_test_store();
        let mut ledger = Ledger::new(store);

        let mut rng = StdRng::from_seed([42; 32]);
        let (public_key, _) = generate_keypair(&mut rng);

        ledger.initialize_account(public_key, 1000);
        assert_eq!(ledger.get_balance(&public_key), 1000);
    }

    #[tokio::test]
    async fn test_apply_transaction() {
        let store = create_test_store();
        let mut ledger = Ledger::new(store);

        let mut rng = StdRng::from_seed([42; 32]);
        let (sender, _) = generate_keypair(&mut rng);
        let (destination, _) = generate_keypair(&mut rng);

        ledger.initialize_account(sender, 1000);
        ledger.initialize_account(destination, 500);

        let tx = TransactionData {
            sender,
            amount: 200,
            destination,
            nonce: 0,
            epoch: 0,
        };

        assert!(ledger.apply_transaction(&tx).is_ok());
        assert_eq!(ledger.get_balance(&sender), 800);
        assert_eq!(ledger.get_balance(&destination), 700);
    }

    #[tokio::test]
    async fn test_insufficient_balance() {
        let store = create_test_store();
        let mut ledger = Ledger::new(store);

        let mut rng = StdRng::from_seed([42; 32]);
        let (sender, _) = generate_keypair(&mut rng);
        let (destination, _) = generate_keypair(&mut rng);

        ledger.initialize_account(sender, 100);

        let tx = TransactionData {
            sender,
            amount: 200,
            destination,
            nonce: 0,
            epoch: 0,
        };

        assert!(ledger.apply_transaction(&tx).is_err());
        assert_eq!(ledger.get_balance(&sender), 100);
        assert_eq!(ledger.get_balance(&destination), 0);
    }
}
