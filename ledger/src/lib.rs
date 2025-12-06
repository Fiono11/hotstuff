use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use store::Store;
use thiserror::Error;
use types::{Digest, PublicKey, Transaction};

#[cfg(test)]
#[path = "tests/ledger_tests.rs"]
mod ledger_tests;

pub type LedgerResult<T> = Result<T, LedgerError>;

#[derive(Error, Debug)]
pub enum LedgerError {
    #[error("Store error: {0}")]
    Store(#[from] store::StoreError),
    #[error("Deserialization error: {0}")]
    Deserialization(String),
    #[error("Transaction validation failed: {0}")]
    Validation(String),
    #[error("Insufficient balance: account has {0}, required {1}")]
    InsufficientBalance(u128, u128),
    #[error("Invalid nonce: expected {0}, got {1}")]
    InvalidNonce(u32, u32),
    #[error("Transaction not found: {0:?}")]
    TransactionNotFound(Digest),
}

/// Account state stored in the ledger.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Account {
    /// Account balance.
    pub balance: u128,
    /// Next expected nonce for transactions from this account.
    pub nonce: u32,
}

impl Account {
    pub fn new(initial_balance: u128) -> Self {
        Self {
            balance: initial_balance,
            nonce: 0,
        }
    }

    pub fn with_nonce(initial_balance: u128, nonce: u32) -> Self {
        Self {
            balance: initial_balance,
            nonce,
        }
    }
}

/// Ledger manages account balances and executes transactions.
pub struct Ledger {
    store: Store,
    /// In-memory cache of account states for fast access.
    /// This is a write-through cache - all updates are persisted to store.
    accounts: HashMap<PublicKey, Account>,
}

impl Ledger {
    /// Create a new ledger instance.
    /// The ledger uses the store for persistence and maintains an in-memory cache.
    pub fn new(store: Store) -> Self {
        Self {
            store,
            accounts: HashMap::new(),
        }
    }

    /// Get the balance for an account.
    /// Returns 0 if the account doesn't exist.
    pub async fn get_balance(&mut self, account: &PublicKey) -> LedgerResult<u128> {
        let account_state = self.get_account(account).await?;
        Ok(account_state.balance)
    }

    /// Get the account state (balance and nonce).
    pub async fn get_account(&mut self, account: &PublicKey) -> LedgerResult<Account> {
        // Check cache first
        if let Some(account_state) = self.accounts.get(account) {
            return Ok(account_state.clone());
        }

        // Load from store
        let key = Self::account_key(account);
        if let Some(bytes) = self.store.read(key).await? {
            let config = bincode::config::standard();
            let (account_state, _): (Account, usize) =
                bincode::serde::decode_from_slice(&bytes, config)
                    .map_err(|e| LedgerError::Deserialization(e.to_string()))?;
            // Update cache
            self.accounts.insert(*account, account_state.clone());
            Ok(account_state)
        } else {
            // Account doesn't exist, return default
            let default = Account::new(0);
            Ok(default)
        }
    }

    /// Execute a committed transaction.
    /// This validates the transaction and updates account balances.
    /// The store parameter is used to fetch the transaction data.
    /// Returns the new balance of the sender after execution.
    pub async fn execute_transaction(
        &mut self,
        digest: &Digest,
        store: &Store,
    ) -> LedgerResult<u128> {
        // Fetch the transaction from store
        let mut store_clone = store.clone();
        let tx_bytes = store_clone
            .read(digest.to_vec())
            .await?
            .ok_or_else(|| LedgerError::TransactionNotFound(digest.clone()))?;

        let config = bincode::config::standard();
        let (tx, _): (Transaction, usize) = bincode::serde::decode_from_slice(&tx_bytes, config)
            .map_err(|e| LedgerError::Deserialization(e.to_string()))?;

        // Validate the transaction
        self.validate_transaction(&tx)?;

        // Execute the transaction (update balances) and return the new balance
        self.apply_transaction(&tx).await
    }

    /// Execute a transaction directly from a Transaction object.
    /// This avoids reading from store, useful when transactions are kept in memory.
    /// Returns the new balance of the sender after execution.
    pub async fn execute_transaction_with_tx(
        &mut self,
        tx: &Transaction,
    ) -> LedgerResult<u128> {
        // Validate the transaction
        self.validate_transaction(tx)?;

        // Execute the transaction (update balances) and return the new balance
        self.apply_transaction(tx).await
    }

    /// Validate a transaction before execution.
    fn validate_transaction(&self, tx: &Transaction) -> LedgerResult<()> {
        // Verify signature
        let digest = tx.digest_for_signing();
        tx.signature
            .verify(&digest, &tx.sender)
            .map_err(|e| LedgerError::Validation(format!("Invalid signature: {}", e)))?;

        // Note: Balance and nonce checks are done in apply_transaction.
        Ok(())
    }

    /// Apply a validated transaction to update account balances.
    /// Note: Nonces are ignored - only balance is subtracted from sender.
    /// Returns the new balance after subtraction.
    async fn apply_transaction(&mut self, tx: &Transaction) -> LedgerResult<u128> {
        // Get sender account
        let mut sender_account = self.get_account(&tx.sender).await?;

        // Check balance
        if sender_account.balance < tx.amount {
            return Err(LedgerError::InsufficientBalance(
                sender_account.balance,
                tx.amount,
            ));
        }

        // Only subtract balance from sender (ignore nonce, don't add to receiver)
        sender_account.balance -= tx.amount;
        // Note: nonce is not incremented and receiver balance is not updated

        // Save the new balance
        let new_balance = sender_account.balance;

        // Persist sender account
        self.save_account(&tx.sender, &sender_account).await?;

        Ok(new_balance)
    }

    /// Save an account state to the store and update cache.
    async fn save_account(
        &mut self,
        account: &PublicKey,
        account_state: &Account,
    ) -> LedgerResult<()> {
        let key = Self::account_key(account);
        let config = bincode::config::standard();
        let bytes = bincode::serde::encode_to_vec(account_state, config)
            .map_err(|e| LedgerError::Deserialization(e.to_string()))?;
        let mut store_clone = self.store.clone();
        store_clone.write(key, bytes).await;
        // Update cache
        self.accounts.insert(*account, account_state.clone());
        Ok(())
    }

    /// Initialize an account with an initial balance.
    /// This is useful for genesis accounts or account creation.
    pub async fn initialize_account(
        &mut self,
        account: &PublicKey,
        initial_balance: u128,
    ) -> LedgerResult<()> {
        let account_state = Account::new(initial_balance);
        self.save_account(account, &account_state).await?;
        Ok(())
    }

    /// Subtract an amount from an account's balance.
    /// This is used when voting for a transaction to reserve the amount.
    pub async fn subtract_from_balance(
        &mut self,
        account: &PublicKey,
        amount: u128,
    ) -> LedgerResult<()> {
        let mut account_state = self.get_account(account).await?;

        if account_state.balance < amount {
            return Err(LedgerError::InsufficientBalance(
                account_state.balance,
                amount,
            ));
        }

        account_state.balance -= amount;
        self.save_account(account, &account_state).await?;
        Ok(())
    }

    /// Get the key used to store an account in the store.
    fn account_key(account: &PublicKey) -> Vec<u8> {
        let mut key = b"account:".to_vec();
        key.extend_from_slice(&account.0);
        key
    }

    /// Get all accounts (for debugging/testing purposes).
    pub fn get_all_accounts(&self) -> &HashMap<PublicKey, Account> {
        &self.accounts
    }

    /// Load all accounts from the store into the cache.
    /// This iterates through known account keys and loads them.
    pub async fn load_all_accounts(&mut self, known_accounts: &[PublicKey]) -> LedgerResult<()> {
        for account in known_accounts {
            // This will load from store if not in cache
            let _ = self.get_account(account).await?;
        }
        Ok(())
    }
}
