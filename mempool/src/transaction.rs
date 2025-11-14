use crypto::PublicKey;
use serde::{Deserialize, Serialize};

pub type Transaction = Vec<u8>;

/// Transaction data structure containing sender, amount, destination, nonce, and epoch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransactionData {
    pub sender: PublicKey,
    pub amount: u64,
    pub destination: PublicKey,
    pub nonce: u32,
    pub epoch: u64,
}

impl TransactionData {
    /// Serialize the transaction data into a Transaction (Vec<u8>).
    pub fn to_transaction(&self) -> Result<Transaction, bincode::Error> {
        bincode::serialize(self)
    }

    /// Deserialize a Transaction (Vec<u8>) into TransactionData.
    pub fn from_transaction(data: &Transaction) -> Result<Self, bincode::Error> {
        bincode::deserialize(data)
    }
}
