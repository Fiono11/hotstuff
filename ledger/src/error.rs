use crypto::PublicKey;
use std::fmt;

#[derive(Debug)]
pub enum LedgerError {
    InsufficientBalance {
        account: PublicKey,
        balance: u64,
        requested: u64,
    },
    BatchNotFound(String),
    StoreError(String),
    DeserializationError(String),
    InvalidMessage(String),
}

impl fmt::Display for LedgerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LedgerError::InsufficientBalance {
                account,
                balance,
                requested,
            } => write!(
                f,
                "Insufficient balance for account {}: has {}, requested {}",
                account, balance, requested
            ),
            LedgerError::BatchNotFound(msg) => write!(f, "Batch not found: {}", msg),
            LedgerError::StoreError(msg) => write!(f, "Store error: {}", msg),
            LedgerError::DeserializationError(msg) => write!(f, "Deserialization error: {}", msg),
            LedgerError::InvalidMessage(msg) => write!(f, "Invalid message: {}", msg),
        }
    }
}

impl std::error::Error for LedgerError {}

pub type LedgerResult<T> = Result<T, LedgerError>;

