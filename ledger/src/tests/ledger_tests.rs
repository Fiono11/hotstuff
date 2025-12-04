use crate::{Account, Ledger, LedgerError};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use std::convert::TryInto;
use store::Store;
use types::{generate_production_keypair, Digest, Transaction};

#[tokio::test]
async fn test_ledger_initialization() {
    let store = Store::new("test_db_init").expect("Failed to create store");
    let mut ledger = Ledger::new(store);

    let (account, _) = generate_production_keypair();
    let balance = ledger.get_balance(&account).await.unwrap();
    assert_eq!(balance, 0);
}

#[tokio::test]
async fn test_account_initialization() {
    let store = Store::new("test_db_init_account").expect("Failed to create store");
    let mut ledger = Ledger::new(store);

    let (account, _) = generate_production_keypair();
    ledger
        .initialize_account(&account, 1000)
        .await
        .expect("Failed to initialize account");

    let balance = ledger.get_balance(&account).await.unwrap();
    assert_eq!(balance, 1000);
}

#[tokio::test]
async fn test_transaction_execution() {
    let mut store = Store::new("test_db_tx").expect("Failed to create store");
    let mut ledger = Ledger::new(store.clone());

    let (sender, sender_sk) = generate_production_keypair();
    let (receiver, _) = generate_production_keypair();

    // Initialize sender with balance
    ledger
        .initialize_account(&sender, 1000)
        .await
        .expect("Failed to initialize sender");

    // Create and store a transaction
    let tx = Transaction::new_signed(sender, 100, receiver, 0, 0, &sender_sk);
    let tx_bytes = tx.to_bytes();
    // Calculate digest the same way mempool does
    let digest = Digest(
        Sha512::digest(&tx_bytes).as_slice()[..32]
            .try_into()
            .unwrap(),
    );
    store.write(digest.to_vec(), tx_bytes).await;

    // Execute the transaction
    ledger
        .execute_transaction(&digest, &store)
        .await
        .expect("Failed to execute transaction");

    // Check balances
    let sender_balance = ledger.get_balance(&sender).await.unwrap();
    let receiver_balance = ledger.get_balance(&receiver).await.unwrap();

    assert_eq!(sender_balance, 900);
    assert_eq!(receiver_balance, 100);
}

#[tokio::test]
async fn test_insufficient_balance() {
    let mut store = Store::new("test_db_insufficient").expect("Failed to create store");
    let mut ledger = Ledger::new(store.clone());

    let (sender, sender_sk) = generate_production_keypair();
    let (receiver, _) = generate_production_keypair();

    // Initialize sender with insufficient balance
    ledger
        .initialize_account(&sender, 50)
        .await
        .expect("Failed to initialize sender");

    // Create transaction with amount > balance
    let tx = Transaction::new_signed(sender, 100, receiver, 0, 0, &sender_sk);
    let tx_bytes = tx.to_bytes();
    let digest = Digest(
        Sha512::digest(&tx_bytes).as_slice()[..32]
            .try_into()
            .unwrap(),
    );
    store.write(digest.to_vec(), tx_bytes).await;

    // Execution should fail
    let result = ledger.execute_transaction(&digest, &store).await;
    assert!(matches!(
        result,
        Err(LedgerError::InsufficientBalance(50, 100))
    ));
}

#[tokio::test]
async fn test_invalid_nonce() {
    let mut store = Store::new("test_db_nonce").expect("Failed to create store");
    let mut ledger = Ledger::new(store.clone());

    let (sender, sender_sk) = generate_production_keypair();
    let (receiver, _) = generate_production_keypair();

    // Initialize sender
    ledger
        .initialize_account(&sender, 1000)
        .await
        .expect("Failed to initialize sender");

    // Create transaction with wrong nonce
    let tx = Transaction::new_signed(sender, 100, receiver, 5, 0, &sender_sk);
    let tx_bytes = tx.to_bytes();
    let digest = Digest(
        Sha512::digest(&tx_bytes).as_slice()[..32]
            .try_into()
            .unwrap(),
    );
    store.write(digest.to_vec(), tx_bytes).await;

    // Execution should fail
    let result = ledger.execute_transaction(&digest, &store).await;
    assert!(matches!(result, Err(LedgerError::InvalidNonce(0, 5))));
}
