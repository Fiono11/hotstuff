# Ledger Module

A simple ledger implementation for tracking account balances in the HotStuff consensus system.

## Features

- Track account balances using `PublicKey` as account identifiers
- Initialize accounts with starting balances
- Apply transactions to update balances
- Process committed blocks from consensus
- Query account balances

## Usage

### Basic Example

```rust
use crypto::generate_keypair;
use ledger::Ledger;
use mempool::TransactionData;
use rand::rngs::OsRng;
use store::Store;

// Create a store
let store = Store::new(".db_ledger")?;

// Create a ledger
let mut ledger = Ledger::new(store);

// Generate some accounts
let mut rng = OsRng;
let (alice, _) = generate_keypair(&mut rng);
let (bob, _) = generate_keypair(&mut rng);

// Initialize accounts with balances
ledger.initialize_account(alice, 1000);
ledger.initialize_account(bob, 500);

// Create a transaction
let tx = TransactionData {
    sender: alice,
    amount: 200,
    destination: bob,
    nonce: 0,
    epoch: 0,
};

// Apply the transaction
ledger.apply_transaction(&tx)?;

// Query balances
println!("Alice balance: {}", ledger.get_balance(&alice)); // 800
println!("Bob balance: {}", ledger.get_balance(&bob));     // 700
```

### Processing Committed Blocks

```rust
use ledger::Ledger;
use consensus::Block;

// When a block is committed by consensus
async fn process_committed_block(ledger: &mut Ledger, block: &Block) {
    // Apply all transactions in the block
    if let Err(e) = ledger.apply_block(block).await {
        eprintln!("Failed to apply block: {}", e);
    }
    
    // Print ledger summary
    ledger.print_summary();
}
```

### Running the Example

To see a complete example with fake accounts and transactions:

```bash
cargo run --example create_fake_ledger --package ledger
```

## API

### `Ledger::new(store: Store) -> Ledger`
Create a new ledger with the given store.

### `ledger.initialize_account(public_key: PublicKey, balance: u64)`
Initialize an account with a starting balance. If the account already exists, this overwrites its balance.

### `ledger.initialize_accounts(accounts: Vec<(PublicKey, u64)>)`
Initialize multiple accounts at once.

### `ledger.get_balance(public_key: &PublicKey) -> u64`
Get the balance of an account. Returns 0 if the account doesn't exist.

### `ledger.get_all_accounts() -> Vec<Account>`
Get all accounts and their balances.

### `ledger.apply_transaction(tx: &TransactionData) -> LedgerResult<()>`
Apply a single transaction. Returns an error if the sender has insufficient balance.

### `ledger.apply_batch(batch_digest: &Digest) -> LedgerResult<()>`
Apply all transactions from a batch stored in the store.

### `ledger.apply_block(block: &Block) -> LedgerResult<()>`
Apply all transactions from a committed block.

### `ledger.print_summary()`
Print a summary of all accounts and their balances.

## Error Handling

The ledger returns `LedgerResult<T>` which can contain:

- `LedgerError::InsufficientBalance` - When a transaction tries to spend more than available
- `LedgerError::BatchNotFound` - When a batch digest is not found in the store
- `LedgerError::StoreError` - When there's an error reading from the store
- `LedgerError::DeserializationError` - When transaction or batch data cannot be deserialized
- `LedgerError::InvalidMessage` - When the message format is invalid

