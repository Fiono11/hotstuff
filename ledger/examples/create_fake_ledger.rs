use crypto::generate_keypair;
use ledger::Ledger;
use mempool::TransactionData;
use rand::rngs::OsRng;
use std::fs;
use store::Store;

/// Example: Create a fake ledger with accounts and balances
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    // Create a store for the ledger
    let store_path = ".db_fake_ledger";
    let _ = fs::remove_dir_all(store_path);
    let store = Store::new(store_path)?;

    // Create a new ledger
    let mut ledger = Ledger::new(store);

    // Generate some fake accounts
    let mut rng = OsRng;
    let accounts: Vec<_> = (0..10)
        .map(|i| {
            let (public_key, _) = generate_keypair(&mut rng);
            // Give each account a random balance between 1000 and 10000
            let balance = 1000 + (i * 1000);
            (public_key, balance)
        })
        .collect();

    // Initialize accounts with balances
    println!("Initializing accounts...");
    for (public_key, balance) in &accounts {
        ledger.initialize_account(*public_key, *balance);
        println!("  Account {}: {} coins", public_key, balance);
    }

    // Print initial ledger summary
    println!("\n=== Initial Ledger State ===");
    ledger.print_summary();

    // Create some fake transactions
    println!("\n=== Creating Sample Transactions ===");
    let mut transactions = Vec::new();
    for i in 0..5 {
        let sender = accounts[i % accounts.len()].0;
        let destination = accounts[(i + 1) % accounts.len()].0;
        let amount = 100 + (i * 50) as u64;

        let tx = TransactionData {
            sender,
            amount,
            destination,
            nonce: i as u32,
            epoch: 0,
        };

        println!(
            "  Transaction {}: {} -> {} (amount: {})",
            i, sender, destination, amount
        );
        transactions.push(tx);
    }

    // Apply transactions
    println!("\n=== Applying Transactions ===");
    for tx in &transactions {
        match ledger.apply_transaction(tx) {
            Ok(()) => {
                println!(
                    "  ✓ Applied: {} -> {} (amount: {})",
                    tx.sender, tx.destination, tx.amount
                );
            }
            Err(e) => {
                println!("  ✗ Failed: {}", e);
            }
        }
    }

    // Print final ledger summary
    println!("\n=== Final Ledger State ===");
    ledger.print_summary();

    // Query specific account balances
    println!("\n=== Querying Account Balances ===");
    for (public_key, _) in &accounts[..3] {
        let balance = ledger.get_balance(public_key);
        println!("  Account {}: {} coins", public_key, balance);
    }

    Ok(())
}
