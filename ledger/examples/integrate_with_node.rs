// Example: How to integrate the ledger with a HotStuff node
// This shows how to process committed blocks and update the ledger

use consensus::Block;
use crypto::{generate_keypair, PublicKey};
use ledger::Ledger;
use rand::rngs::OsRng;
use std::fs;
use store::Store;

/// Example showing how to integrate ledger with node's committed blocks
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    // In a real node, you would get the store from Node::new()
    // For this example, we create a new store
    let store_path = ".db_ledger_integration";
    let _ = fs::remove_dir_all(store_path);
    let store = Store::new(store_path)?;

    // Create a ledger
    let mut ledger = Ledger::new(store);

    // Initialize some accounts with balances (e.g., from genesis)
    let mut rng = OsRng;
    let accounts: Vec<(PublicKey, u64)> = (0..5)
        .map(|i| {
            let (public_key, _) = generate_keypair(&mut rng);
            let balance = 10000 + (i * 1000);
            (public_key, balance)
        })
        .collect();

    println!("Initializing genesis accounts...");
    ledger.initialize_accounts(accounts.clone());

    // In a real node, you would receive blocks from node.commit.recv()
    // Here's how you would process them:

    println!("\n=== Simulating Block Processing ===");
    println!("In your node.rs, you would do:");
    println!("  let mut ledger = Ledger::new(store);");
    println!("  ledger.initialize_accounts(genesis_accounts);");
    println!("  ");
    println!("  while let Some(block) = node.commit.recv().await {{");
    println!("      ledger.apply_block(&block).await?;");
    println!("      ledger.print_summary();");
    println!("  }}");

    // Example: Process a block (in real usage, this comes from consensus)
    // Note: This is just a demonstration - you'd need actual block data
    println!("\n=== Ledger State ===");
    ledger.print_summary();

    Ok(())
}
