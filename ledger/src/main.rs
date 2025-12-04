use ledger::Ledger;
use store::Store;
use types::{generate_production_keypair, PublicKey, SecretKey};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a store for the ledger
    let store_path = "ledger_init_db";
    let store = Store::new(store_path)?;

    // Create a new ledger
    let mut ledger = Ledger::new(store);

    // Calculate the balance for each account (u128::MAX / 4)
    let balance_per_account = u128::MAX / 4;

    // Generate 4 keypairs and initialize accounts
    let mut accounts: Vec<(PublicKey, SecretKey)> = Vec::new();

    println!("Initializing ledger with 4 accounts...");
    for i in 0..4 {
        let (public_key, secret_key) = generate_production_keypair();
        //accounts.push((public_key, secret_key));

        // Initialize the account with the balance
        ledger
            .initialize_account(&public_key, balance_per_account)
            .await?;

        // Verify the balance was set correctly
        let balance = ledger.get_balance(&public_key).await?;

        // Print full base64-encoded keys
        println!("Account {}:", i + 1);
        println!("  Public Key (full): {}", public_key.encode_base64());
        println!("  Secret Key (full): {}", secret_key.encode_base64());
        println!("  Display: {} (balance: {})", public_key, balance);
        println!();

        assert_eq!(balance, balance_per_account);
    }

    println!("\nLedger initialized successfully!");
    println!(
        "Total balance across all accounts: {}",
        balance_per_account * 4
    );

    Ok(())
}
