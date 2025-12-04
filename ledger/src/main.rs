use ledger::Ledger;
use store::Store;
use types::{PublicKey, SecretKey};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a store for the ledger
    let store_path = "ledger_init_db";
    let store = Store::new(store_path)?;

    // Create a new ledger
    let mut ledger = Ledger::new(store);

    // Calculate the balance for each account (u128::MAX / 4)
    let balance_per_account = u128::MAX / 4;

    // Hardcoded accounts from terminal output
    let accounts: Vec<(PublicKey, SecretKey)> = vec![
        (
            PublicKey::decode_base64("LIUvl4zY4nG/TZIvlQLGaUryKflf+eqY8VDWPDRT8WM=")?,
            SecretKey::decode_base64("TlHt5/RGIQyEfu8hyrPUzIpHPPoyB4hJoxhBDyUPJn8shS+XjNjicb9Nki+VAsZpSvIp+V/56pjxUNY8NFPxYw==")?,
        ),
        (
            PublicKey::decode_base64("1HxzAJYTzgqFV8uewbFqmw0Z/vBa7P9fk/4VLWWz9NM=")?,
            SecretKey::decode_base64("2C82Yf/z8xSmvv7FfgG3sdet2RtLN04dJ+G+wWs3ePfUfHMAlhPOCoVXy57BsWqbDRn+8Frs/1+T/hUtZbP00w==")?,
        ),
        (
            PublicKey::decode_base64("tWBeZcYDe00SSTbFn5R+LyZhK3426IWiZym+LmE8K6c=")?,
            SecretKey::decode_base64("MAtBualo+k1BRdR+wyHzyxOrDXqInztmbLcrP6/9hV21YF5lxgN7TRJJNsWflH4vJmErfjbohaJnKb4uYTwrpw==")?,
        ),
        (
            PublicKey::decode_base64("Rx91kiXjP2BbfrqNspKwwJQZqxVEcZwQZlSysQ6LkI0=")?,
            SecretKey::decode_base64("OfGqJckMMgOaRw3t4CAHCNpwXq1lT/2Y41pudBbQQhxHH3WSJeM/YFt+uo2ykrDAlBmrFURxnBBmVLKxDouQjQ==")?,
        ),
    ];

    println!("Initializing ledger with 4 accounts...");
    for (i, (public_key, secret_key)) in accounts.iter().enumerate() {
        // Initialize the account with the balance
        ledger
            .initialize_account(public_key, balance_per_account)
            .await?;

        // Verify the balance was set correctly
        let balance = ledger.get_balance(public_key).await?;

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
