use anyhow::{Context, Result};
use bytes;
use clap::{ArgAction, Parser};
use env_logger::Env;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use log::{info, warn};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use types::{generate_production_keypair, PublicKey, SecretKey, Transaction};

#[derive(Parser)]
#[clap(author, version, about, long_about = "Benchmark client for Rai nodes.")]
struct Cli {
    /// The nodes timeout value.
    #[clap(short, long, value_parser, value_name = "INT")]
    timeout: u64,
    /// The total number of transactions to send.
    #[clap(short, long, value_parser, value_name = "INT")]
    total_txs: u64,
    /// Network addresses that must be reachable before starting the benchmark.
    #[clap(short, long, value_parser, value_name = "[Addr]", action = ArgAction::Append)]
    nodes: Vec<SocketAddr>,
    /// The account (public key) to use for signing transactions (base64 encoded).
    #[clap(short, long, value_parser, value_name = "STRING")]
    account: Option<String>,
    /// The secret key to use for signing transactions (base64 encoded).
    #[clap(short, long, value_parser, value_name = "STRING")]
    secret: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    info!("Total transactions: {}", cli.total_txs);

    // Parse account and secret if provided
    let (sender_pk, sender_sk) = if let Some(account_str) = &cli.account {
        let pk = PublicKey::decode_base64(account_str)
            .context("Failed to decode account public key (must be base64 encoded)")?;
        let sk = if let Some(secret_str) = &cli.secret {
            SecretKey::decode_base64(secret_str)
                .context("Failed to decode secret key (must be base64 encoded)")?
        } else {
            return Err(anyhow::anyhow!("Secret key is required when account is specified. Use --secret to provide the base64-encoded secret key."));
        };
        (pk, sk)
    } else {
        // Generate a new keypair if account is not specified
        generate_production_keypair()
    };

    let client = Client {
        total_txs: cli.total_txs,
        timeout: cli.timeout,
        nodes: cli.nodes,
        sender_pk,
        sender_sk,
    };

    // Wait for all nodes to be online and synchronized.
    client.wait().await;

    // Start the benchmark.
    client.send().await.context("Failed to submit transactions")
}

struct Client {
    total_txs: u64,
    timeout: u64,
    nodes: Vec<SocketAddr>,
    sender_pk: PublicKey,
    sender_sk: SecretKey,
}

impl Client {
    pub async fn send(&self) -> Result<()> {
        // Collect all target addresses (use nodes directly).
        let mut all_targets = self.nodes.clone();
        all_targets.sort();
        all_targets.dedup();

        // Connect to all nodes.
        info!("Connecting to {} nodes...", all_targets.len());
        let mut transports = Vec::new();
        for address in &all_targets {
            let stream = TcpStream::connect(*address)
                .await
                .context(format!("failed to connect to {}", address))?;
            transports.push(Framed::new(stream, LengthDelimitedCodec::new()));
        }
        info!("Connected to all {} nodes", transports.len());

        // Use the provided sender keypair, generate receiver keypair
        let sender_pk = self.sender_pk;
        let sender_sk = &self.sender_sk;
        let (receiver_pk, _) = generate_production_keypair();

        // Submit all transactions.
        let mut total_sent = 0u64;
        let mut nonce = 0u32;

        // Create a sample transaction to determine size
        let sample_transaction = Transaction::new_signed(
            sender_pk,
            1000, // amount
            receiver_pk,
            1,
            0, // epoch
            &sender_sk,
        );
        let sample_bytes = sample_transaction.to_bytes();
        let tx_size = sample_bytes.len();

        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions (total: {})", self.total_txs);
        info!("Transaction size: {} B", tx_size);

        while total_sent < self.total_txs {
            nonce += 1;

            let transaction = Transaction::new_signed(
                sender_pk,
                1000, // amount
                receiver_pk,
                nonce,
                0, // epoch
                sender_sk,
            );

            let bytes = transaction.to_bytes();

            // Log transaction info
            info!(
                "Sending transaction {} (nonce: {}, size: {} B)",
                total_sent,
                nonce,
                bytes.len()
            );

            // Send transaction to all nodes in parallel.
            let send_futures: Vec<_> = transports
                .iter_mut()
                .map(|transport| transport.send(bytes::Bytes::from(bytes.clone())))
                .collect();

            let results = join_all(send_futures).await;
            if let Some(Err(e)) = results.iter().find(|r| r.is_err()) {
                warn!("Failed to send transaction to at least one node: {}", e);
                // Continue sending even if one node fails
            }

            total_sent += 1;
        }

        info!("Sent all {} transactions, exiting", self.total_txs);
        Ok(())
    }

    pub async fn wait(&self) {
        // First wait for all nodes to be online.
        info!("Waiting for all nodes to be online...");
        join_all(self.nodes.iter().cloned().map(|address| {
            tokio::spawn(async move {
                while TcpStream::connect(address).await.is_err() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
        }))
        .await;

        // Then wait for the nodes to be synchronized.
        info!("Waiting for all nodes to be synchronized...");
        sleep(Duration::from_millis(2 * self.timeout)).await;
    }
}
