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
use types::{generate_production_keypair, Transaction};

#[derive(Parser)]
#[clap(author, version, about, long_about = "Benchmark client for Rai nodes.")]
struct Cli {
    /// The nodes timeout value.
    #[clap(short, long, value_parser, value_name = "INT")]
    timeout: u64,
    /// The size of each transaction in bytes.
    #[clap(short, long, value_parser, value_name = "INT")]
    size: usize,
    /// The total number of transactions to send.
    #[clap(short, long, value_parser, value_name = "INT")]
    total_txs: u64,
    /// Network addresses that must be reachable before starting the benchmark.
    #[clap(short, long, value_parser, value_name = "[Addr]", action = ArgAction::Append)]
    nodes: Vec<SocketAddr>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    info!("Transactions size: {} B", cli.size);
    info!("Total transactions: {}", cli.total_txs);
    let client = Client {
        size: cli.size,
        total_txs: cli.total_txs,
        timeout: cli.timeout,
        nodes: cli.nodes,
    };

    // Wait for all nodes to be online and synchronized.
    client.wait().await;

    // Start the benchmark.
    client.send().await.context("Failed to submit transactions")
}

struct Client {
    size: usize,
    total_txs: u64,
    timeout: u64,
    nodes: Vec<SocketAddr>,
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

        // Generate keypairs for sender and receiver
        let (sender_pk, sender_sk) = generate_production_keypair();
        let (receiver_pk, _) = generate_production_keypair();

        // Submit all transactions.
        let mut total_sent = 0u64;
        let mut nonce = 0u32;

        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions (total: {})", self.total_txs);

        while total_sent < self.total_txs {
            nonce += 1;
            
            // Create a transaction with the desired size
            // We'll adjust the amount to try to match the size, but the actual size
            // will depend on the serialized transaction structure
            let amount = if self.size > 100 {
                // Use a larger amount to increase transaction size
                (self.size as u128) * 1000
            } else {
                1000
            };
            
            let transaction = Transaction::new_signed(
                sender_pk,
                amount,
                receiver_pk,
                nonce,
                0, // epoch
                &sender_sk,
            );

            let bytes = transaction.to_bytes();
            
            // If the transaction is smaller than desired, we can't easily pad it
            // since it's a structured type. The size will be determined by the
            // actual transaction structure.
            if bytes.len() < self.size {
                warn!(
                    "Transaction size {} is smaller than requested size {}. Actual size: {}",
                    total_sent, self.size, bytes.len()
                );
            }

            // Log transaction info
            info!("Sending transaction {} (nonce: {}, size: {} B)", total_sent, nonce, bytes.len());

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
