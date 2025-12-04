use anyhow::{Context, Result};
use bytes::BufMut as _;
use bytes::BytesMut;
use clap::{ArgAction, Parser};
use crypto::Digest;
use ed25519_dalek::{Digest as _, Sha512};
use env_logger::Env;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use log::{info, warn};
use std::convert::TryInto;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[derive(Parser)]
#[clap(
    author,
    version,
    about,
    long_about = "Benchmark client for HotStuff nodes."
)]
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
        // The transaction size must be at least 16 bytes to ensure all txs are different.
        if self.size < 16 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 9 bytes",
            ));
        }

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

        // Submit all transactions.
        let mut tx = BytesMut::with_capacity(self.size);
        let mut total_sent = 0u64;
        let mut r = 0u64;

        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions (total: {})", self.total_txs);

        while total_sent < self.total_txs {
            r += 1;
            tx.put_u8(1u8);
            tx.put_u64(r); // Ensures all clients send different txs.
            tx.resize(self.size, 0u8);
            let bytes = tx.split().freeze();

            // Calculate digest of the transaction
            let digest = Digest(Sha512::digest(&bytes).as_slice()[..32].try_into().unwrap());

            // Log all transactions with digest
            info!("Sending transaction {} digest: {:?}", total_sent, digest);

            // Send transaction to all nodes in parallel.
            let send_futures: Vec<_> = transports
                .iter_mut()
                .map(|transport| transport.send(bytes.clone()))
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
