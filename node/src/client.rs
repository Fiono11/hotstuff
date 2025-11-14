use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use crypto::generate_keypair;
use env_logger::Env;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use log::{info, warn};
use mempool::TransactionData;
use rand::rngs::OsRng;
use std::collections::HashSet;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::time::{interval, sleep, Duration, Instant};
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
    /// The rate (txs/s) at which to send the transactions.
    #[clap(short, long, value_parser, value_name = "INT")]
    rate: u64,
    /// The amount for each transaction.
    #[clap(short, long, value_parser, value_name = "INT", default_value = "100")]
    amount: u64,
    /// The epoch for transactions.
    #[clap(
        short = 'e',
        long,
        value_parser,
        value_name = "INT",
        default_value = "0"
    )]
    epoch: u64,
    /// Network addresses of nodes to send transactions to.
    #[clap(
        short,
        long,
        value_parser,
        value_name = "ADDR",
        required = true,
        multiple = true
    )]
    nodes: Vec<SocketAddr>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    // Remove duplicates from nodes list
    let mut all_nodes: HashSet<SocketAddr> = HashSet::new();
    all_nodes.extend(cli.nodes.iter().cloned());
    let all_nodes_vec: Vec<SocketAddr> = all_nodes.into_iter().collect();

    info!("Node addresses: {:?}", all_nodes_vec);
    info!("Transaction amount: {}", cli.amount);
    info!("Transaction epoch: {}", cli.epoch);
    info!("Transactions rate: {} tx/s", cli.rate);
    let client = Client {
        rate: cli.rate,
        timeout: cli.timeout,
        amount: cli.amount,
        epoch: cli.epoch,
        nodes: all_nodes_vec,
    };

    // Wait for all nodes to be online and synchronized.
    client.wait().await;

    // Start the benchmark.
    client.send().await.context("Failed to submit transactions")
}

struct Client {
    rate: u64,
    timeout: u64,
    amount: u64,
    epoch: u64,
    nodes: Vec<SocketAddr>,
}

impl Client {
    pub async fn send(&self) -> Result<()> {
        const PRECISION: u64 = 20; // Sample precision.
        const BURST_DURATION: u64 = 1000 / PRECISION;

        // Connect to all nodes.
        info!("Connecting to {} nodes...", self.nodes.len());
        let mut transports = Vec::new();
        for address in &self.nodes {
            match TcpStream::connect(address).await {
                Ok(stream) => {
                    let transport = Framed::new(stream, LengthDelimitedCodec::new());
                    transports.push((address.clone(), transport));
                    info!("Connected to {}", address);
                }
                Err(e) => {
                    warn!("Failed to connect to {}: {}", address, e);
                }
            }
        }

        if transports.is_empty() {
            return Err(anyhow::Error::msg("Failed to connect to any node"));
        }

        info!(
            "Successfully connected to {}/{} nodes",
            transports.len(),
            self.nodes.len()
        );

        // Generate fixed sender and destination keys for this client instance.
        // Each transaction will have a unique nonce to ensure uniqueness.
        let mut rng = OsRng;
        let (sender, _) = generate_keypair(&mut rng);
        let (destination, _) = generate_keypair(&mut rng);

        info!("Sender: {}", sender);
        info!("Destination: {}", destination);

        // Submit all transactions.
        let burst = self.rate / PRECISION;
        let mut counter = 0u32;
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);

        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions");

        'main: loop {
            interval.as_mut().tick().await;
            let now = Instant::now();

            for x in 0..burst {
                if x == (counter as u64) % burst {
                    // NOTE: This log entry is used to compute performance.
                    info!("Sending sample transaction {}", counter);
                }

                // Create real transaction data.
                let tx_data = TransactionData {
                    sender,
                    amount: self.amount,
                    destination,
                    nonce: 0,
                    epoch: self.epoch,
                };

                // Serialize the transaction.
                let bytes: Bytes = tx_data
                    .to_transaction()
                    .context("Failed to serialize transaction")?
                    .into();

                // Broadcast to all connected nodes.
                let mut failed_nodes = Vec::new();
                for (idx, (address, transport)) in transports.iter_mut().enumerate() {
                    if let Err(e) = transport.send(bytes.clone()).await {
                        warn!("Failed to send transaction to {}: {}", address, e);
                        failed_nodes.push(idx);
                    }
                }

                // Remove failed connections (in reverse order to maintain indices).
                for &idx in failed_nodes.iter().rev() {
                    transports.remove(idx);
                }

                if transports.is_empty() {
                    warn!("All connections failed, stopping");
                    break 'main;
                }
            }
            if now.elapsed().as_millis() > BURST_DURATION as u128 {
                // NOTE: This log entry is used to compute performance.
                warn!("Transaction rate too high for this client");
            }
            counter += 1;
        }
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
