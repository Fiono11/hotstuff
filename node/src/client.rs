use anyhow::{Context, Result};
use bytes::BufMut as _;
use bytes::BytesMut;
use clap::Parser;
use env_logger::Env;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use log::{info, warn};
use rand::Rng;
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
    /// The size of each transaction in bytes.
    #[clap(short, long, value_parser, value_name = "INT")]
    size: usize,
    /// The rate (txs/s) at which to send the transactions.
    #[clap(short, long, value_parser, value_name = "INT")]
    rate: u64,
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
    info!("Transactions size: {} B", cli.size);
    info!("Transactions rate: {} tx/s", cli.rate);
    let client = Client {
        size: cli.size,
        rate: cli.rate,
        timeout: cli.timeout,
        nodes: all_nodes_vec,
    };

    // Wait for all nodes to be online and synchronized.
    client.wait().await;

    // Start the benchmark.
    client.send().await.context("Failed to submit transactions")
}

struct Client {
    size: usize,
    rate: u64,
    timeout: u64,
    nodes: Vec<SocketAddr>,
}

impl Client {
    pub async fn send(&self) -> Result<()> {
        const PRECISION: u64 = 20; // Sample precision.
        const BURST_DURATION: u64 = 1000 / PRECISION;

        // The transaction size must be at least 16 bytes to ensure all txs are different.
        if self.size < 16 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 9 bytes",
            ));
        }

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

        // Submit all transactions.
        let burst = self.rate / PRECISION;
        let mut tx = BytesMut::with_capacity(self.size);
        let mut counter = 0;
        let mut r = rand::thread_rng().gen();
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);

        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions");

        'main: loop {
            interval.as_mut().tick().await;
            let now = Instant::now();

            for x in 0..burst {
                if x == counter % burst {
                    // NOTE: This log entry is used to compute performance.
                    info!("Sending sample transaction {}", counter);

                    tx.put_u8(0u8); // Sample txs start with 0.
                    tx.put_u64(counter); // This counter identifies the tx.
                } else {
                    r += 1;

                    tx.put_u8(1u8); // Standard txs start with 1.
                    tx.put_u64(r); // Ensures all clients send different txs.
                };
                tx.resize(self.size, 0u8);
                let bytes = tx.split().freeze();

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
