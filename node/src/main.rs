mod config;
mod node;

use crate::config::Export as _;
use crate::config::{Committee, Secret};
use crate::node::Node;
use clap::{Parser, Subcommand};
use consensus::Committee as ConsensusCommittee;
use env_logger::Env;
use futures::future::join_all;
use ledger::Ledger;
use log::error;
use mempool::Committee as MempoolCommittee;
use std::fs;
use store::Store;
use tokio::task::JoinHandle;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    /// Turn debugging information on.
    #[clap(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    /// The command to execute.
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Generate a new keypair.
    Keys {
        /// The file where to print the new key pair.
        #[clap(short, long, value_parser, value_name = "FILE")]
        filename: String,
    },
    /// Run a single node.
    Run {
        /// The file containing the node keys.
        #[clap(short, long, value_parser, value_name = "FILE")]
        keys: String,
        /// The file containing committee information.
        #[clap(short, long, value_parser, value_name = "FILE")]
        committee: String,
        /// Optional file containing the node parameters.
        #[clap(short, long, value_parser, value_name = "FILE")]
        parameters: Option<String>,
        /// The path where to create the data store.
        #[clap(short, long, value_parser, value_name = "PATH")]
        store: String,
    },
    /// Deploy a local testbed with the specified number of nodes.
    Deploy {
        #[clap(short, long, value_parser = clap::value_parser!(u16).range(4..))]
        nodes: u16,
        /// Optional path to ledger store for computing stakes from account balances.
        #[clap(long, value_parser, value_name = "PATH")]
        ledger_store: Option<String>,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let log_level = match cli.verbose {
        0 => "error",
        1 => "warn",
        2 => "info",
        3 => "debug",
        _ => "trace",
    };
    let mut logger = env_logger::Builder::from_env(Env::default().default_filter_or(log_level));
    #[cfg(feature = "benchmark")]
    logger.format_timestamp_millis();
    logger.init();

    match cli.command {
        Command::Keys { filename } => {
            if let Err(e) = Node::print_key_file(&filename) {
                error!("{}", e);
            }
        }
        Command::Run {
            keys,
            committee,
            parameters,
            store,
        } => match Node::new(&committee, &keys, &store, parameters).await {
            Ok(mut node) => {
                tokio::spawn(async move {
                    node.analyze_block().await;
                })
                .await
                .expect("Failed to analyze committed blocks");
            }
            Err(e) => error!("{}", e),
        },
        Command::Deploy {
            nodes,
            ledger_store,
        } => match deploy_testbed(nodes, ledger_store.as_deref()).await {
            Ok(handles) => {
                let _ = join_all(handles).await;
            }
            Err(e) => error!("Failed to deploy testbed: {}", e),
        },
    }
}

async fn deploy_testbed(
    nodes: u16,
    ledger_store: Option<&str>,
) -> Result<Vec<JoinHandle<()>>, Box<dyn std::error::Error>> {
    let keys: Vec<_> = (0..nodes).map(|_| Secret::new()).collect();

    // Compute stakes from ledger if provided, otherwise use default of 1
    let stakes: Vec<u32> = if let Some(store_path) = ledger_store {
        compute_stakes_from_ledger(&keys, store_path).await?
    } else {
        vec![1; nodes as usize]
    };

    // Print the committee file.
    let epoch = 1;
    let mempool_committee = MempoolCommittee::new(
        keys.iter()
            .enumerate()
            .map(|(i, key)| {
                let name = key.name;
                let stake = stakes[i];
                let front = format!("127.0.0.1:{}", 25_000 + i).parse().unwrap();
                let mempool = format!("127.0.0.1:{}", 25_100 + i).parse().unwrap();
                (name, stake, front, mempool)
            })
            .collect(),
        epoch,
    );
    let consensus_committee = ConsensusCommittee::new(
        keys.iter()
            .enumerate()
            .map(|(i, key)| {
                let name = key.name;
                let stake = stakes[i];
                let addresses = format!("127.0.0.1:{}", 25_200 + i).parse().unwrap();
                (name, stake, addresses)
            })
            .collect(),
        epoch,
    );
    let committee_file = "committee.json";
    let _ = fs::remove_file(committee_file);
    Committee {
        mempool: mempool_committee,
        consensus: consensus_committee,
    }
    .write(committee_file)?;

    // Write the key files and spawn all nodes.
    keys.iter()
        .enumerate()
        .map(|(i, keypair)| {
            let key_file = format!("node_{}.json", i);
            let _ = fs::remove_file(&key_file);
            keypair.write(&key_file)?;

            let store_path = format!("db_{}", i);
            let _ = fs::remove_dir_all(&store_path);

            Ok(tokio::spawn(async move {
                match Node::new(committee_file, &key_file, &store_path, None).await {
                    Ok(mut node) => {
                        // Sink the commit channel.
                        while node.commit.recv().await.is_some() {}
                    }
                    Err(e) => error!("{}", e),
                }
            }))
        })
        .collect::<Result<_, Box<dyn std::error::Error>>>()
}

/// Compute stakes for each authority based on their account balance in the ledger.
/// Stakes are proportional to u128::MAX (the total supply).
async fn compute_stakes_from_ledger(
    keys: &[Secret],
    store_path: &str,
) -> Result<Vec<u32>, Box<dyn std::error::Error>> {
    let store = Store::new(store_path)?;
    let mut ledger = Ledger::new(store);

    const TOTAL_SUPPLY: u128 = u128::MAX;
    const MAX_STAKE: u32 = u32::MAX;

    let mut stakes = Vec::new();

    for key in keys {
        let balance = ledger.get_balance(&key.name).await?;

        // Compute stake proportionally: stake = (balance * u32::MAX) / u128::MAX
        // Since balance * u32::MAX can overflow u128, handle overflow case.
        let stake = if balance == 0 {
            0u32
        } else if balance == TOTAL_SUPPLY {
            MAX_STAKE
        } else {
            let max_stake_128 = MAX_STAKE as u128;

            // Try multiplication first, fall back to division if overflow
            if let Some(numerator) = balance.checked_mul(max_stake_128) {
                (numerator / TOTAL_SUPPLY) as u32
            } else {
                // Overflow: use division-first approach
                // stake ≈ balance / (TOTAL_SUPPLY / MAX_STAKE)
                let divisor = TOTAL_SUPPLY / max_stake_128;
                if divisor == 0 {
                    MAX_STAKE
                } else {
                    (balance / divisor).min(MAX_STAKE as u128) as u32
                }
            }
        };

        stakes.push(stake);
    }

    Ok(stakes)
}
