mod backup;
mod catalog;
mod clickhouse;
mod config;
mod connect;
mod kafka;
mod model;
mod preflight;
mod snapshot;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Backup { config: PathBuf },
    ValidateTargets { config: PathBuf },
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Backup { config } => backup::run(&config).await,
        Command::ValidateTargets { config } => preflight::run(&config).await,
    }
}
