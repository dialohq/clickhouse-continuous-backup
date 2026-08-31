mod backup;
mod clickhouse;
mod config;
mod connect;
mod controller;
mod kafka;
mod metadata;
mod model;
mod preflight;
mod recovery_resource;
mod replay;
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
    PrintRecoveryCrd,
    RecoveryController { config: PathBuf },
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Backup { config } => backup::run(&config).await,
        Command::ValidateTargets { config } => preflight::run(&config).await,
        Command::PrintRecoveryCrd => {
            print!("{}", recovery_resource::crd_yaml()?);
            Ok(())
        }
        Command::RecoveryController { config } => controller::run(&config).await,
    }
}
