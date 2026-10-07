use anyhow::Result;
use clap::{Parser, Subcommand};
use durable_clickhouse_backup::{
    config::{BackupConfig, PauseServerConfig},
    controller, pause, preflight, recovery_resource,
};
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
    PauseServer { config: PathBuf },
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Backup { config } => {
            let config = BackupConfig::from_file(&config)?;
            let output = durable_clickhouse_backup::run(&config).await?;
            println!("{}", serde_json::to_string(&output)?);
            Ok(())
        }
        Command::ValidateTargets { config } => preflight::run(&config).await,
        Command::PrintRecoveryCrd => {
            print!("{}", recovery_resource::crd_yaml()?);
            Ok(())
        }
        Command::RecoveryController { config } => controller::run(&config).await,
        Command::PauseServer { config } => {
            let config = PauseServerConfig::from_file(&config)?;
            pause::run(&config).await
        }
    }
}
