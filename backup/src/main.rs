use anyhow::Result;
use clap::{Parser, Subcommand};
use durable_clickhouse_backup::{
    config::{BackupConfig, PauseServerConfig},
    controller, pause, preflight, recovery_resource,
};
use std::{io::IsTerminal, path::PathBuf};
use tracing_subscriber::EnvFilter;

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
    // Logs go to stderr, so the backup's JSON output on stdout stays parseable. Defaults to info;
    // override with e.g. `RUST_LOG=durable_clickhouse_backup=debug`.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
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
