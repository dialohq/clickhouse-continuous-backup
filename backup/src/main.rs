mod backup;
mod catalog;
mod clickhouse;
mod config;
mod connect;
mod kafka;
mod model;
mod preflight;
mod snapshot;

use anyhow::{Context, Result, bail};

#[tokio::main]
async fn main() -> Result<()> {
    let command = std::env::args()
        .nth(1)
        .context("expected command: backup or validate-targets")?;
    match command.as_str() {
        "backup" => backup::run().await,
        "validate-targets" => preflight::run().await,
        _ => bail!("unknown command: {command}"),
    }
}
