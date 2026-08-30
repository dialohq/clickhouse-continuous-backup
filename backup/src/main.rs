mod backup;
mod catalog;
mod clickhouse;
mod config;
mod connect;
mod model;
mod restore;

use anyhow::{Context, Result, bail};

#[tokio::main]
async fn main() -> Result<()> {
    let command = std::env::args()
        .nth(1)
        .context("expected command: backup or restore-offsets")?;
    match command.as_str() {
        "backup" => backup::run().await,
        "restore-offsets" => restore::run().await,
        _ => bail!("unknown command: {command}"),
    }
}
