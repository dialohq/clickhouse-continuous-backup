use anyhow::Result;

use crate::{clickhouse::ClickHouse, config::TargetConfig};

pub async fn run(path: &std::path::Path) -> Result<()> {
    let config = TargetConfig::from_file(path)?;
    ClickHouse::new(
        config.clickhouse_url,
        config.clickhouse_username,
        config.clickhouse_password,
        &config.timeouts,
    )?
    .require_target_engines(&config.pipelines)
    .await
}
