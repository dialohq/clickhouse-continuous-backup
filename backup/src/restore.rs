use std::{fs, io::Read};

use anyhow::{Context, Result, bail};

use crate::{
    backup::validate_recovery_point,
    clickhouse::ClickHouse,
    config::RestoreConfig,
    connect::Connect,
    kafka::KafkaLog,
    model::{ConnectorCheckpoint, KeeperRow, RecoveryPoint},
};

pub async fn run() -> Result<()> {
    let config = RestoreConfig::from_environment()?;
    let manifest = read_manifest(&config.manifest_file)?;
    let point: RecoveryPoint =
        serde_json::from_str(&manifest).context("invalid recovery-point manifest")?;
    validate_recovery_point(&point)?;
    if point.backup.name != config.expected_backup_name {
        bail!("recovery-point backup does not match EXPECTED_BACKUP_NAME")
    }

    let mut expected = config.connector_names.clone();
    let mut actual = point
        .connectors
        .iter()
        .map(|connector| connector.name.clone())
        .collect::<Vec<_>>();
    expected.sort_unstable();
    actual.sort_unstable();
    if expected != actual {
        bail!("manifest connector set does not match this release")
    }

    let connect = Connect::new(config.connect_url.clone(), &config.timeouts)?;
    for connector in &config.connector_names {
        connect
            .require_stopped(connector, config.stop_timeout)
            .await?;
    }

    let kafka = KafkaLog::new(
        &config.kafka_bootstrap_servers,
        &config.kafka_properties()?,
        &config.timeouts,
    )?;
    let clickhouse = ClickHouse::new(
        config.clickhouse_url,
        config.clickhouse_username,
        config.clickhouse_password,
        &config.timeouts,
    )?;
    for checkpoint in &point.connectors {
        let actual = clickhouse
            .keeper_rows(&checkpoint.keeper.database, &checkpoint.keeper.table)
            .await?;
        require_keeper_match(checkpoint, actual)?;
    }
    kafka.verify(&point.connectors)?;

    for connector in &config.connector_names {
        connect
            .require_stopped(connector, config.stop_timeout)
            .await?;
    }
    for checkpoint in &point.connectors {
        connect
            .patch_offsets(&checkpoint.name, &checkpoint.offsets)
            .await?;
    }
    for checkpoint in &point.connectors {
        let mut actual = connect.offsets(&checkpoint.name).await?;
        let mut expected = checkpoint.offsets.clone();
        actual.sort_by_key(|offset| {
            (
                offset.partition.kafka_topic.clone(),
                offset.partition.kafka_partition,
            )
        });
        expected.sort_by_key(|offset| {
            (
                offset.partition.kafka_topic.clone(),
                offset.partition.kafka_partition,
            )
        });
        if actual != expected {
            bail!("connector offset verification failed: {}", checkpoint.name)
        }
    }
    for connector in &config.connector_names {
        connect
            .require_stopped(connector, config.stop_timeout)
            .await?;
    }
    println!("{manifest}");
    Ok(())
}

fn require_keeper_match(
    checkpoint: &ConnectorCheckpoint,
    mut actual: Vec<KeeperRow>,
) -> Result<()> {
    let mut expected = checkpoint.keeper.rows.clone();
    actual.sort_by(|left, right| left.key.cmp(&right.key));
    expected.sort_by(|left, right| left.key.cmp(&right.key));
    if actual != expected {
        bail!(
            "restored ClickHouse KeeperMap does not match the recovery point: {}",
            checkpoint.name
        )
    }
    Ok(())
}

fn read_manifest(path: &str) -> Result<String> {
    if path == "-" {
        let mut manifest = String::new();
        std::io::stdin().read_to_string(&mut manifest)?;
        Ok(manifest)
    } else {
        fs::read_to_string(path)
            .with_context(|| format!("failed to read recovery manifest: {path}"))
    }
}

#[cfg(test)]
mod tests {
    use crate::model::{ConnectorCheckpoint, KeeperCheckpoint, KeeperRow};

    use super::require_keeper_match;

    fn row(key: &str, max_offset: u64) -> KeeperRow {
        KeeperRow {
            key: key.to_owned(),
            min_offset: max_offset,
            max_offset,
            state: "AFTER_PROCESSING".to_owned(),
        }
    }

    fn checkpoint() -> ConnectorCheckpoint {
        ConnectorCheckpoint {
            name: "records".to_owned(),
            topic: "records.input".to_owned(),
            partitions: 2,
            offsets: vec![],
            observed_connect_offsets: vec![],
            keeper: KeeperCheckpoint {
                database: "records".to_owned(),
                table: "records_state".to_owned(),
                rows: vec![row("records.input-0", 10), row("records.input-1", 20)],
            },
        }
    }

    #[test]
    fn keeper_comparison_is_order_independent_and_exact() {
        let point = checkpoint();
        assert!(
            require_keeper_match(
                &point,
                vec![row("records.input-1", 20), row("records.input-0", 10)]
            )
            .is_ok()
        );
        assert!(
            require_keeper_match(
                &point,
                vec![row("records.input-0", 10), row("records.input-1", 19)]
            )
            .is_err()
        );
    }
}
