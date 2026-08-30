use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use tokio::signal::unix::{SignalKind, signal};

use crate::{
    catalog::Catalog,
    clickhouse::ClickHouse,
    config::BackupConfig,
    connect::{Connect, validate_offsets},
    model::{
        BackupDependency, BackupKind, BackupOutput, BackupReference, CHAIN_HEAD_FORMAT,
        CHAIN_HEAD_KEY, ChainHead, ConnectorCheckpoint, KafkaOffset, KafkaOffsetValue,
        KafkaPartition, KeeperCheckpoint, KeeperRow, Pipeline, RECOVERY_POINT_FORMAT,
        RecoveryPoint,
    },
};

struct BackupPlan {
    kind: BackupKind,
    chain_id: String,
    position: u32,
    base: Option<BackupReference>,
    chain_base: Option<BackupReference>,
    generation: u64,
}

pub async fn run() -> Result<()> {
    let config = BackupConfig::from_environment()?;
    let connectors = config
        .pipelines
        .iter()
        .map(|pipeline| pipeline.connector.as_str())
        .collect::<Vec<_>>();
    let connect = Connect::new(config.connect_url.clone())?;
    for connector in &connectors {
        connect.require_running(connector).await?;
    }

    let catalog = Catalog::new(
        &config.kafka_bootstrap_servers,
        &config.kafka_properties()?,
        config.recovery_topic.clone(),
        &config.run_id,
    )?;
    let head = catalog
        .get(CHAIN_HEAD_KEY)
        .await?
        .map(|value| serde_json::from_str::<ChainHead>(&value).context("invalid backup chain head"))
        .transpose()?;
    validate_head(head.as_ref())?;
    let plan = plan_backup(
        head.as_ref(),
        config.max_incrementals_per_full,
        &config.run_id,
    );

    let operation = execute(&config, &connect, &catalog, &plan);
    tokio::pin!(operation);
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let result = tokio::select! {
        result = &mut operation => result,
        _ = interrupt.recv() => Err(anyhow::anyhow!("backup interrupted")),
        _ = terminate.recv() => Err(anyhow::anyhow!("backup terminated")),
    };
    let resume = resume_all(&connect, &connectors).await;
    match (result, resume) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(operation), Ok(())) => Err(operation),
        (Ok(()), Err(resume)) => Err(resume),
        (Err(operation), Err(resume)) => Err(operation.context(resume)),
    }
}

async fn execute(
    config: &BackupConfig,
    connect: &Connect,
    catalog: &Catalog,
    plan: &BackupPlan,
) -> Result<()> {
    for pipeline in &config.pipelines {
        connect.pause(&pipeline.connector).await?;
    }
    for pipeline in &config.pipelines {
        connect
            .wait_paused(&pipeline.connector, config.pause_timeout)
            .await?;
    }

    let clickhouse = ClickHouse::new(
        config.clickhouse_url.clone(),
        config.clickhouse_username.clone(),
        config.clickhouse_password.clone(),
    )?;
    clickhouse
        .require_backup_engines(&config.pipelines, config.max_incrementals_per_full > 0)
        .await?;
    let mut checkpoints = Vec::with_capacity(config.pipelines.len());
    for pipeline in &config.pipelines {
        let observed = connect.offsets(&pipeline.connector).await?;
        let rows = clickhouse
            .keeper_rows(&pipeline.database, &pipeline.state_table)
            .await?;
        checkpoints.push(checkpoint(pipeline, observed, rows)?);
    }

    let stamp = Utc::now().format("%Y%m%dT%H%M%SZ");
    let path = match plan.kind {
        BackupKind::Full => format!(
            "{}/chains/{}/base-{}-{}.{}",
            config.path_prefix, plan.chain_id, stamp, config.run_id, config.archive_extension
        ),
        BackupKind::Incremental => format!(
            "{}/chains/{}/incremental-{:04}-{}-{}.{}",
            config.path_prefix,
            plan.chain_id,
            plan.position,
            stamp,
            config.run_id,
            config.archive_extension
        ),
    };
    let destination = format!("S3({}, '{}')", config.named_collection, path);
    let (id, _) = clickhouse
        .create_backup(
            &config.backup_objects,
            &destination,
            plan.base.as_ref().map(|backup| backup.name.as_str()),
        )
        .await?;
    let details = clickhouse.backup_details(id, &destination).await?;
    let checkpoint_path = format!(
        "{}/chains/{}/checkpoint-{:04}-{}-{}.{}",
        config.path_prefix,
        plan.chain_id,
        plan.position,
        stamp,
        config.run_id,
        config.archive_extension
    );
    let checkpoint_destination = format!("S3({}, '{}')", config.named_collection, checkpoint_path);
    let (checkpoint_id, _) = clickhouse
        .create_backup(&config.backup_state_objects, &checkpoint_destination, None)
        .await?;
    let checkpoint_details = clickhouse
        .backup_details(checkpoint_id, &checkpoint_destination)
        .await?;
    let backup = BackupReference {
        id,
        name: destination,
        kind: plan.kind.clone(),
        chain_id: plan.chain_id.clone(),
        position: plan.position,
        base: plan.base.as_ref().map(|backup| BackupDependency {
            id: backup.id,
            name: backup.name.clone(),
        }),
    };
    let recovery_point = RecoveryPoint {
        format: RECOVERY_POINT_FORMAT.to_owned(),
        created_at: Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        backup: backup.clone(),
        checkpoint_backup: BackupDependency {
            id: checkpoint_id,
            name: checkpoint_destination,
        },
        connectors: checkpoints,
    };
    validate_recovery_point(&recovery_point)?;
    let head = next_head(plan, backup);
    let manifest = serde_json::to_string(&recovery_point)?;
    let head_value = serde_json::to_string(&head)?;
    catalog
        .publish(&[(&id.to_string(), &manifest), (CHAIN_HEAD_KEY, &head_value)])
        .await?;
    println!(
        "{}",
        serde_json::to_string(&BackupOutput {
            details: &details,
            checkpoint_details: &checkpoint_details,
            recovery_point: &recovery_point
        })?
    );
    Ok(())
}

fn plan_backup(head: Option<&ChainHead>, max_incrementals: u32, run_id: &str) -> BackupPlan {
    match head.filter(|head| head.incrementals < max_incrementals) {
        Some(head) => BackupPlan {
            kind: BackupKind::Incremental,
            chain_id: head.chain_id.clone(),
            position: head.incrementals + 1,
            base: Some(head.latest.clone()),
            chain_base: Some(head.base.clone()),
            generation: head.generation + 1,
        },
        None => BackupPlan {
            kind: BackupKind::Full,
            chain_id: run_id.to_owned(),
            position: 0,
            base: None,
            chain_base: None,
            generation: head.map_or(1, |head| head.generation + 1),
        },
    }
}

fn next_head(plan: &BackupPlan, backup: BackupReference) -> ChainHead {
    let base = plan.chain_base.clone().unwrap_or_else(|| backup.clone());
    ChainHead {
        format: CHAIN_HEAD_FORMAT.to_owned(),
        generation: plan.generation,
        chain_id: plan.chain_id.clone(),
        base,
        latest: backup,
        incrementals: plan.position,
    }
}

fn checkpoint(
    pipeline: &Pipeline,
    observed: Vec<KafkaOffset>,
    rows: Vec<KeeperRow>,
) -> Result<ConnectorCheckpoint> {
    validate_offsets(&observed)?;
    let prefix = format!("{}-", pipeline.topic);
    if rows.iter().any(|row| !row.key.starts_with(&prefix)) {
        bail!("KeeperMap state contains an unexpected topic")
    }
    if rows.iter().any(|row| row.min_offset > row.max_offset) {
        bail!("KeeperMap state contains an invalid offset range")
    }
    let relevant = rows
        .iter()
        .filter_map(|row| {
            row.key
                .strip_prefix(&prefix)
                .map(|partition| (partition, row))
        })
        .map(|(partition, row)| {
            let partition = partition
                .parse::<u32>()
                .context("KeeperMap state key has an invalid partition")?;
            if partition >= pipeline.partitions {
                bail!("KeeperMap state contains an out-of-range partition")
            }
            if row.state != "AFTER_PROCESSING" {
                bail!("KeeperMap state is not safely committed: {}", row.key)
            }
            Ok((partition, row))
        })
        .collect::<Result<HashMap<_, _>>>()?;
    if relevant.len()
        != rows
            .iter()
            .filter(|row| row.key.starts_with(&prefix))
            .count()
    {
        bail!("KeeperMap state contains duplicate partitions")
    }

    let mut offsets = Vec::with_capacity(pipeline.partitions as usize);
    for partition in 0..pipeline.partitions {
        let exact = relevant
            .get(&partition)
            .map(|row| {
                row.max_offset
                    .checked_add(1)
                    .context("KeeperMap offset overflow")
            })
            .transpose()?
            .unwrap_or(0);
        if let Some(committed) = observed.iter().find(|offset| {
            offset.partition.kafka_topic == pipeline.topic
                && offset.partition.kafka_partition == partition
        }) && committed.offset.kafka_offset > exact
        {
            bail!(
                "Kafka Connect offset is ahead of ClickHouse KeeperMap state: {}-{partition}",
                pipeline.topic
            )
        }
        offsets.push(KafkaOffset {
            partition: KafkaPartition {
                kafka_topic: pipeline.topic.clone(),
                kafka_partition: partition,
            },
            offset: KafkaOffsetValue {
                kafka_offset: exact,
            },
        });
    }
    if observed.iter().any(|offset| {
        offset.partition.kafka_topic != pipeline.topic
            || offset.partition.kafka_partition >= pipeline.partitions
    }) {
        bail!("connector returned an unexpected topic partition")
    }
    Ok(ConnectorCheckpoint {
        name: pipeline.connector.clone(),
        topic: pipeline.topic.clone(),
        partitions: pipeline.partitions,
        offsets,
        observed_connect_offsets: observed,
        keeper: KeeperCheckpoint {
            database: pipeline.database.clone(),
            table: pipeline.state_table.clone(),
            rows,
        },
    })
}

fn validate_head(head: Option<&ChainHead>) -> Result<()> {
    let Some(head) = head else { return Ok(()) };
    let valid_latest = if head.incrementals == 0 {
        head.latest.kind == BackupKind::Full
            && head.latest.base.is_none()
            && head.latest == head.base
    } else {
        head.latest.kind == BackupKind::Incremental
            && head.latest.base.as_ref().is_some_and(|dependency| {
                dependency.id != head.latest.id && dependency.name != head.latest.name
            })
    };
    if head.format != CHAIN_HEAD_FORMAT
        || head.generation == 0
        || head.generation == u64::MAX
        || !safe_chain_id(&head.chain_id)
        || head.base.kind != BackupKind::Full
        || !backup_destination(&head.base.name)
        || head.base.chain_id != head.chain_id
        || head.base.position != 0
        || head.base.base.is_some()
        || !backup_destination(&head.latest.name)
        || head.latest.chain_id != head.chain_id
        || head.latest.position != head.incrementals
        || head
            .latest
            .base
            .as_ref()
            .is_some_and(|base| !backup_destination(&base.name))
        || !valid_latest
    {
        bail!("invalid backup chain head")
    }
    Ok(())
}

pub fn validate_recovery_point(point: &RecoveryPoint) -> Result<()> {
    let backup = &point.backup;
    let valid_backup = match backup.kind {
        BackupKind::Full => backup.position == 0 && backup.base.is_none(),
        BackupKind::Incremental => backup.position > 0 && backup.base.is_some(),
    };
    if point.format != RECOVERY_POINT_FORMAT
        || point.connectors.is_empty()
        || !safe_chain_id(&backup.chain_id)
        || !backup_destination(&backup.name)
        || backup
            .base
            .as_ref()
            .is_some_and(|base| !backup_destination(&base.name))
        || !backup_destination(&point.checkpoint_backup.name)
        || point.checkpoint_backup.id == backup.id
        || !valid_backup
    {
        bail!("invalid recovery-point manifest")
    }
    let mut names = point
        .connectors
        .iter()
        .map(|connector| &connector.name)
        .collect::<Vec<_>>();
    names.sort_unstable();
    if names.windows(2).any(|pair| pair[0] == pair[1]) {
        bail!("invalid recovery-point manifest")
    }
    for connector in &point.connectors {
        validate_offsets(&connector.offsets)?;
        validate_offsets(&connector.observed_connect_offsets)?;
        let mut row_keys = connector
            .keeper
            .rows
            .iter()
            .map(|row| &row.key)
            .collect::<Vec<_>>();
        row_keys.sort_unstable();
        if connector.name.is_empty()
            || connector.topic.is_empty()
            || connector.partitions == 0
            || connector.offsets.len() != connector.partitions as usize
            || !clickhouse_identifier(&connector.keeper.database)
            || !clickhouse_identifier(&connector.keeper.table)
            || connector
                .keeper
                .rows
                .iter()
                .any(|row| row.state != "AFTER_PROCESSING" || row.min_offset > row.max_offset)
            || connector.observed_connect_offsets.iter().any(|offset| {
                offset.partition.kafka_topic != connector.topic
                    || offset.partition.kafka_partition >= connector.partitions
            })
            || row_keys.windows(2).any(|pair| pair[0] == pair[1])
        {
            bail!("invalid recovery-point manifest")
        }
        for (partition, exact) in connector.offsets.iter().enumerate() {
            if exact.partition.kafka_topic != connector.topic
                || exact.partition.kafka_partition != partition as u32
            {
                bail!("invalid recovery-point manifest")
            }
            let key = format!(
                "{}-{}",
                exact.partition.kafka_topic, exact.partition.kafka_partition
            );
            let expected = connector
                .keeper
                .rows
                .iter()
                .find(|row| row.key == key)
                .map(|row| {
                    row.max_offset
                        .checked_add(1)
                        .context("KeeperMap offset overflow")
                })
                .transpose()?
                .unwrap_or(0);
            if exact.offset.kafka_offset != expected {
                bail!("recovery offset does not match ClickHouse KeeperMap state")
            }
            if connector
                .observed_connect_offsets
                .iter()
                .find(|observed| observed.partition == exact.partition)
                .is_some_and(|observed| observed.offset.kafka_offset > expected)
            {
                bail!("observed Kafka Connect offset is ahead of ClickHouse KeeperMap state")
            }
        }
        if connector.keeper.rows.iter().any(|row| {
            !connector.offsets.iter().any(|offset| {
                row.key
                    == format!(
                        "{}-{}",
                        offset.partition.kafka_topic, offset.partition.kafka_partition
                    )
            })
        }) {
            bail!("invalid recovery-point manifest")
        }
    }
    Ok(())
}

fn clickhouse_identifier(value: &str) -> bool {
    let mut characters = value.chars();
    characters
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
        && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

fn safe_chain_id(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '-')
}

fn backup_destination(value: &str) -> bool {
    let Some((collection, path)) = value
        .strip_prefix("S3(")
        .and_then(|value| value.strip_suffix("')"))
        .and_then(|value| value.split_once(", '"))
    else {
        return false;
    };
    clickhouse_identifier(collection)
        && !path.starts_with('/')
        && path
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "_./-".contains(character))
        && path
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
}

async fn resume_all(connect: &Connect, connectors: &[&str]) -> Result<()> {
    let mut failures = Vec::new();
    for connector in connectors {
        if let Err(error) = connect.resume(connector).await {
            failures.push(format!("{connector}: {error:#}"));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("failed to resume connectors: {}", failures.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use uuid::Uuid;

    use super::*;

    fn reference(kind: BackupKind, position: u32) -> BackupReference {
        BackupReference {
            id: Uuid::from_u128(position as u128 + 1),
            name: format!("S3(backups, 'root/chains/chain/backup-{position}.tar.zst')"),
            kind,
            chain_id: "chain".to_owned(),
            position,
            base: None,
        }
    }

    fn head(incrementals: u32) -> ChainHead {
        let mut latest = reference(
            if incrementals == 0 {
                BackupKind::Full
            } else {
                BackupKind::Incremental
            },
            incrementals,
        );
        if incrementals > 0 {
            latest.base = Some(BackupDependency {
                id: Uuid::from_u128(incrementals as u128),
                name: format!(
                    "S3(backups, 'root/chains/chain/backup-{}.tar.zst')",
                    incrementals - 1
                ),
            });
        }
        ChainHead {
            format: CHAIN_HEAD_FORMAT.to_owned(),
            generation: incrementals as u64 + 1,
            chain_id: "chain".to_owned(),
            base: reference(BackupKind::Full, 0),
            latest,
            incrementals,
        }
    }

    fn pipeline() -> Pipeline {
        Pipeline {
            connector: "events".to_owned(),
            database: "events".to_owned(),
            state_table: "events_state".to_owned(),
            table: "events".to_owned(),
            topic: "events.canonical".to_owned(),
            partitions: 3,
        }
    }

    fn offset(partition: u32, value: u64) -> KafkaOffset {
        KafkaOffset {
            partition: KafkaPartition {
                kafka_topic: "events.canonical".to_owned(),
                kafka_partition: partition,
            },
            offset: KafkaOffsetValue {
                kafka_offset: value,
            },
        }
    }

    fn row(partition: u32, max: u64, state: &str) -> KeeperRow {
        KeeperRow {
            key: format!("events.canonical-{partition}"),
            min_offset: max,
            max_offset: max,
            state: state.to_owned(),
        }
    }

    #[test]
    fn first_backup_is_full() {
        let plan = plan_backup(None, 3, "run");
        assert_eq!(plan.kind, BackupKind::Full);
        assert_eq!(plan.position, 0);
        assert_eq!(plan.chain_id, "run");
    }

    #[test]
    fn continues_incremental_chain_until_limit() {
        let chain = head(1);
        let plan = plan_backup(Some(&chain), 3, "run");
        assert_eq!(plan.kind, BackupKind::Incremental);
        assert_eq!(plan.position, 2);
        assert_eq!(
            plan.base.unwrap().name,
            "S3(backups, 'root/chains/chain/backup-1.tar.zst')"
        );
    }

    #[test]
    fn starts_new_full_at_limit_or_when_disabled() {
        assert_eq!(plan_backup(Some(&head(3)), 3, "new").kind, BackupKind::Full);
        assert_eq!(plan_backup(Some(&head(0)), 0, "new").kind, BackupKind::Full);
    }

    #[test]
    fn keeper_checkpoint_is_exact_even_when_connect_lags() {
        let checkpoint = checkpoint(
            &pipeline(),
            vec![offset(0, 8)],
            vec![row(0, 9, "AFTER_PROCESSING")],
        )
        .unwrap();
        assert_eq!(
            checkpoint.offsets,
            vec![offset(0, 10), offset(1, 0), offset(2, 0)]
        );
        assert_eq!(checkpoint.observed_connect_offsets, vec![offset(0, 8)]);
    }

    #[test]
    fn connect_may_lag_keeper_by_any_amount_but_never_lead() {
        for max_offset in [0, 1, 2, 31, 1024, u32::MAX as u64] {
            let exact = max_offset + 1;
            for observed in [0, 1, exact / 2, exact] {
                let point = checkpoint(
                    &pipeline(),
                    vec![offset(0, observed)],
                    vec![row(0, max_offset, "AFTER_PROCESSING")],
                )
                .unwrap();
                assert_eq!(point.offsets[0], offset(0, exact));
            }
            assert!(
                checkpoint(
                    &pipeline(),
                    vec![offset(0, exact + 1)],
                    vec![row(0, max_offset, "AFTER_PROCESSING")],
                )
                .is_err()
            );
        }
    }

    #[test]
    fn partitions_are_derived_independently_from_unordered_state() {
        let point = checkpoint(
            &pipeline(),
            vec![offset(2, 90), offset(0, 10)],
            vec![
                row(2, 99, "AFTER_PROCESSING"),
                row(0, 10, "AFTER_PROCESSING"),
            ],
        )
        .unwrap();
        assert_eq!(
            point.offsets,
            vec![offset(0, 11), offset(1, 0), offset(2, 100)]
        );
    }

    #[test]
    fn rejects_connect_ahead_of_clickhouse() {
        let error = checkpoint(
            &pipeline(),
            vec![offset(0, 11)],
            vec![row(0, 9, "AFTER_PROCESSING")],
        )
        .unwrap_err();
        assert!(error.to_string().contains("ahead"));
    }

    #[test]
    fn rejects_unfinished_keeper_state() {
        let error = checkpoint(
            &pipeline(),
            vec![offset(0, 9)],
            vec![row(0, 9, "BEFORE_PROCESSING")],
        )
        .unwrap_err();
        assert!(error.to_string().contains("not safely committed"));
    }

    #[test]
    fn rejects_out_of_range_and_duplicate_keeper_partitions() {
        assert!(checkpoint(&pipeline(), vec![], vec![row(3, 9, "AFTER_PROCESSING")]).is_err());
        assert!(
            checkpoint(
                &pipeline(),
                vec![],
                vec![
                    row(0, 9, "AFTER_PROCESSING"),
                    row(0, 10, "AFTER_PROCESSING")
                ]
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_keeper_rows_for_another_topic() {
        let mut unexpected = row(0, 9, "AFTER_PROCESSING");
        unexpected.key = "other-0".to_owned();
        assert!(checkpoint(&pipeline(), vec![], vec![unexpected]).is_err());
    }

    #[test]
    fn rejects_unexpected_connect_topic_and_offset_overflow() {
        let mut unexpected = offset(0, 1);
        unexpected.partition.kafka_topic = "other".to_owned();
        assert!(checkpoint(&pipeline(), vec![unexpected], vec![]).is_err());
        assert!(checkpoint(&pipeline(), vec![offset(3, 1)], vec![]).is_err());
        assert!(
            checkpoint(
                &pipeline(),
                vec![],
                vec![row(0, u64::MAX, "AFTER_PROCESSING")]
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_invalid_keeper_offset_range() {
        let mut invalid = row(0, 9, "AFTER_PROCESSING");
        invalid.min_offset = 10;
        assert!(checkpoint(&pipeline(), vec![], vec![invalid]).is_err());
    }

    #[test]
    fn validates_chain_head_structure() {
        assert!(validate_head(Some(&head(2))).is_ok());
        let mut invalid = head(2);
        invalid.latest.position = 1;
        assert!(validate_head(Some(&invalid)).is_err());
        invalid = head(2);
        invalid.base.kind = BackupKind::Incremental;
        assert!(validate_head(Some(&invalid)).is_err());
        invalid = head(2);
        invalid.chain_id.clear();
        assert!(validate_head(Some(&invalid)).is_err());
        invalid = head(2);
        invalid.latest.name = "S3(backups, 'root/../escape.tar.zst')".to_owned();
        assert!(validate_head(Some(&invalid)).is_err());
        invalid = head(2);
        invalid.latest.base = Some(BackupDependency {
            id: invalid.latest.id,
            name: invalid.latest.name.clone(),
        });
        assert!(validate_head(Some(&invalid)).is_err());
        invalid = head(2);
        invalid.generation = u64::MAX;
        assert!(validate_head(Some(&invalid)).is_err());
    }

    fn recovery_point() -> RecoveryPoint {
        RecoveryPoint {
            format: RECOVERY_POINT_FORMAT.to_owned(),
            created_at: "2026-08-30T12:00:00Z".to_owned(),
            backup: reference(BackupKind::Full, 0),
            checkpoint_backup: BackupDependency {
                id: Uuid::from_u128(999),
                name: "S3(backups, 'root/chains/chain/checkpoint.tar.zst')".to_owned(),
            },
            connectors: vec![ConnectorCheckpoint {
                name: "events".to_owned(),
                topic: "events.canonical".to_owned(),
                partitions: 3,
                offsets: vec![offset(0, 10), offset(1, 0), offset(2, 0)],
                observed_connect_offsets: vec![offset(0, 9)],
                keeper: KeeperCheckpoint {
                    database: "events".to_owned(),
                    table: "events_state".to_owned(),
                    rows: vec![row(0, 9, "AFTER_PROCESSING")],
                },
            }],
        }
    }

    #[test]
    fn validates_exact_recovery_point() {
        assert!(validate_recovery_point(&recovery_point()).is_ok());
    }

    #[test]
    fn rejects_tampered_recovery_offset() {
        let mut point = recovery_point();
        point.connectors[0].offsets[0].offset.kafka_offset = 9;
        assert!(validate_recovery_point(&point).is_err());

        point = recovery_point();
        point.connectors[0].offsets[0].offset.kafka_offset = 11;
        assert!(validate_recovery_point(&point).is_err());
    }

    #[test]
    fn rejects_observed_offset_ahead_in_manifest() {
        let mut point = recovery_point();
        point.connectors[0].observed_connect_offsets[0]
            .offset
            .kafka_offset = 11;
        assert!(validate_recovery_point(&point).is_err());
    }

    #[test]
    fn rejects_duplicate_connector_and_keeper_keys() {
        let mut point = recovery_point();
        point.connectors.push(point.connectors[0].clone());
        assert!(validate_recovery_point(&point).is_err());

        point = recovery_point();
        let row = point.connectors[0].keeper.rows[0].clone();
        point.connectors[0].keeper.rows.push(row);
        assert!(validate_recovery_point(&point).is_err());

        point = recovery_point();
        point.connectors[0].offsets.pop();
        assert!(validate_recovery_point(&point).is_err());

        point = recovery_point();
        point.connectors[0]
            .observed_connect_offsets
            .push(offset(0, 8));
        assert!(validate_recovery_point(&point).is_err());
    }

    #[test]
    fn rejects_invalid_backup_shape() {
        let mut point = recovery_point();
        point.backup.kind = BackupKind::Incremental;
        assert!(validate_recovery_point(&point).is_err());
        point = recovery_point();
        point.backup.position = 1;
        assert!(validate_recovery_point(&point).is_err());
    }

    #[test]
    fn rejects_tampered_topic_partition_shape() {
        let mut point = recovery_point();
        point.connectors[0].offsets.swap(0, 1);
        assert!(validate_recovery_point(&point).is_err());

        point = recovery_point();
        point.connectors[0].partitions = 2;
        assert!(validate_recovery_point(&point).is_err());

        point = recovery_point();
        point.connectors[0].topic = "other".to_owned();
        assert!(validate_recovery_point(&point).is_err());

        point = recovery_point();
        point.connectors[0].observed_connect_offsets[0]
            .partition
            .kafka_partition = 3;
        assert!(validate_recovery_point(&point).is_err());
    }

    #[test]
    fn rejects_unsafe_keeper_identity_and_range() {
        let mut point = recovery_point();
        point.connectors[0].keeper.table = "events`; DROP DATABASE events".to_owned();
        assert!(validate_recovery_point(&point).is_err());

        point = recovery_point();
        point.connectors[0].keeper.rows[0].min_offset = 10;
        assert!(validate_recovery_point(&point).is_err());

        point = recovery_point();
        point.checkpoint_backup.name = "S3(backups, 'root/../checkpoint.tar.zst')".to_owned();
        assert!(validate_recovery_point(&point).is_err());

        point = recovery_point();
        point.checkpoint_backup.id = point.backup.id;
        assert!(validate_recovery_point(&point).is_err());

        point = recovery_point();
        point.connectors[0].keeper.rows[0].key = "events.canonical-1".to_owned();
        assert!(validate_recovery_point(&point).is_err());
    }
}
