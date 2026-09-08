use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use tokio::signal::unix::{SignalKind, signal};

use crate::{
    clickhouse::ClickHouse,
    config::BackupConfig,
    connect::{Connect, validate_offsets},
    kafka::KafkaLog,
    metadata::{BackupMetadataLease, BackupMetadataStorage, KafkaBackupMetadataStorage},
    model::{
        BackupChain, BackupChainState, BackupKind, BackupManifest, BackupOutput, BackupParent,
        BackupReference, ConnectorCheckpoint, KafkaOffset, KafkaOffsetValue, KafkaPartition,
        KeeperCheckpoint, KeeperRow, Pipeline, clickhouse_identifier, kafka_name, safe_chain_id,
        safe_keeper_path, safe_storage_path,
    },
    snapshot::SnapshotLayout,
};

struct BackupPlan {
    kind: BackupKind,
    chain_id: String,
    position: u32,
    parent: Option<BackupReference>,
    root: Option<BackupReference>,
    generation: u64,
    pipelines: Vec<Pipeline>,
}

pub async fn run(config: &BackupConfig) -> Result<BackupOutput> {
    config.validate()?;
    let connectors = config
        .pipelines
        .iter()
        .map(|pipeline| pipeline.connector.as_str())
        .collect::<Vec<_>>();
    let connect = Connect::new(config.connect_url.clone(), &config.timeouts)?;
    for connector in &connectors {
        connect.require_running(connector).await?;
    }

    let metadata = KafkaBackupMetadataStorage::new(
        &config.kafka_bootstrap_servers,
        &config.kafka_properties()?,
        config.catalog_topic.clone(),
        &config.run_id,
        &config.timeouts,
    )?;
    let lease = metadata.acquire().await?;
    validate_chain(lease.chain(), &config.pipelines)?;
    let plan = plan_backup(
        lease.chain_state(),
        config.max_incrementals_per_full,
        &config.run_id,
        &config.pipelines,
    );

    let clickhouse = ClickHouse::new(
        config.clickhouse_url.clone(),
        config.clickhouse_username.clone(),
        config.clickhouse_password.clone(),
        &config.timeouts,
    )?;
    clickhouse.require_backup_engines(&config.pipelines).await?;
    let snapshots = SnapshotLayout::new(&config.run_id, &config.pipelines);
    snapshots.cleanup(&clickhouse).await?;

    let operation = create_backup(config, &connect, &clickhouse, lease, &plan, &snapshots);
    tokio::pin!(operation);
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let result = tokio::select! {
        result = &mut operation => result,
        _ = interrupt.recv() => Err(anyhow::anyhow!("backup interrupted")),
        _ = terminate.recv() => Err(anyhow::anyhow!("backup terminated")),
    };
    let resume = resume_ingestion(&connect, &connectors).await;
    if let Err(error) = snapshots.cleanup(&clickhouse).await {
        eprintln!("failed to remove ClickHouse snapshot tables: {error:#}");
    }
    match (result, resume) {
        (Ok(output), Ok(())) => Ok(output),
        (Err(operation), Ok(())) => Err(operation),
        (Ok(_), Err(resume)) => Err(resume),
        (Err(operation), Err(resume)) => Err(operation.context(resume)),
    }
}

async fn create_backup(
    config: &BackupConfig,
    connect: &Connect,
    clickhouse: &ClickHouse,
    metadata: impl BackupMetadataLease,
    plan: &BackupPlan,
    snapshots: &SnapshotLayout,
) -> Result<BackupOutput> {
    let kafka = KafkaLog::new(
        &config.kafka_bootstrap_servers,
        &config.kafka_properties()?,
        &config.timeouts,
    )?;

    let checkpoints = snapshots
        .capture_immutable_snapshots_during_short_ingestion_pauses(
            config, connect, clickhouse, &kafka,
        )
        .await?;
    kafka.require_offsets_replayable(&checkpoints)?;

    let stamp = Utc::now().format("%Y%m%dT%H%M%SZ");
    let archive_path = match plan.kind {
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
    let destination = format!("S3({}, '{}')", config.named_collection, archive_path);
    let (archive_id, _) = clickhouse
        .upload_backup(
            &snapshots.backup_objects(),
            &destination,
            plan.parent.as_ref().map(|backup| backup.name.as_str()),
            config.max_backup_bandwidth,
        )
        .await?;
    let details = clickhouse
        .require_backup_created(archive_id, &destination)
        .await?;

    let manifest = BackupManifest {
        created_at: Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        backup: BackupReference {
            id: archive_id,
            name: destination,
            kind: plan.kind.clone(),
            chain_id: plan.chain_id.clone(),
            position: plan.position,
            parent: plan.parent.as_ref().map(|backup| BackupParent {
                id: backup.id,
                name: backup.name.clone(),
            }),
        },
        connectors: checkpoints,
    };
    validate_manifest(&manifest)?;
    kafka.require_offsets_replayable(&manifest.connectors)?;

    let chain_state = BackupChainState {
        generation: plan.generation,
        chain_id: plan.chain_id.clone(),
        root: plan.root.clone().unwrap_or_else(|| manifest.backup.clone()),
        tip: manifest.backup.clone(),
        incremental_count: plan.position,
        pipelines: plan.pipelines.clone(),
    };
    metadata.commit(&manifest, &chain_state).await?;

    Ok(BackupOutput { details, manifest })
}

pub(crate) fn require_unchanged_keeper(
    checkpoint: &ConnectorCheckpoint,
    mut current: Vec<KeeperRow>,
) -> Result<()> {
    let mut expected = checkpoint.keeper.rows.clone();
    current.sort_by(|left, right| left.key.cmp(&right.key));
    expected.sort_by(|left, right| left.key.cmp(&right.key));
    if current != expected {
        bail!(
            "KeeperMap changed while backup was running: {}",
            checkpoint.name
        )
    }
    Ok(())
}

fn plan_backup(
    chain_state: Option<&BackupChainState>,
    max_incrementals: u32,
    run_id: &str,
    pipelines: &[Pipeline],
) -> BackupPlan {
    match chain_state.filter(|state| state.incremental_count < max_incrementals) {
        Some(state) => BackupPlan {
            kind: BackupKind::Incremental,
            chain_id: state.chain_id.clone(),
            position: state.incremental_count + 1,
            parent: Some(state.tip.clone()),
            root: Some(state.root.clone()),
            generation: state.generation + 1,
            pipelines: pipelines.to_vec(),
        },
        None => BackupPlan {
            kind: BackupKind::Full,
            chain_id: run_id.to_owned(),
            position: 0,
            parent: None,
            root: None,
            generation: chain_state.map_or(1, |state| state.generation + 1),
            pipelines: pipelines.to_vec(),
        },
    }
}

pub(crate) fn checkpoint(
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
    if rows.iter().any(|row| row.max_offset > i64::MAX as u64) {
        bail!("KeeperMap state exceeds the connector's signed offset range")
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
            path: pipeline.keeper_path.clone(),
            rows,
        },
    })
}

pub fn validate_chain(chain: Option<&BackupChain>, pipelines: &[Pipeline]) -> Result<()> {
    let Some(chain) = chain else { return Ok(()) };
    let state = &chain.state;
    validate_chain_state(Some(state), pipelines)?;
    // the reader returns manifests from the current tip back to the full backup.
    if chain.manifests.len() as u64 != u64::from(state.incremental_count) + 1
        || state.generation <= u64::from(state.incremental_count)
        || chain.manifests.first().map(|point| &point.backup) != Some(&state.tip)
        || chain.manifests.last().map(|point| &point.backup) != Some(&state.root)
        || has_duplicates(
            chain
                .manifests
                .iter()
                .map(|point| point.backup.id)
                .collect(),
        )
        || has_duplicates(
            chain
                .manifests
                .iter()
                .map(|point| &point.backup.name)
                .collect(),
        )
    {
        bail!("invalid backup chain: count, endpoints, generation, or duplicate backups")
    }
    for (index, point) in chain.manifests.iter().enumerate() {
        validate_manifest(point)?;
        if point.backup.id.is_nil()
            || point.backup.chain_id != state.chain_id
            || u64::from(point.backup.position) != u64::from(state.incremental_count) - index as u64
            || point.connectors.len() != pipelines.len()
        {
            bail!("invalid backup chain manifest {}", point.backup.id)
        }
        let parent = chain.manifests.get(index + 1);
        if point
            .backup
            .parent
            .as_ref()
            .map(|parent| (parent.id, &parent.name))
            != parent.map(|parent| (parent.backup.id, &parent.backup.name))
        {
            bail!("invalid backup chain parent for {}", point.backup.id)
        }
        for pipeline in pipelines {
            let checkpoint = point
                .connectors
                .iter()
                .find(|checkpoint| {
                    checkpoint.name == pipeline.connector
                        && checkpoint.topic == pipeline.topic
                        && checkpoint.partitions == pipeline.partitions
                        && checkpoint.keeper.database == pipeline.database
                        && checkpoint.keeper.table == pipeline.state_table
                        && checkpoint.keeper.path == pipeline.keeper_path
                })
                .context("backup chain manifest does not match configured pipelines")?;
            if let Some(parent) = parent {
                let previous = parent
                    .connectors
                    .iter()
                    .find(|previous| previous.name == checkpoint.name)
                    .context("backup chain parent is missing a connector")?;
                if checkpoint
                    .offsets
                    .iter()
                    .zip(&previous.offsets)
                    .any(|(current, previous)| {
                        current.offset.kafka_offset < previous.offset.kafka_offset
                    })
                {
                    bail!(
                        "backup chain offsets move backwards for {}",
                        checkpoint.name
                    )
                }
            }
        }
    }
    Ok(())
}

fn validate_chain_state(state: Option<&BackupChainState>, pipelines: &[Pipeline]) -> Result<()> {
    let Some(state) = state else { return Ok(()) };
    let valid_tip = if state.incremental_count == 0 {
        state.tip.kind == BackupKind::Full && state.tip.parent.is_none() && state.tip == state.root
    } else {
        state.tip.kind == BackupKind::Incremental
            && state
                .tip
                .parent
                .as_ref()
                .is_some_and(|parent| parent.id != state.tip.id && parent.name != state.tip.name)
    };
    if state.generation == 0
        || state.generation == u64::MAX
        || state.pipelines != pipelines
        || !safe_chain_id(&state.chain_id)
        || state.root.kind != BackupKind::Full
        || !backup_destination(&state.root.name)
        || state.root.chain_id != state.chain_id
        || state.root.position != 0
        || state.root.parent.is_some()
        || !backup_destination(&state.tip.name)
        || state.tip.chain_id != state.chain_id
        || state.tip.position != state.incremental_count
        || state
            .tip
            .parent
            .as_ref()
            .is_some_and(|parent| !backup_destination(&parent.name))
        || !valid_tip
    {
        bail!("invalid backup chain state")
    }
    Ok(())
}

pub fn validate_manifest(point: &BackupManifest) -> Result<()> {
    let backup = &point.backup;
    let valid_backup = match backup.kind {
        BackupKind::Full => backup.position == 0 && backup.parent.is_none(),
        BackupKind::Incremental => backup.position > 0 && backup.parent.is_some(),
    };
    if point.connectors.is_empty()
        || !safe_chain_id(&backup.chain_id)
        || !backup_destination(&backup.name)
        || backup
            .parent
            .as_ref()
            .is_some_and(|parent| !backup_destination(&parent.name))
        || !valid_backup
    {
        bail!("invalid backup manifest")
    }
    if has_duplicates(
        point
            .connectors
            .iter()
            .map(|connector| &connector.name)
            .collect(),
    ) {
        bail!("invalid backup manifest")
    }
    for connector in &point.connectors {
        validate_offsets(&connector.offsets)?;
        validate_offsets(&connector.observed_connect_offsets)?;
        if !kafka_name(&connector.name)
            || !kafka_name(&connector.topic)
            || connector.partitions == 0
            || connector.offsets.len() != connector.partitions as usize
            || !clickhouse_identifier(&connector.keeper.database)
            || !clickhouse_identifier(&connector.keeper.table)
            || !safe_keeper_path(&connector.keeper.path)
            || connector.keeper.rows.iter().any(|row| {
                row.state != "AFTER_PROCESSING"
                    || row.min_offset > row.max_offset
                    || row.max_offset > i64::MAX as u64
            })
            || connector.observed_connect_offsets.iter().any(|offset| {
                offset.partition.kafka_topic != connector.topic
                    || offset.partition.kafka_partition >= connector.partitions
            })
            || has_duplicates(connector.keeper.rows.iter().map(|row| &row.key).collect())
        {
            bail!("invalid backup manifest")
        }
        for (partition, exact) in connector.offsets.iter().enumerate() {
            if exact.partition.kafka_topic != connector.topic
                || exact.partition.kafka_partition != partition as u32
            {
                bail!("invalid backup manifest")
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
                bail!("backup offset does not match ClickHouse KeeperMap state")
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
            bail!("invalid backup manifest")
        }
    }
    Ok(())
}

fn has_duplicates<T: Ord>(mut values: Vec<T>) -> bool {
    values.sort_unstable();
    values.windows(2).any(|pair| pair[0] == pair[1])
}

fn backup_destination(value: &str) -> bool {
    let Some((collection, path)) = value
        .strip_prefix("S3(")
        .and_then(|value| value.strip_suffix("')"))
        .and_then(|value| value.split_once(", '"))
    else {
        return false;
    };
    clickhouse_identifier(collection) && safe_storage_path(path)
}

pub(crate) async fn resume_ingestion(connect: &Connect, connectors: &[&str]) -> Result<()> {
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
            parent: None,
        }
    }

    fn chain_state(incremental_count: u32) -> BackupChainState {
        let mut tip = reference(
            if incremental_count == 0 {
                BackupKind::Full
            } else {
                BackupKind::Incremental
            },
            incremental_count,
        );
        if incremental_count > 0 {
            tip.parent = Some(BackupParent {
                id: Uuid::from_u128(incremental_count as u128),
                name: format!(
                    "S3(backups, 'root/chains/chain/backup-{}.tar.zst')",
                    incremental_count - 1
                ),
            });
        }
        BackupChainState {
            generation: incremental_count as u64 + 1,
            chain_id: "chain".to_owned(),
            root: reference(BackupKind::Full, 0),
            tip,
            incremental_count,
            pipelines: vec![pipeline()],
        }
    }

    fn pipeline() -> Pipeline {
        Pipeline {
            connector: "records".to_owned(),
            database: "records".to_owned(),
            state_table: "records_state".to_owned(),
            keeper_path: "/durable-clickhouse-sink/default/records".to_owned(),
            table: "records".to_owned(),
            topic: "records.input".to_owned(),
            partitions: 3,
        }
    }

    fn offset(partition: u32, value: u64) -> KafkaOffset {
        KafkaOffset {
            partition: KafkaPartition {
                kafka_topic: "records.input".to_owned(),
                kafka_partition: partition,
            },
            offset: KafkaOffsetValue {
                kafka_offset: value,
            },
        }
    }

    fn row(partition: u32, max: u64, state: &str) -> KeeperRow {
        KeeperRow {
            key: format!("records.input-{partition}"),
            min_offset: max,
            max_offset: max,
            state: state.to_owned(),
        }
    }

    #[test]
    fn first_backup_is_full() {
        let plan = plan_backup(None, 3, "run", &[pipeline()]);
        assert_eq!(plan.kind, BackupKind::Full);
        assert_eq!(plan.position, 0);
        assert_eq!(plan.chain_id, "run");
    }

    #[test]
    fn continues_incremental_chain_until_limit() {
        let chain = chain_state(1);
        let plan = plan_backup(Some(&chain), 3, "run", &[pipeline()]);
        assert_eq!(plan.kind, BackupKind::Incremental);
        assert_eq!(plan.position, 2);
        assert_eq!(
            plan.parent.unwrap().name,
            "S3(backups, 'root/chains/chain/backup-1.tar.zst')"
        );
    }

    #[test]
    fn starts_new_full_at_limit_or_when_disabled() {
        assert_eq!(
            plan_backup(Some(&chain_state(3)), 3, "new", &[pipeline()]).kind,
            BackupKind::Full
        );
        assert_eq!(
            plan_backup(Some(&chain_state(0)), 0, "new", &[pipeline()]).kind,
            BackupKind::Full
        );
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
    fn detects_keeper_movement_during_backup() {
        let checkpoint = checkpoint(
            &pipeline(),
            vec![offset(0, 10)],
            vec![row(0, 9, "AFTER_PROCESSING")],
        )
        .unwrap();
        assert!(require_unchanged_keeper(&checkpoint, vec![row(0, 9, "AFTER_PROCESSING")]).is_ok());
        assert!(
            require_unchanged_keeper(&checkpoint, vec![row(0, 10, "AFTER_PROCESSING")]).is_err()
        );
    }

    #[test]
    fn validates_chain_state_structure() {
        assert!(validate_chain_state(Some(&chain_state(2)), &[pipeline()]).is_ok());
        let mut invalid = chain_state(2);
        invalid.tip.position = 1;
        assert!(validate_chain_state(Some(&invalid), &[pipeline()]).is_err());
        invalid = chain_state(2);
        invalid.root.kind = BackupKind::Incremental;
        assert!(validate_chain_state(Some(&invalid), &[pipeline()]).is_err());
        invalid = chain_state(2);
        invalid.chain_id.clear();
        assert!(validate_chain_state(Some(&invalid), &[pipeline()]).is_err());
        invalid = chain_state(2);
        invalid.tip.name = "S3(backups, 'root/../escape.tar.zst')".to_owned();
        assert!(validate_chain_state(Some(&invalid), &[pipeline()]).is_err());
        invalid = chain_state(2);
        invalid.tip.parent = Some(BackupParent {
            id: invalid.tip.id,
            name: invalid.tip.name.clone(),
        });
        assert!(validate_chain_state(Some(&invalid), &[pipeline()]).is_err());
        invalid = chain_state(2);
        invalid.generation = u64::MAX;
        assert!(validate_chain_state(Some(&invalid), &[pipeline()]).is_err());
        let mut different = pipeline();
        different.topic = "other".to_owned();
        assert!(validate_chain_state(Some(&chain_state(2)), &[different]).is_err());
    }

    fn manifest() -> BackupManifest {
        BackupManifest {
            created_at: "2026-08-30T12:00:00Z".to_owned(),
            backup: reference(BackupKind::Full, 0),
            connectors: vec![ConnectorCheckpoint {
                name: "records".to_owned(),
                topic: "records.input".to_owned(),
                partitions: 3,
                offsets: vec![offset(0, 10), offset(1, 0), offset(2, 0)],
                observed_connect_offsets: vec![offset(0, 9)],
                keeper: KeeperCheckpoint {
                    database: "records".to_owned(),
                    table: "records_state".to_owned(),
                    path: "/durable-clickhouse-sink/default/records".to_owned(),
                    rows: vec![row(0, 9, "AFTER_PROCESSING")],
                },
            }],
        }
    }

    #[test]
    fn validates_exact_manifest() {
        assert!(validate_manifest(&manifest()).is_ok());
    }

    fn chain(incremental_count: u32) -> BackupChain {
        BackupChain {
            state: chain_state(incremental_count),
            manifests: (0..=incremental_count)
                .rev()
                .map(|position| {
                    let mut point = manifest();
                    point.backup = chain_state(position).tip;
                    point
                })
                .collect(),
        }
    }

    #[test]
    fn validates_complete_backup_chains() {
        assert!(validate_chain(None, &[pipeline()]).is_ok());
        for count in 0..=3 {
            assert!(validate_chain(Some(&chain(count)), &[pipeline()]).is_ok());
        }
    }

    #[test]
    fn rejects_broken_backup_chain_links() {
        let reject = |mutate: fn(&mut BackupChain)| {
            let mut chain = chain(2);
            mutate(&mut chain);
            assert!(validate_chain(Some(&chain), &[pipeline()]).is_err());
        };
        reject(|chain| {
            chain.manifests.clear();
        });
        reject(|chain| {
            chain.manifests.remove(1);
        });
        reject(|chain| {
            chain.manifests.push(chain.manifests[2].clone());
        });
        reject(|chain| {
            chain.state.tip.id = Uuid::from_u128(999);
        });
        reject(|chain| {
            chain.state.root.id = Uuid::from_u128(999);
        });
        reject(|chain| {
            chain.state.generation = 2;
        });
        reject(|chain| {
            chain.manifests[1].backup.id = Uuid::nil();
        });
        reject(|chain| {
            chain.manifests[1].backup.id = chain.manifests[0].backup.id;
        });
        reject(|chain| {
            chain.manifests[1].backup.name = chain.manifests[0].backup.name.clone();
        });
        reject(|chain| {
            chain.manifests[1].backup.chain_id = "other-chain".to_owned();
        });
        reject(|chain| {
            chain.manifests[1].backup.position = 2;
        });
        reject(|chain| {
            chain.manifests[1].backup.parent.as_mut().unwrap().id = Uuid::from_u128(999);
        });
        reject(|chain| {
            chain.manifests[1].backup.parent.as_mut().unwrap().name =
                "S3(backups, 'other.tar.zst')".to_owned();
        });
        reject(|chain| {
            chain.manifests[1].backup.parent = None;
        });
    }

    #[test]
    fn rejects_invalid_backup_chain_checkpoints() {
        let reject = |mutate: fn(&mut BackupChain)| {
            let mut chain = chain(2);
            mutate(&mut chain);
            assert!(validate_chain(Some(&chain), &[pipeline()]).is_err());
        };
        reject(|chain| {
            chain.manifests[1].connectors.clear();
        });
        reject(|chain| {
            chain.manifests[1].connectors[0].name = "other".to_owned();
        });
        reject(|chain| {
            chain.manifests[1].connectors[0].keeper.path = "/other".to_owned();
        });
        reject(|chain| {
            chain.manifests[1].connectors[0].keeper.rows[0].state = "BEFORE_PROCESSING".to_owned();
        });
        reject(|chain| {
            chain.manifests[1].connectors[0].offsets[0]
                .offset
                .kafka_offset = 11;
        });
        // Each manifest is valid on its own, but the child's checkpoint regresses.
        let mut backwards = chain(1);
        backwards.manifests[1].connectors[0].keeper.rows[0].max_offset = 10;
        backwards.manifests[1].connectors[0].offsets[0]
            .offset
            .kafka_offset = 11;
        assert!(validate_manifest(&backwards.manifests[1]).is_ok());
        assert!(validate_chain(Some(&backwards), &[pipeline()]).is_err());
    }

    #[test]
    fn rejects_tampered_backup_offset() {
        let mut point = manifest();
        point.connectors[0].offsets[0].offset.kafka_offset = 9;
        assert!(validate_manifest(&point).is_err());

        point = manifest();
        point.connectors[0].offsets[0].offset.kafka_offset = 11;
        assert!(validate_manifest(&point).is_err());
    }

    #[test]
    fn rejects_observed_offset_ahead_in_manifest() {
        let mut point = manifest();
        point.connectors[0].observed_connect_offsets[0]
            .offset
            .kafka_offset = 11;
        assert!(validate_manifest(&point).is_err());
    }

    #[test]
    fn rejects_duplicate_connector_and_keeper_keys() {
        let mut point = manifest();
        point.connectors.push(point.connectors[0].clone());
        assert!(validate_manifest(&point).is_err());

        point = manifest();
        let row = point.connectors[0].keeper.rows[0].clone();
        point.connectors[0].keeper.rows.push(row);
        assert!(validate_manifest(&point).is_err());

        point = manifest();
        point.connectors[0].offsets.pop();
        assert!(validate_manifest(&point).is_err());

        point = manifest();
        point.connectors[0]
            .observed_connect_offsets
            .push(offset(0, 8));
        assert!(validate_manifest(&point).is_err());
    }

    #[test]
    fn rejects_invalid_backup_shape() {
        let mut point = manifest();
        point.backup.kind = BackupKind::Incremental;
        assert!(validate_manifest(&point).is_err());
        point = manifest();
        point.backup.position = 1;
        assert!(validate_manifest(&point).is_err());
    }

    #[test]
    fn rejects_tampered_topic_partition_shape() {
        let mut point = manifest();
        point.connectors[0].offsets.swap(0, 1);
        assert!(validate_manifest(&point).is_err());

        point = manifest();
        point.connectors[0].partitions = 2;
        assert!(validate_manifest(&point).is_err());

        point = manifest();
        point.connectors[0].topic = "other".to_owned();
        assert!(validate_manifest(&point).is_err());

        point = manifest();
        point.connectors[0].observed_connect_offsets[0]
            .partition
            .kafka_partition = 3;
        assert!(validate_manifest(&point).is_err());
    }

    #[test]
    fn rejects_unsafe_keeper_identity_and_range() {
        let mut point = manifest();
        point.connectors[0].keeper.table = "records`; DROP DATABASE records".to_owned();
        assert!(validate_manifest(&point).is_err());

        point = manifest();
        point.connectors[0].keeper.rows[0].min_offset = 10;
        assert!(validate_manifest(&point).is_err());

        point = manifest();
        point.connectors[0].keeper.path = "/durable-clickhouse-sink/../records".to_owned();
        assert!(validate_manifest(&point).is_err());

        point = manifest();
        point.connectors[0].keeper.rows[0].key = "records.input-1".to_owned();
        assert!(validate_manifest(&point).is_err());
    }
}
