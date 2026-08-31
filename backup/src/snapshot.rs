use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail};

use crate::{
    backup::{checkpoint, require_unchanged_keeper, resume_ingestion},
    clickhouse::ClickHouse,
    config::BackupConfig,
    connect::Connect,
    kafka::KafkaLog,
    model::{ConnectorCheckpoint, Pipeline},
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct SnapshotTable {
    database: String,
    source: String,
    snapshot: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SnapshotGroup {
    pipelines: Vec<Pipeline>,
    target: SnapshotTable,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotLayout {
    groups: Vec<SnapshotGroup>,
}

impl SnapshotLayout {
    pub(crate) fn new(run_id: &str, pipelines: &[Pipeline]) -> Self {
        let mut grouped = BTreeMap::<(String, String), Vec<Pipeline>>::new();
        for pipeline in pipelines {
            let key = (pipeline.database.clone(), pipeline.table.clone());
            grouped.entry(key).or_default().push(pipeline.clone());
        }

        let run_id = run_id.replace('-', "_");
        let groups = grouped
            .into_iter()
            .enumerate()
            .map(|(target_index, ((database, source), mut pipelines))| {
                pipelines.sort_by(|left, right| left.connector.cmp(&right.connector));
                let target = SnapshotTable {
                    database,
                    source,
                    snapshot: format!("__dcs_{run_id}_target_{target_index}"),
                };
                SnapshotGroup { pipelines, target }
            })
            .collect();
        Self { groups }
    }

    pub(crate) async fn capture_immutable_snapshots_during_short_ingestion_pauses(
        &self,
        config: &BackupConfig,
        connect: &Connect,
        clickhouse: &ClickHouse,
        kafka: &KafkaLog,
    ) -> Result<Vec<ConnectorCheckpoint>> {
        let mut checkpoints = Vec::with_capacity(config.pipelines.len());
        for group in &self.groups {
            let connectors = group
                .pipelines
                .iter()
                .map(|pipeline| pipeline.connector.as_str())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let snapshot = pause_ingestion_and_capture_snapshot(
                config,
                connect,
                clickhouse,
                kafka,
                group,
                &connectors,
            )
            .await;
            let resume = resume_ingestion(connect, &connectors).await;
            match (snapshot, resume) {
                (Ok(captured), Ok(())) => checkpoints.extend(captured),
                (Err(snapshot), Ok(())) => return Err(snapshot),
                (Ok(_), Err(resume)) => return Err(resume),
                (Err(snapshot), Err(resume)) => return Err(snapshot.context(resume)),
            }
        }
        Ok(checkpoints)
    }

    pub(crate) fn backup_objects(&self) -> String {
        self.groups
            .iter()
            .map(|group| &group.target)
            .map(|table| {
                format!(
                    "TABLE `{}`.`{}` AS `{}`.`{}`",
                    table.database, table.snapshot, table.database, table.source
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub(crate) async fn cleanup(&self, clickhouse: &ClickHouse) -> Result<()> {
        let mut failures = Vec::new();
        for table in self.groups.iter().map(|group| &group.target) {
            if let Err(error) = clickhouse
                .drop_table(&table.database, &table.snapshot)
                .await
            {
                failures.push(format!("{}.{}: {error:#}", table.database, table.snapshot));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            bail!("snapshot cleanup failed: {}", failures.join(", "))
        }
    }
}

async fn pause_ingestion_and_capture_snapshot(
    config: &BackupConfig,
    connect: &Connect,
    clickhouse: &ClickHouse,
    kafka: &KafkaLog,
    group: &SnapshotGroup,
    connectors: &[&str],
) -> Result<Vec<ConnectorCheckpoint>> {
    pause_ingestion(config, connect, connectors).await?;
    let checkpoints = capture_checkpoints(connect, clickhouse, &group.pipelines).await?;
    kafka.require_offsets_replayable(&checkpoints)?;
    let table = &group.target;
    clickhouse
        .clone_target(&table.database, &table.source, &table.snapshot)
        .await?;
    require_unchanged_checkpoint(connect, clickhouse, &group.pipelines, &checkpoints).await?;
    Ok(checkpoints)
}

async fn pause_ingestion(
    config: &BackupConfig,
    connect: &Connect,
    connectors: &[&str],
) -> Result<()> {
    for connector in connectors {
        connect.pause(connector).await?;
    }
    for connector in connectors {
        connect.wait_paused(connector, config.pause_timeout).await?;
    }
    Ok(())
}

async fn capture_checkpoints(
    connect: &Connect,
    clickhouse: &ClickHouse,
    pipelines: &[Pipeline],
) -> Result<Vec<ConnectorCheckpoint>> {
    let mut checkpoints = Vec::with_capacity(pipelines.len());
    for pipeline in pipelines {
        let observed = connect.offsets(&pipeline.connector).await?;
        let rows = clickhouse
            .keeper_rows(&pipeline.database, &pipeline.state_table)
            .await?;
        checkpoints.push(checkpoint(pipeline, observed, rows)?);
    }
    Ok(checkpoints)
}

async fn require_unchanged_checkpoint(
    connect: &Connect,
    clickhouse: &ClickHouse,
    pipelines: &[Pipeline],
    checkpoints: &[ConnectorCheckpoint],
) -> Result<()> {
    for (pipeline, checkpoint) in pipelines.iter().zip(checkpoints) {
        connect.require_paused(&pipeline.connector).await?;
        let current = clickhouse
            .keeper_rows(&pipeline.database, &pipeline.state_table)
            .await?;
        require_unchanged_keeper(checkpoint, current)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

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

    #[test]
    fn groups_shared_targets_together() {
        let first = pipeline();
        let mut shared = pipeline();
        shared.connector = "records-secondary".to_owned();
        shared.topic = "records.secondary".to_owned();
        shared.state_table = "records_secondary_state".to_owned();
        shared.keeper_path = "/durable-clickhouse-sink/default/records-secondary".to_owned();
        let mut other = pipeline();
        other.connector = "other".to_owned();
        other.topic = "other.input".to_owned();
        other.table = "other_records".to_owned();
        other.state_table = "other_state".to_owned();
        other.keeper_path = "/durable-clickhouse-sink/default/other".to_owned();

        let layout = SnapshotLayout::new(
            "00000000-0000-0000-0000-000000000001",
            &[first, shared, other],
        );
        assert_eq!(layout.groups.len(), 2);
        assert_eq!(layout.groups[0].pipelines.len(), 1);
        assert_eq!(layout.groups[1].pipelines.len(), 2);
        assert_eq!(layout.backup_objects().matches(" AS ").count(), 2);
    }
}
