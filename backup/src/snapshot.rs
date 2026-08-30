use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail};

use crate::{
    backup::{checkpoint, require_unchanged_keeper, resume_all},
    clickhouse::ClickHouse,
    config::{BackupConfig, SnapshotScope},
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
    targets: Vec<SnapshotTable>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotLayout {
    groups: Vec<SnapshotGroup>,
}

impl SnapshotLayout {
    pub(crate) fn new(run_id: &str, scope: SnapshotScope, pipelines: &[Pipeline]) -> Self {
        let mut grouped = BTreeMap::<String, Vec<Pipeline>>::new();
        for pipeline in pipelines {
            let key = match scope {
                SnapshotScope::Table => format!("{}.{}", pipeline.database, pipeline.table),
                SnapshotScope::Database => pipeline.database.clone(),
            };
            grouped.entry(key).or_default().push(pipeline.clone());
        }

        let run_id = run_id.replace('-', "_");
        let mut target_index = 0;
        let groups = grouped
            .into_values()
            .map(|mut pipelines| {
                pipelines.sort_by(|left, right| left.connector.cmp(&right.connector));
                let targets = pipelines
                    .iter()
                    .map(|pipeline| (pipeline.database.clone(), pipeline.table.clone()))
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .map(|(database, source)| {
                        let snapshot = format!("__dcs_{run_id}_target_{target_index}");
                        target_index += 1;
                        SnapshotTable {
                            database,
                            source,
                            snapshot,
                        }
                    })
                    .collect();
                SnapshotGroup { pipelines, targets }
            })
            .collect();
        Self { groups }
    }

    pub(crate) async fn create(
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
            let barrier = async {
                pause_delivery(config, connect, &connectors).await?;
                let captured = capture_checkpoints(connect, clickhouse, &group.pipelines).await?;
                kafka.verify(&captured)?;
                for table in &group.targets {
                    clickhouse
                        .clone_target(&table.database, &table.source, &table.snapshot)
                        .await?;
                }
                require_stable_checkpoint(connect, clickhouse, &group.pipelines, &captured).await?;
                Ok::<_, anyhow::Error>(captured)
            }
            .await;
            let resume = resume_all(connect, &connectors).await;
            match (barrier, resume) {
                (Ok(captured), Ok(())) => checkpoints.extend(captured),
                (Err(barrier), Ok(())) => return Err(barrier),
                (Ok(_), Err(resume)) => return Err(resume),
                (Err(barrier), Err(resume)) => return Err(barrier.context(resume)),
            }
        }
        Ok(checkpoints)
    }

    pub(crate) fn backup_objects(&self) -> String {
        self.targets()
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
        for table in self.targets() {
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

    fn targets(&self) -> impl Iterator<Item = &SnapshotTable> {
        self.groups.iter().flat_map(|group| &group.targets)
    }
}

async fn pause_delivery(
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

async fn require_stable_checkpoint(
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
    fn table_scope_pauses_shared_targets_together() {
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
            SnapshotScope::Table,
            &[first, shared, other],
        );
        assert_eq!(layout.groups.len(), 2);
        assert_eq!(layout.groups[0].pipelines.len(), 1);
        assert_eq!(layout.groups[1].pipelines.len(), 2);
        assert_eq!(layout.targets().count(), 2);
        assert_eq!(layout.backup_objects().matches(" AS ").count(), 2);
    }

    #[test]
    fn database_scope_groups_all_tables_in_a_database() {
        let first = pipeline();
        let mut second = pipeline();
        second.connector = "other".to_owned();
        second.topic = "other.input".to_owned();
        second.table = "other_records".to_owned();
        second.state_table = "other_state".to_owned();
        second.keeper_path = "/durable-clickhouse-sink/default/other".to_owned();

        let layout = SnapshotLayout::new(
            "00000000-0000-0000-0000-000000000001",
            SnapshotScope::Database,
            &[first, second],
        );
        assert_eq!(layout.groups.len(), 1);
        assert_eq!(layout.groups[0].pipelines.len(), 2);
        assert_eq!(layout.groups[0].targets.len(), 2);
    }
}
