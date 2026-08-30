use std::{path::Path, sync::Arc};

use anyhow::{Context as _, Result, bail};
use chrono::Utc;
use futures::StreamExt;
use kube::{
    Api, Client, ResourceExt,
    api::{Patch, PatchParams},
    runtime::{Controller, controller::Action, watcher},
};
use serde_json::{Map, Value, json};

use crate::{
    backup::validate_manifest,
    catalog,
    clickhouse::{ClickHouse, RestoreState},
    config::ControllerConfig,
    connect::Connect,
    model::{BackupManifest, KafkaOffset, KafkaOffsetValue, KafkaPartition, KeeperRow},
    recovery_resource::{RecoveryOffset, RecoveryPlan, TableRecovery, TableRecoveryStatus},
    replay::{KafkaReplay, kafka_identity},
};

struct Context {
    client: Client,
    config: ControllerConfig,
    connect: Connect,
    clickhouse: ClickHouse,
    replay: KafkaReplay,
    kafka_properties: std::collections::HashMap<String, String>,
}

#[derive(Debug)]
struct ReconcileError(anyhow::Error);

impl std::fmt::Display for ReconcileError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for ReconcileError {}

pub async fn run(path: &Path) -> Result<()> {
    let config = ControllerConfig::from_file(path)?;
    let kafka_properties = config.kafka_properties()?;
    let client = Client::try_default().await?;
    let connect = Connect::new(config.connect_url.clone(), &config.timeouts)?;
    let clickhouse = ClickHouse::new(
        config.clickhouse_url.clone(),
        config.clickhouse_username.clone(),
        config.clickhouse_password.clone(),
        &config.timeouts,
    )?;
    let replay = KafkaReplay::new(
        config.kafka_bootstrap_servers.clone(),
        kafka_properties.clone(),
        config.replay_topic_replication_factor,
        config.replay_topic_retention_ms,
        config.replay_batch_records,
        &config.timeouts,
    );
    let resources = Api::<TableRecovery>::namespaced(client.clone(), &config.namespace);
    let context = Arc::new(Context {
        client,
        config,
        connect,
        clickhouse,
        replay,
        kafka_properties,
    });
    Controller::new(resources, watcher::Config::default())
        .run(reconcile, error_policy, context)
        .for_each(|result| async move {
            if let Err(error) = result {
                eprintln!("TableRecovery reconciliation failed: {error}");
            }
        })
        .await;
    Ok(())
}

async fn reconcile(
    resource: Arc<TableRecovery>,
    context: Arc<Context>,
) -> std::result::Result<Action, ReconcileError> {
    match reconcile_recovery(&resource, &context).await {
        Ok(action) => Ok(action),
        Err(error) => {
            patch_failure(&resource, &context, &error)
                .await
                .map_err(ReconcileError)?;
            Err(ReconcileError(error))
        }
    }
}

async fn reconcile_recovery(resource: &TableRecovery, context: &Context) -> Result<Action> {
    let phase = resource
        .status
        .as_ref()
        .and_then(|status| status.phase.as_deref());
    if matches!(phase, Some("Complete" | "Streaming")) {
        return Ok(Action::await_change());
    }
    if resource.spec.source.database == resource.spec.destination.database
        && resource.spec.source.table == resource.spec.destination.table
    {
        bail!("source and destination tables must differ")
    }
    let uid = resource
        .uid()
        .context("TableRecovery has no Kubernetes UID")?;
    let manifest = catalog::lookup(
        &context.config.kafka_bootstrap_servers,
        &context.kafka_properties,
        &context.config.catalog_topic,
        &resource.spec.source.recovery_point_id,
        &uid,
        &context.config.timeouts,
    )
    .await?
    .context("recovery point was not found in the Kafka catalog")?;
    let point: BackupManifest = serde_json::from_str(&manifest)?;
    validate_manifest(&point)?;
    let plan = RecoveryPlan::new(&resource.spec, &point)?;
    if let Some(targets) = &plan.target_offsets {
        context.replay.verify_ranges(&plan.start_offsets, targets)?;
    } else {
        context.replay.verify_starts(&plan.start_offsets)?;
    }

    let rows = context
        .clickhouse
        .destination_rows(
            &resource.spec.destination.database,
            &resource.spec.destination.table,
        )
        .await?;
    if phase.is_none() {
        if rows != 0 {
            bail!("a new recovery requires an empty destination table")
        }
        update_phase(resource, context, "Restoring", &plan, vec![], vec![]).await?;
    }
    if phase != Some("Replaying") {
        match context
            .clickhouse
            .restore_state(&uid, &point.backup.name)
            .await?
        {
            RestoreState::Restored => {}
            RestoreState::Running => {
                return Ok(Action::requeue(context.config.timeouts.controller_retry));
            }
            RestoreState::Missing if rows == 0 => {
                context
                    .clickhouse
                    .restore_table(
                        &resource.spec.source.database,
                        &resource.spec.source.table,
                        &resource.spec.destination.database,
                        &resource.spec.destination.table,
                        &point.backup.name,
                        &uid,
                    )
                    .await?;
            }
            RestoreState::Missing => {
                bail!(
                    "recovery destination is non-empty but ClickHouse cannot confirm restore completion; use a new empty destination"
                )
            }
        }
    }
    update_phase(resource, context, "Replaying", &plan, vec![], vec![]).await?;

    let mut replay_connectors = Vec::new();
    let mut follow_connectors = Vec::new();
    for (index, checkpoint) in plan.connectors.iter().enumerate() {
        let source_config = context.connect.config(&checkpoint.name).await?;
        if let Some(targets) = &plan.target_offsets {
            let starts = topic_offsets(&plan.start_offsets, &checkpoint.topic);
            let targets = topic_offsets(targets, &checkpoint.topic);
            if starts != targets {
                let replay_topic = replay_topic(&uid, index)?;
                let replay_offsets = context
                    .replay
                    .copy(
                        &uid,
                        &index.to_string(),
                        &checkpoint.topic,
                        &replay_topic,
                        &starts,
                        &targets,
                    )
                    .await?;
                let connector = connector_name(&uid, "replay", index);
                run_replay_connector(
                    context,
                    resource,
                    &source_config,
                    &connector,
                    &replay_topic,
                    index,
                    &replay_offsets,
                )
                .await?;
                replay_connectors.push(connector);
            }
            let connector = connector_name(&uid, "follow", index);
            ensure_follow_connector(
                context,
                resource,
                &source_config,
                &connector,
                checkpoint,
                &targets,
                index,
                false,
            )
            .await?;
            follow_connectors.push(connector);
        } else {
            let connector = connector_name(&uid, "follow", index);
            let starts = topic_offsets(&plan.start_offsets, &checkpoint.topic);
            ensure_follow_connector(
                context,
                resource,
                &source_config,
                &connector,
                checkpoint,
                &starts,
                index,
                true,
            )
            .await?;
            follow_connectors.push(connector);
        }
    }
    let phase = if plan.target_offsets.is_some() {
        "Complete"
    } else {
        "Streaming"
    };
    update_phase(
        resource,
        context,
        phase,
        &plan,
        replay_connectors,
        follow_connectors,
    )
    .await?;
    Ok(Action::await_change())
}

async fn run_replay_connector(
    context: &Context,
    resource: &TableRecovery,
    source_config: &Map<String, Value>,
    connector: &str,
    topic: &str,
    index: usize,
    end_offsets: &[RecoveryOffset],
) -> Result<()> {
    context.replay.verify_replay_retained(end_offsets)?;
    let state_table = state_table(resource, "replay", index)?;
    let keeper_path = keeper_path(resource, "replay", index)?;
    context
        .clickhouse
        .ensure_keeper_table(
            &resource.spec.destination.database,
            &state_table,
            &keeper_path,
        )
        .await?;
    let config = connector_config(
        context,
        resource,
        source_config,
        topic,
        &state_table,
        &keeper_path,
    );
    let created = context.connect.create_stopped(connector, &config).await?;
    let expected = kafka_offsets(end_offsets);
    if created {
        let beginning = end_offsets
            .iter()
            .map(|offset| RecoveryOffset {
                topic: offset.topic.clone(),
                partition: offset.partition,
                offset: 0,
            })
            .collect::<Vec<_>>();
        context
            .connect
            .patch_offsets(connector, &kafka_offsets(&beginning))
            .await?;
    }
    if !equal_offsets(&context.connect.offsets(connector).await?, &expected) {
        context.connect.resume(connector).await?;
        context
            .connect
            .wait_running(connector, context.config.timeouts.recovery_catchup)
            .await?;
        context
            .connect
            .wait_offsets(
                connector,
                &expected,
                context.config.timeouts.recovery_catchup,
            )
            .await?;
    }
    context.connect.stop(connector).await?;
    context
        .connect
        .require_stopped(connector, context.config.timeouts.recovery_catchup)
        .await?;
    context.replay.verify_replay_retained(end_offsets)
}

#[allow(clippy::too_many_arguments)]
async fn ensure_follow_connector(
    context: &Context,
    resource: &TableRecovery,
    source_config: &Map<String, Value>,
    connector: &str,
    checkpoint: &crate::model::ConnectorCheckpoint,
    offsets: &[RecoveryOffset],
    index: usize,
    resume: bool,
) -> Result<()> {
    let state_table = state_table(resource, "follow", index)?;
    let keeper_path = keeper_path(resource, "follow", index)?;
    let checkpoint_offsets = checkpoint
        .offsets
        .iter()
        .map(|offset| RecoveryOffset {
            topic: offset.partition.kafka_topic.clone(),
            partition: offset.partition.kafka_partition,
            offset: offset.offset.kafka_offset,
        })
        .collect::<Vec<_>>();
    let rows = if offsets == checkpoint_offsets {
        checkpoint.keeper.rows.clone()
    } else {
        keeper_rows(offsets)
    };
    context
        .clickhouse
        .ensure_keeper_table(
            &resource.spec.destination.database,
            &state_table,
            &keeper_path,
        )
        .await?;
    let existing_rows = context
        .clickhouse
        .keeper_rows(&resource.spec.destination.database, &state_table)
        .await?;
    let config = connector_config(
        context,
        resource,
        source_config,
        &checkpoint.topic,
        &state_table,
        &keeper_path,
    );
    let created = context.connect.create_stopped(connector, &config).await?;
    let offsets = kafka_offsets(offsets);
    if created || existing_rows.is_empty() {
        context.connect.stop(connector).await?;
        context
            .connect
            .require_stopped(connector, context.config.timeouts.recovery_catchup)
            .await?;
        context.connect.patch_offsets(connector, &offsets).await?;
        ensure_keeper_rows(
            &context.clickhouse,
            &resource.spec.destination.database,
            &state_table,
            &keeper_path,
            &rows,
        )
        .await?;
    } else if resume {
        validate_keeper_progress(&existing_rows, &rows, &offsets)?;
    } else {
        ensure_keeper_rows(
            &context.clickhouse,
            &resource.spec.destination.database,
            &state_table,
            &keeper_path,
            &rows,
        )
        .await?;
        if !equal_offsets(&context.connect.offsets(connector).await?, &offsets) {
            bail!("bounded follow connector moved beyond its requested target")
        }
    }
    if resume {
        context.connect.resume(connector).await?;
        context
            .connect
            .wait_running(connector, context.config.timeouts.recovery_catchup)
            .await?;
    } else {
        context
            .connect
            .require_stopped(connector, context.config.timeouts.recovery_catchup)
            .await?;
    }
    Ok(())
}

fn validate_keeper_progress(
    actual: &[KeeperRow],
    initial: &[KeeperRow],
    offsets: &[KafkaOffset],
) -> Result<()> {
    let allowed = offsets
        .iter()
        .map(|offset| {
            format!(
                "{}-{}",
                offset.partition.kafka_topic, offset.partition.kafka_partition
            )
        })
        .collect::<std::collections::BTreeSet<_>>();
    if actual.iter().any(|row| !allowed.contains(&row.key)) {
        bail!("live recovery KeeperMap contains an unexpected partition")
    }
    for expected in initial {
        let current = actual
            .iter()
            .find(|row| row.key == expected.key)
            .context("live recovery KeeperMap lost a checkpoint row")?;
        if current.max_offset < expected.max_offset {
            bail!("live recovery KeeperMap moved behind its checkpoint")
        }
    }
    Ok(())
}

async fn ensure_keeper_rows(
    clickhouse: &ClickHouse,
    database: &str,
    table: &str,
    path: &str,
    expected: &[KeeperRow],
) -> Result<()> {
    clickhouse
        .ensure_keeper_table(database, table, path)
        .await?;
    let actual = clickhouse.keeper_rows(database, table).await?;
    if actual.iter().any(|row| !expected.contains(row)) {
        bail!("existing recovery KeeperMap contains conflicting rows")
    }
    let missing = expected
        .iter()
        .filter(|row| !actual.contains(row))
        .cloned()
        .collect::<Vec<_>>();
    clickhouse
        .insert_keeper_rows(database, table, &missing)
        .await?;
    let mut actual = clickhouse.keeper_rows(database, table).await?;
    let mut expected = expected.to_vec();
    actual.sort_by(|left, right| left.key.cmp(&right.key));
    expected.sort_by(|left, right| left.key.cmp(&right.key));
    if actual != expected {
        bail!("recovery KeeperMap did not match after rehydration")
    }
    Ok(())
}

fn connector_config(
    context: &Context,
    resource: &TableRecovery,
    source: &Map<String, Value>,
    topic: &str,
    state_table: &str,
    keeper_path: &str,
) -> Map<String, Value> {
    let mut config = source.clone();
    for key in ["name", "topics.regex", "consumer.override.group.id"] {
        config.remove(key);
    }
    let values = [
        ("topics", topic.to_owned()),
        (
            "topic2TableMap",
            format!("{}={}", topic, resource.spec.destination.table),
        ),
        ("hostname", context.config.clickhouse_connector_host.clone()),
        ("port", context.config.clickhouse_connector_port.to_string()),
        (
            "ssl",
            context.config.clickhouse_connector_secure.to_string(),
        ),
        ("database", resource.spec.destination.database.clone()),
        ("zkPath", keeper_path.to_owned()),
        ("zkDatabase", state_table.to_owned()),
        (
            "consumer.override.isolation.level",
            "read_committed".to_owned(),
        ),
        ("consumer.override.auto.offset.reset", "none".to_owned()),
    ];
    for (key, value) in values {
        config.insert(key.to_owned(), Value::String(value));
    }
    config
}

fn keeper_rows(offsets: &[RecoveryOffset]) -> Vec<KeeperRow> {
    offsets
        .iter()
        .filter(|offset| offset.offset > 0)
        .map(|offset| KeeperRow {
            key: format!("{}-{}", offset.topic, offset.partition),
            min_offset: offset.offset - 1,
            max_offset: offset.offset - 1,
            state: "AFTER_PROCESSING".to_owned(),
        })
        .collect()
}

fn kafka_offsets(offsets: &[RecoveryOffset]) -> Vec<KafkaOffset> {
    offsets
        .iter()
        .map(|offset| KafkaOffset {
            partition: KafkaPartition {
                kafka_topic: offset.topic.clone(),
                kafka_partition: offset.partition,
            },
            offset: KafkaOffsetValue {
                kafka_offset: offset.offset,
            },
        })
        .collect()
}

fn equal_offsets(left: &[KafkaOffset], right: &[KafkaOffset]) -> bool {
    let values = |offsets: &[KafkaOffset]| {
        offsets
            .iter()
            .map(|offset| {
                (
                    offset.partition.kafka_topic.clone(),
                    offset.partition.kafka_partition,
                    offset.offset.kafka_offset,
                )
            })
            .collect::<std::collections::BTreeSet<_>>()
    };
    values(left) == values(right)
}

fn topic_offsets(offsets: &[RecoveryOffset], topic: &str) -> Vec<RecoveryOffset> {
    offsets
        .iter()
        .filter(|offset| offset.topic == topic)
        .cloned()
        .collect()
}

fn resource_token(resource: &TableRecovery) -> Result<String> {
    Ok(resource
        .uid()
        .context("TableRecovery has no Kubernetes UID")?
        .replace('-', ""))
}

fn connector_name(uid: &str, role: &str, index: usize) -> String {
    format!("dcs-{}-{role}-{index}", uid.replace('-', ""))
}

fn replay_topic(uid: &str, index: usize) -> Result<String> {
    kafka_identity("durable-clickhouse-replay", uid, &index.to_string())
}

fn state_table(resource: &TableRecovery, role: &str, index: usize) -> Result<String> {
    Ok(format!(
        "dcs_recovery_{}_{role}_{index}_state",
        resource_token(resource)?
    ))
}

fn keeper_path(resource: &TableRecovery, role: &str, index: usize) -> Result<String> {
    Ok(format!(
        "/durable-clickhouse-sink/recovery/{}/{role}/{index}",
        resource_token(resource)?
    ))
}

async fn update_phase(
    resource: &TableRecovery,
    context: &Context,
    phase: &str,
    plan: &RecoveryPlan,
    replay_connectors: Vec<String>,
    follow_connectors: Vec<String>,
) -> Result<()> {
    patch_status(
        resource,
        context,
        TableRecoveryStatus {
            observed_generation: resource.metadata.generation,
            phase: Some(phase.to_owned()),
            resolved_start_offsets: Some(plan.start_offsets.clone()),
            resolved_target_offsets: plan.target_offsets.clone(),
            replay_connectors: Some(replay_connectors),
            follow_connectors: Some(follow_connectors),
            conditions: vec![condition(
                "Ready",
                if matches!(phase, "Complete" | "Streaming") {
                    "True"
                } else {
                    "False"
                },
                phase,
                &format!("TableRecovery is {phase}"),
            )],
        },
    )
    .await
}

async fn patch_status(
    resource: &TableRecovery,
    context: &Context,
    status: TableRecoveryStatus,
) -> Result<()> {
    let resources =
        Api::<TableRecovery>::namespaced(context.client.clone(), &context.config.namespace);
    resources
        .patch_status(
            &resource.name_any(),
            &PatchParams::default(),
            &Patch::Merge(json!({"status": status})),
        )
        .await?;
    Ok(())
}

async fn patch_failure(
    resource: &TableRecovery,
    context: &Context,
    error: &anyhow::Error,
) -> Result<()> {
    let resources =
        Api::<TableRecovery>::namespaced(context.client.clone(), &context.config.namespace);
    resources
        .patch_status(
            &resource.name_any(),
            &PatchParams::default(),
            &Patch::Merge(json!({
                "status": {
                    "observedGeneration": resource.metadata.generation,
                    "conditions": [condition("Ready", "False", "ReconcileFailed", error)]
                }
            })),
        )
        .await?;
    Ok(())
}

fn condition(
    condition_type: &str,
    status: &str,
    reason: &str,
    message: &impl std::fmt::Display,
) -> crate::recovery_resource::RecoveryCondition {
    crate::recovery_resource::RecoveryCondition {
        condition_type: condition_type.to_owned(),
        status: status.to_owned(),
        reason: reason.to_owned(),
        message: message.to_string(),
        last_transition_time: Utc::now().to_rfc3339(),
    }
}

fn error_policy(
    _resource: Arc<TableRecovery>,
    _error: &ReconcileError,
    context: Arc<Context>,
) -> Action {
    Action::requeue(context.config.timeouts.controller_retry)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeper_rows_encode_exclusive_offsets() {
        assert_eq!(
            keeper_rows(&[
                RecoveryOffset {
                    topic: "events".to_owned(),
                    partition: 0,
                    offset: 0,
                },
                RecoveryOffset {
                    topic: "events".to_owned(),
                    partition: 1,
                    offset: 42,
                },
            ]),
            vec![KeeperRow {
                key: "events-1".to_owned(),
                min_offset: 41,
                max_offset: 41,
                state: "AFTER_PROCESSING".to_owned(),
            }]
        );
    }

    #[test]
    fn recovery_names_are_stable_and_safe() {
        let uid = "12345678-1234-1234-1234-123456789abc";
        assert_eq!(
            connector_name(uid, "follow", 2),
            "dcs-12345678123412341234123456789abc-follow-2"
        );
        assert!(replay_topic(uid, 2).unwrap().len() <= 249);
    }

    #[test]
    fn live_keeper_progress_may_advance_but_not_rewind() {
        let offsets = kafka_offsets(&[RecoveryOffset {
            topic: "events".to_owned(),
            partition: 0,
            offset: 42,
        }]);
        let initial = keeper_rows(&[RecoveryOffset {
            topic: "events".to_owned(),
            partition: 0,
            offset: 42,
        }]);
        let mut advanced = initial.clone();
        advanced[0].max_offset = 50;
        assert!(validate_keeper_progress(&advanced, &initial, &offsets).is_ok());
        advanced[0].max_offset = 40;
        assert!(validate_keeper_progress(&advanced, &initial, &offsets).is_err());
        advanced[0].key = "other-0".to_owned();
        assert!(validate_keeper_progress(&advanced, &initial, &offsets).is_err());
    }
}
