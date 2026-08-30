use anyhow::{Result, bail};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::model::{BackupManifest, ConnectorCheckpoint, KafkaOffset, clickhouse_identifier};

#[derive(CustomResource, Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "chbackup.dialo.ai",
    version = "v1alpha1",
    kind = "TableRecovery",
    plural = "tablerecoveries",
    shortname = "chr",
    namespaced,
    status = "TableRecoveryStatus"
)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TableRecoverySpec {
    pub source: RecoverySource,
    pub destination: RecoveryDestination,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_offsets: Option<Vec<RecoveryOffset>>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecoverySource {
    pub database: String,
    pub table: String,
    #[serde(rename = "backupID")]
    pub backup_id: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecoveryDestination {
    pub database: String,
    pub table: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecoveryOffset {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TableRecoveryStatus {
    pub observed_generation: Option<i64>,
    pub phase: Option<String>,
    pub resolved_start_offsets: Option<Vec<RecoveryOffset>>,
    pub resolved_target_offsets: Option<Vec<RecoveryOffset>>,
    pub replay_connectors: Option<Vec<String>>,
    pub follow_connectors: Option<Vec<String>>,
    #[serde(default)]
    pub conditions: Vec<RecoveryCondition>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryPlan {
    pub connectors: Vec<ConnectorCheckpoint>,
    pub start_offsets: Vec<RecoveryOffset>,
    pub target_offsets: Option<Vec<RecoveryOffset>>,
}

impl RecoveryPlan {
    pub fn new(spec: &TableRecoverySpec, point: &BackupManifest) -> Result<Self> {
        for (name, value) in [
            ("source database", &spec.source.database),
            ("source table", &spec.source.table),
            ("destination database", &spec.destination.database),
            ("destination table", &spec.destination.table),
        ] {
            if !clickhouse_identifier(value) {
                bail!("{name} must be a ClickHouse identifier")
            }
        }
        if point.backup.id.to_string() != spec.source.backup_id {
            bail!("backup ID does not match the requested source")
        }
        let connectors = point
            .connectors
            .iter()
            .filter(|checkpoint| {
                checkpoint.database == spec.source.database && checkpoint.table == spec.source.table
            })
            .cloned()
            .collect::<Vec<_>>();
        if connectors.is_empty() {
            bail!("recovery point does not contain the requested source table")
        }
        let start_offsets = flatten_offsets(&connectors)?;
        let target_offsets = spec
            .target_offsets
            .as_ref()
            .map(|targets| validate_targets(&start_offsets, targets))
            .transpose()?;
        Ok(Self {
            connectors,
            start_offsets,
            target_offsets,
        })
    }
}

fn flatten_offsets(checkpoints: &[ConnectorCheckpoint]) -> Result<Vec<RecoveryOffset>> {
    let mut offsets = Vec::new();
    for checkpoint in checkpoints {
        for offset in &checkpoint.offsets {
            if offset.partition.kafka_topic != checkpoint.topic {
                bail!("connector checkpoint contains an unexpected topic")
            }
            offsets.push(recovery_offset(offset));
        }
    }
    sorted_unique(offsets, "recovery point")
}

fn validate_targets(
    starts: &[RecoveryOffset],
    targets: &[RecoveryOffset],
) -> Result<Vec<RecoveryOffset>> {
    let targets = sorted_unique(targets.to_vec(), "targetOffsets")?;
    if !starts
        .iter()
        .map(|offset| (&offset.topic, offset.partition))
        .eq(targets
            .iter()
            .map(|offset| (&offset.topic, offset.partition)))
    {
        bail!("targetOffsets must contain every source topic-partition exactly once")
    }
    for (start, target) in starts.iter().zip(&targets) {
        if target.offset < start.offset {
            bail!(
                "target offset is before the recovery point for {}-{}",
                start.topic,
                start.partition
            )
        }
    }
    Ok(targets)
}

fn sorted_unique(mut offsets: Vec<RecoveryOffset>, source: &str) -> Result<Vec<RecoveryOffset>> {
    offsets
        .sort_by(|left, right| (&left.topic, left.partition).cmp(&(&right.topic, right.partition)));
    if offsets
        .windows(2)
        .any(|pair| pair[0].topic == pair[1].topic && pair[0].partition == pair[1].partition)
    {
        bail!("{source} contains a duplicate topic-partition")
    }
    Ok(offsets)
}

fn recovery_offset(offset: &KafkaOffset) -> RecoveryOffset {
    RecoveryOffset {
        topic: offset.partition.kafka_topic.clone(),
        partition: offset.partition.kafka_partition,
        offset: offset.offset.kafka_offset,
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecoveryCondition {
    #[serde(rename = "type")]
    pub condition_type: String,
    pub status: String,
    pub reason: String,
    pub message: String,
    pub last_transition_time: String,
}

pub fn crd_yaml() -> anyhow::Result<String> {
    use kube::CustomResourceExt;

    let mut crd = serde_json::to_value(TableRecovery::crd())?;
    add_immutability_rule(&mut crd)?;
    Ok(serde_yaml::to_string(&crd)?)
}

fn add_immutability_rule(crd: &mut serde_json::Value) -> anyhow::Result<()> {
    let schema = crd
        .pointer_mut("/spec/versions/0/schema/openAPIV3Schema/properties/spec")
        .ok_or_else(|| anyhow::anyhow!("generated TableRecovery CRD has no spec schema"))?;
    schema["x-kubernetes-validations"] = serde_json::json!([{
        "rule": "self == oldSelf",
        "message": "TableRecovery spec is immutable"
    }]);
    Ok(())
}

#[cfg(test)]
mod tests {
    use kube::CustomResourceExt;
    use uuid::Uuid;

    use super::*;
    use crate::model::{BackupKind, BackupManifest, BackupReference, KeeperCheckpoint};

    fn checkpoint(name: &str, topic: &str, database: &str, table: &str) -> ConnectorCheckpoint {
        ConnectorCheckpoint {
            name: name.to_owned(),
            database: database.to_owned(),
            table: table.to_owned(),
            topic: topic.to_owned(),
            partitions: 2,
            offsets: [10, 20]
                .into_iter()
                .enumerate()
                .map(|(partition, offset)| KafkaOffset {
                    partition: crate::model::KafkaPartition {
                        kafka_topic: topic.to_owned(),
                        kafka_partition: partition as u32,
                    },
                    offset: crate::model::KafkaOffsetValue {
                        kafka_offset: offset,
                    },
                })
                .collect(),
            observed_connect_offsets: vec![],
            keeper: KeeperCheckpoint {
                database: database.to_owned(),
                table: format!("{name}_state"),
                path: format!("/test/{name}"),
                rows: vec![],
            },
        }
    }

    fn point(id: Uuid) -> BackupManifest {
        BackupManifest {
            created_at: "2026-08-30T00:00:00Z".to_owned(),
            backup: BackupReference {
                id,
                name: "S3(named, 'backup')".to_owned(),
                kind: BackupKind::Full,
                chain_id: "chain".to_owned(),
                position: 0,
                base: None,
            },
            connectors: vec![
                checkpoint("one", "one.input", "events", "records"),
                checkpoint("two", "two.input", "events", "records"),
                checkpoint("other", "other.input", "events", "other"),
            ],
        }
    }

    fn spec(id: Uuid, targets: Option<Vec<RecoveryOffset>>) -> TableRecoverySpec {
        TableRecoverySpec {
            source: RecoverySource {
                database: "events".to_owned(),
                table: "records".to_owned(),
                backup_id: id.to_string(),
            },
            destination: RecoveryDestination {
                database: "recovery".to_owned(),
                table: "records".to_owned(),
            },
            target_offsets: targets,
        }
    }

    #[test]
    fn crd_requires_an_immutable_spec() {
        let mut crd = serde_json::to_value(TableRecovery::crd()).unwrap();
        add_immutability_rule(&mut crd).unwrap();
        assert_eq!(crd["spec"]["group"], "chbackup.dialo.ai");
        let rules = crd
            .pointer(
                "/spec/versions/0/schema/openAPIV3Schema/properties/spec/x-kubernetes-validations",
            )
            .unwrap();
        assert_eq!(rules[0]["rule"], "self == oldSelf");
        assert!(
            crd.pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/source/properties/backupID")
                .is_some()
        );
    }

    #[test]
    fn omitted_targets_mean_unbounded_follow() {
        let id = Uuid::new_v4();
        let plan = RecoveryPlan::new(&spec(id, None), &point(id)).unwrap();
        assert_eq!(plan.connectors.len(), 2);
        assert_eq!(plan.start_offsets.len(), 4);
        assert_eq!(plan.target_offsets, None);
    }

    #[test]
    fn explicit_targets_are_complete_exclusive_offsets() {
        let id = Uuid::new_v4();
        let targets = vec![
            RecoveryOffset {
                topic: "two.input".to_owned(),
                partition: 1,
                offset: 25,
            },
            RecoveryOffset {
                topic: "one.input".to_owned(),
                partition: 0,
                offset: 15,
            },
            RecoveryOffset {
                topic: "two.input".to_owned(),
                partition: 0,
                offset: 10,
            },
            RecoveryOffset {
                topic: "one.input".to_owned(),
                partition: 1,
                offset: 20,
            },
        ];
        let plan = RecoveryPlan::new(&spec(id, Some(targets)), &point(id)).unwrap();
        let targets = plan.target_offsets.unwrap();
        assert_eq!(targets[0].topic, "one.input");
        assert_eq!(targets[0].offset, 15);
    }

    #[test]
    fn rejects_partial_duplicate_early_and_unrelated_targets() {
        let id = Uuid::new_v4();
        let base = vec![
            RecoveryOffset {
                topic: "one.input".to_owned(),
                partition: 0,
                offset: 10,
            },
            RecoveryOffset {
                topic: "one.input".to_owned(),
                partition: 1,
                offset: 20,
            },
            RecoveryOffset {
                topic: "two.input".to_owned(),
                partition: 0,
                offset: 10,
            },
            RecoveryOffset {
                topic: "two.input".to_owned(),
                partition: 1,
                offset: 20,
            },
        ];
        assert!(RecoveryPlan::new(&spec(id, Some(base[..3].to_vec())), &point(id)).is_err());
        let mut duplicate = base.clone();
        duplicate.push(base[0].clone());
        assert!(RecoveryPlan::new(&spec(id, Some(duplicate)), &point(id)).is_err());
        let mut early = base.clone();
        early[1].offset = 19;
        assert!(RecoveryPlan::new(&spec(id, Some(early)), &point(id)).is_err());
        let mut unrelated = base;
        unrelated[0].topic = "unknown".to_owned();
        assert!(RecoveryPlan::new(&spec(id, Some(unrelated)), &point(id)).is_err());
    }

    #[test]
    fn rejects_unsafe_clickhouse_identifiers() {
        let id = Uuid::new_v4();
        let mut requested = spec(id, None);
        requested.destination.table = "records` SETTINGS allow_s3_native_copy=1".to_owned();
        assert!(RecoveryPlan::new(&requested, &point(id)).is_err());
    }
}
