use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const RECOVERY_POINT_FORMAT: &str = "durable-clickhouse-sink/recovery-point-v3";
pub const CHAIN_HEAD_FORMAT: &str = "durable-clickhouse-sink/backup-chain-v3";
pub const CHAIN_HEAD_KEY: &str = "__durable_clickhouse_sink_chain_head_v3";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Pipeline {
    pub connector: String,
    pub database: String,
    pub state_table: String,
    pub keeper_path: String,
    pub table: String,
    pub topic: String,
    pub partitions: u32,
}

impl Pipeline {
    pub fn validate(&self) -> Result<()> {
        if self.connector.is_empty() || self.topic.is_empty() || self.partitions == 0 {
            bail!("pipeline connector, topic, and positive partition count are required")
        }
        for (name, value) in [("connector", &self.connector), ("topic", &self.topic)] {
            if !kafka_name(value) {
                bail!("pipeline {name} must be a Kafka-safe name")
            }
        }
        for (name, value) in [
            ("database", &self.database),
            ("state_table", &self.state_table),
            ("table", &self.table),
        ] {
            let mut characters = value.chars();
            if !characters
                .next()
                .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
                || !characters
                    .all(|character| character.is_ascii_alphanumeric() || character == '_')
            {
                bail!("pipeline {name} must be a ClickHouse identifier")
            }
        }
        if !self.keeper_path.starts_with('/')
            || !self
                .keeper_path
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || "_/-".contains(character))
            || self
                .keeper_path
                .split('/')
                .skip(1)
                .any(|segment| segment.is_empty() || segment == "." || segment == "..")
        {
            bail!("pipeline keeper_path must be an absolute safe Keeper path")
        }
        Ok(())
    }
}

pub fn kafka_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 249
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "._-".contains(character))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct KafkaPartition {
    pub kafka_topic: String,
    pub kafka_partition: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct KafkaOffsetValue {
    pub kafka_offset: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct KafkaOffset {
    pub partition: KafkaPartition,
    pub offset: KafkaOffsetValue,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectOffsets {
    pub offsets: Vec<KafkaOffset>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct KeeperRow {
    pub key: String,
    #[serde(rename = "minOffset")]
    pub min_offset: u64,
    #[serde(rename = "maxOffset")]
    pub max_offset: u64,
    pub state: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct KeeperCheckpoint {
    pub database: String,
    pub table: String,
    pub path: String,
    pub rows: Vec<KeeperRow>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConnectorCheckpoint {
    pub name: String,
    pub topic: String,
    pub partitions: u32,
    pub offsets: Vec<KafkaOffset>,
    pub observed_connect_offsets: Vec<KafkaOffset>,
    pub keeper: KeeperCheckpoint,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BackupKind {
    Full,
    Incremental,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BackupReference {
    pub id: Uuid,
    pub name: String,
    pub kind: BackupKind,
    pub chain_id: String,
    pub position: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base: Option<BackupDependency>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BackupDependency {
    pub id: Uuid,
    pub name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecoveryPoint {
    pub format: String,
    pub created_at: String,
    pub backup: BackupReference,
    pub connectors: Vec<ConnectorCheckpoint>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChainHead {
    pub format: String,
    pub generation: u64,
    pub chain_id: String,
    pub base: BackupReference,
    pub latest: BackupReference,
    pub incrementals: u32,
    pub pipelines: Vec<Pipeline>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BackupDetails {
    pub id: Uuid,
    pub name: String,
    pub status: String,
    pub num_files: u64,
    pub uncompressed_size: u64,
    pub compressed_size: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct BackupOutput<'a> {
    #[serde(flatten)]
    pub details: &'a BackupDetails,
    pub recovery_point: &'a RecoveryPoint,
}
