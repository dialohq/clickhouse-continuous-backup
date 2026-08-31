use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
        if self.partitions == 0 {
            bail!("pipeline partition count must be positive")
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
            if !clickhouse_identifier(value) {
                bail!("pipeline {name} must be a ClickHouse identifier")
            }
        }
        if !safe_keeper_path(&self.keeper_path) {
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

pub fn clickhouse_identifier(value: &str) -> bool {
    let mut characters = value.chars();
    characters
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
        && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

pub fn safe_chain_id(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '-')
}

pub fn safe_keeper_path(value: &str) -> bool {
    value.starts_with('/')
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "_/-".contains(character))
        && safe_path_segments(value.split('/').skip(1))
}

pub fn safe_storage_path(value: &str) -> bool {
    !value.starts_with('/')
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "_./-".contains(character))
        && safe_path_segments(value.split('/'))
}

fn safe_path_segments<'a>(mut segments: impl Iterator<Item = &'a str>) -> bool {
    segments.all(|segment| !segment.is_empty() && segment != "." && segment != "..")
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
    pub database: String,
    pub table: String,
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
    pub parent: Option<BackupParent>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BackupParent {
    pub id: Uuid,
    pub name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BackupManifest {
    pub created_at: String,
    pub backup: BackupReference,
    pub connectors: Vec<ConnectorCheckpoint>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BackupChainState {
    pub generation: u64,
    pub chain_id: String,
    pub root: BackupReference,
    pub tip: BackupReference,
    pub incremental_count: u32,
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
    pub manifest: &'a BackupManifest,
}
