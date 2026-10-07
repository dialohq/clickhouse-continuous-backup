//! Request and response bodies of the pause server, shared by the server and `PauseClient`.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::model::{KafkaOffset, KeeperRow, Pipeline};

/// Selects pipelines by exactly one key, e.g. `{"topic": "records.input"}`.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum PauseRequest {
    Topic(String),
    Connector(String),
    Table(String),
}

impl PauseRequest {
    pub(crate) fn matches(&self, pipeline: &Pipeline) -> bool {
        match self {
            Self::Topic(topic) => pipeline.topic == *topic,
            Self::Connector(connector) => pipeline.connector == *connector,
            Self::Table(table) => pipeline.table == *table,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct PauseResponse {
    pub(crate) token: Uuid,
    pub(crate) watermark: Vec<PipelineOffsets>,
}

/// Exact offsets of one pipeline, with the Connect offsets and KeeperMap rows they came from.
#[derive(Serialize, Deserialize)]
pub(crate) struct PipelineOffsets {
    pub(crate) connector: String,
    pub(crate) offsets: Vec<KafkaOffset>,
    pub(crate) connect_offsets: Vec<KafkaOffset>,
    pub(crate) keeper_rows: Vec<KeeperRow>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResumeRequest {
    pub(crate) token: Uuid,
}
