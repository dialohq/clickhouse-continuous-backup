use std::{collections::HashMap, env, fs, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};
use serde::{
    Deserialize, Deserializer,
    de::{DeserializeOwned, Error as _},
};

use crate::model::{Pipeline, kafka_name};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeTimeouts {
    #[serde(
        rename = "clickhouseConnectSeconds",
        deserialize_with = "positive_seconds"
    )]
    pub clickhouse_connect: Duration,
    #[serde(
        rename = "connectConnectSeconds",
        deserialize_with = "positive_seconds"
    )]
    pub connect_connect: Duration,
    #[serde(
        rename = "connectRequestSeconds",
        deserialize_with = "positive_seconds"
    )]
    pub connect_request: Duration,
    #[serde(rename = "connectPollSeconds", deserialize_with = "positive_seconds")]
    pub connect_poll: Duration,
    #[serde(rename = "kafkaMetadataSeconds", deserialize_with = "positive_seconds")]
    pub kafka_metadata: Duration,
    #[serde(
        rename = "kafkaCatalogAcquireSeconds",
        deserialize_with = "positive_seconds"
    )]
    pub kafka_catalog_acquire: Duration,
    #[serde(
        rename = "kafkaCatalogReadSeconds",
        deserialize_with = "positive_seconds"
    )]
    pub kafka_catalog_read: Duration,
    #[serde(
        rename = "kafkaTransactionSeconds",
        deserialize_with = "positive_seconds"
    )]
    pub kafka_transaction: Duration,
    #[serde(rename = "kafkaMaxPollSeconds", deserialize_with = "positive_seconds")]
    pub kafka_max_poll: Duration,
}

#[derive(Clone, Debug)]
pub struct BackupConfig {
    pub connect_url: String,
    pub clickhouse_url: String,
    pub clickhouse_username: String,
    pub clickhouse_password: String,
    pub backup_objects: String,
    pub backup_state_objects: String,
    pub named_collection: String,
    pub path_prefix: String,
    pub archive_extension: String,
    pub run_id: String,
    pub pause_timeout: Duration,
    pub kafka_bootstrap_servers: String,
    pub kafka_properties_file: Option<PathBuf>,
    pub recovery_topic: String,
    pub max_incrementals_per_full: u32,
    pub pipelines: Vec<Pipeline>,
    pub timeouts: RuntimeTimeouts,
}

#[derive(Clone, Debug)]
pub struct RestoreConfig {
    pub connect_url: String,
    pub clickhouse_url: String,
    pub clickhouse_username: String,
    pub clickhouse_password: String,
    pub connector_names: Vec<String>,
    pub expected_backup_name: String,
    pub manifest_file: String,
    pub stop_timeout: Duration,
    pub kafka_bootstrap_servers: String,
    pub kafka_properties_file: Option<PathBuf>,
    pub timeouts: RuntimeTimeouts,
}

#[derive(Clone, Debug)]
pub struct TargetConfig {
    pub clickhouse_url: String,
    pub clickhouse_username: String,
    pub clickhouse_password: String,
    pub pipelines: Vec<Pipeline>,
    pub timeouts: RuntimeTimeouts,
}

impl RuntimeTimeouts {
    fn from_environment() -> Result<Self> {
        json("RUNTIME_TIMEOUTS")
    }
}

fn positive_seconds<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Duration, D::Error> {
    let seconds = u64::deserialize(deserializer)?;
    if seconds == 0 {
        return Err(D::Error::custom("timeout must be a positive integer"));
    }
    Ok(Duration::from_secs(seconds))
}

impl BackupConfig {
    pub fn from_environment() -> Result<Self> {
        let run_id = required("BACKUP_RUN_ID")?;
        validate_token("BACKUP_RUN_ID", &run_id, |character| {
            character.is_ascii_alphanumeric() || character == '-'
        })?;
        let named_collection = identifier("BACKUP_NAMED_COLLECTION")?;
        let path_prefix = required("BACKUP_PATH_PREFIX")?;
        validate_token("BACKUP_PATH_PREFIX", &path_prefix, |character| {
            character.is_ascii_alphanumeric() || "_./-".contains(character)
        })?;
        if !storage_path(&path_prefix) {
            bail!(
                "BACKUP_PATH_PREFIX must be a relative object path without empty, . or .. segments"
            )
        }
        let archive_extension = required("BACKUP_ARCHIVE_EXTENSION")?;
        if !["tar.zst", "tar.gz", "tar.xz", "tar.bz2", "tgz", "tzst"]
            .contains(&archive_extension.as_str())
        {
            bail!("unsupported BACKUP_ARCHIVE_EXTENSION")
        }
        let pipelines: Vec<Pipeline> = json("BACKUP_PIPELINES")?;
        if pipelines.is_empty() {
            bail!("BACKUP_PIPELINES must contain at least one pipeline")
        }
        for pipeline in &pipelines {
            pipeline.validate()?;
        }
        Ok(Self {
            connect_url: required("CONNECT_URL")?,
            clickhouse_url: required("CLICKHOUSE_URL")?,
            clickhouse_username: required("CLICKHOUSE_USERNAME")?,
            clickhouse_password: env::var("CLICKHOUSE_PASSWORD").unwrap_or_default(),
            backup_objects: required("BACKUP_OBJECTS")?,
            backup_state_objects: required("BACKUP_STATE_OBJECTS")?,
            named_collection,
            path_prefix,
            archive_extension,
            run_id,
            pause_timeout: seconds("PAUSE_TIMEOUT_SECONDS")?,
            kafka_bootstrap_servers: required("KAFKA_BOOTSTRAP_SERVERS")?,
            kafka_properties_file: optional("KAFKA_PROPERTIES_FILE").map(PathBuf::from),
            recovery_topic: required("KAFKA_RECOVERY_TOPIC")?,
            max_incrementals_per_full: unsigned("MAX_INCREMENTALS_PER_FULL")?,
            pipelines,
            timeouts: RuntimeTimeouts::from_environment()?,
        })
    }

    pub fn kafka_properties(&self) -> Result<HashMap<String, String>> {
        read_kafka_properties(self.kafka_properties_file.as_ref())
    }
}

impl RestoreConfig {
    pub fn from_environment() -> Result<Self> {
        let connector_names = required("CONNECTOR_NAMES")?
            .lines()
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if connector_names.is_empty() {
            bail!("CONNECTOR_NAMES must contain at least one connector")
        }
        if connector_names.iter().any(|name| !kafka_name(name)) {
            bail!("CONNECTOR_NAMES contains an unsafe connector name")
        }
        Ok(Self {
            connect_url: required("CONNECT_URL")?,
            clickhouse_url: required("CLICKHOUSE_URL")?,
            clickhouse_username: required("CLICKHOUSE_USERNAME")?,
            clickhouse_password: env::var("CLICKHOUSE_PASSWORD").unwrap_or_default(),
            connector_names,
            expected_backup_name: required("EXPECTED_BACKUP_NAME")?,
            manifest_file: required("RECOVERY_MANIFEST_FILE")?,
            stop_timeout: seconds("STOP_TIMEOUT_SECONDS")?,
            kafka_bootstrap_servers: required("KAFKA_BOOTSTRAP_SERVERS")?,
            kafka_properties_file: optional("KAFKA_PROPERTIES_FILE").map(PathBuf::from),
            timeouts: RuntimeTimeouts::from_environment()?,
        })
    }

    pub fn kafka_properties(&self) -> Result<HashMap<String, String>> {
        read_kafka_properties(self.kafka_properties_file.as_ref())
    }
}

impl TargetConfig {
    pub fn from_environment() -> Result<Self> {
        let pipelines: Vec<Pipeline> = json("BACKUP_PIPELINES")?;
        if pipelines.is_empty() {
            bail!("BACKUP_PIPELINES must contain at least one pipeline")
        }
        for pipeline in &pipelines {
            pipeline.validate()?;
        }
        let credentials = optional("CLICKHOUSE_PROPERTIES_FILE")
            .map(|path| read_properties(&PathBuf::from(path), "ClickHouse"))
            .transpose()?;
        let clickhouse_username = credentials
            .as_ref()
            .and_then(|properties| properties.get("username").cloned())
            .map(Ok)
            .unwrap_or_else(|| required("CLICKHOUSE_USERNAME"))?;
        let clickhouse_password = credentials
            .as_ref()
            .and_then(|properties| properties.get("password").cloned())
            .unwrap_or_else(|| env::var("CLICKHOUSE_PASSWORD").unwrap_or_default());
        Ok(Self {
            clickhouse_url: required("CLICKHOUSE_URL")?,
            clickhouse_username,
            clickhouse_password,
            pipelines,
            timeouts: RuntimeTimeouts::from_environment()?,
        })
    }
}

fn read_kafka_properties(path: Option<&PathBuf>) -> Result<HashMap<String, String>> {
    let Some(path) = path else {
        return Ok(HashMap::new());
    };
    read_properties(path, "Kafka")
}

fn read_properties(path: &PathBuf, kind: &str) -> Result<HashMap<String, String>> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read {kind} properties from {}", path.display()))?;
    let properties = contents
        .lines()
        .filter(|line| !line.trim().is_empty() && !line.trim_start().starts_with(['#', '!']))
        .map(parse_property)
        .collect::<Result<Vec<_>>>()?;
    let mut result = HashMap::new();
    for (key, value) in properties {
        if key.is_empty() {
            bail!("{kind} property name must not be empty")
        }
        if result.insert(key.clone(), value).is_some() {
            bail!("duplicate {kind} property: {key}")
        }
    }
    Ok(result)
}

fn required(name: &str) -> Result<String> {
    env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .with_context(|| format!("{name} is required"))
}

fn optional(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

fn seconds(name: &str) -> Result<Duration> {
    let value = required(name)?
        .parse::<u64>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    if value == 0 {
        bail!("{name} must be a positive integer")
    }
    Ok(Duration::from_secs(value))
}

fn unsigned(name: &str) -> Result<u32> {
    required(name)?
        .parse()
        .with_context(|| format!("{name} must be a non-negative integer"))
}

fn json<T: DeserializeOwned>(name: &str) -> Result<T> {
    serde_json::from_str(&required(name)?).with_context(|| format!("{name} must be valid JSON"))
}

fn identifier(name: &str) -> Result<String> {
    let value = required(name)?;
    let mut characters = value.chars();
    if !characters
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
        || !characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        bail!("{name} must be a ClickHouse identifier")
    }
    Ok(value)
}

fn validate_token(name: &str, value: &str, allowed: impl Fn(char) -> bool) -> Result<()> {
    if value.is_empty() || !value.chars().all(allowed) {
        bail!("{name} contains unsupported characters")
    }
    Ok(())
}

fn storage_path(value: &str) -> bool {
    !value.starts_with('/')
        && value
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
}

fn parse_property(line: &str) -> Result<(String, String)> {
    let (key, value) = line
        .split_once(['=', ':'])
        .with_context(|| format!("invalid property: {line}"))?;
    Ok((key.trim().to_owned(), value.trim().to_owned()))
}

#[cfg(test)]
mod tests {
    use super::RuntimeTimeouts;

    const VALID: &str = r#"{
        "clickhouseConnectSeconds": 10,
        "connectConnectSeconds": 5,
        "connectRequestSeconds": 15,
        "connectPollSeconds": 1,
        "kafkaMetadataSeconds": 15,
        "kafkaCatalogAcquireSeconds": 30,
        "kafkaCatalogReadSeconds": 15,
        "kafkaTransactionSeconds": 30,
        "kafkaMaxPollSeconds": 86400
    }"#;

    #[test]
    fn accepts_complete_positive_timeout_contract() {
        assert!(serde_json::from_str::<RuntimeTimeouts>(VALID).is_ok());
    }

    #[test]
    fn rejects_zero_missing_and_unknown_timeout_values() {
        let zero = VALID.replace("\"connectPollSeconds\": 1", "\"connectPollSeconds\": 0");
        assert!(serde_json::from_str::<RuntimeTimeouts>(&zero).is_err());
        assert!(serde_json::from_str::<RuntimeTimeouts>("{}").is_err());
        let unknown = VALID.replace(
            "\"clickhouseConnectSeconds\": 10,",
            "\"unknownSeconds\": 1, \"clickhouseConnectSeconds\": 10,",
        );
        assert!(serde_json::from_str::<RuntimeTimeouts>(&unknown).is_err());
    }
}
