use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::{
    Deserialize, Deserializer,
    de::{DeserializeOwned, Error as _},
};

use crate::model::{Pipeline, clickhouse_identifier, safe_chain_id, safe_storage_path};

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

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BackupConfig {
    pub connect_url: String,
    pub clickhouse_url: String,
    #[serde(skip)]
    pub clickhouse_username: String,
    #[serde(skip)]
    pub clickhouse_password: String,
    pub named_collection: String,
    pub path_prefix: String,
    pub archive_extension: String,
    #[serde(skip)]
    pub run_id: String,
    #[serde(rename = "pauseTimeoutSeconds", deserialize_with = "positive_seconds")]
    pub pause_timeout: Duration,
    pub kafka_bootstrap_servers: String,
    pub kafka_properties_file: Option<PathBuf>,
    pub catalog_topic: String,
    pub max_incrementals_per_full: u32,
    pub max_backup_bandwidth: u64,
    pub pipelines: Vec<Pipeline>,
    pub timeouts: RuntimeTimeouts,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TargetConfig {
    pub clickhouse_url: String,
    clickhouse_properties_file: PathBuf,
    #[serde(skip)]
    pub clickhouse_username: String,
    #[serde(skip)]
    pub clickhouse_password: String,
    pub pipelines: Vec<Pipeline>,
    pub timeouts: RuntimeTimeouts,
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
    pub fn from_file(path: &Path) -> Result<Self> {
        let mut config: Self = read_config(path)?;
        config.run_id = required("BACKUP_RUN_ID")?;
        config.clickhouse_username = required("CLICKHOUSE_USERNAME")?;
        config.clickhouse_password = env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.clickhouse_username.is_empty() {
            bail!("ClickHouse username is required")
        }
        if !safe_chain_id(&self.run_id) {
            bail!("BACKUP_RUN_ID contains unsupported characters")
        }
        if !clickhouse_identifier(&self.named_collection) {
            bail!("namedCollection must be a ClickHouse identifier")
        }
        if !safe_storage_path(&self.path_prefix) {
            bail!("pathPrefix must be a relative object path without empty, . or .. segments")
        }
        if !["tar.zst", "tar.gz", "tar.xz", "tar.bz2", "tgz", "tzst"]
            .contains(&self.archive_extension.as_str())
        {
            bail!("unsupported archiveExtension")
        }
        validate_pipelines(&self.pipelines)?;
        Ok(())
    }

    pub fn kafka_properties(&self) -> Result<HashMap<String, String>> {
        read_kafka_properties(self.kafka_properties_file.as_ref())
    }
}

impl TargetConfig {
    pub fn from_file(path: &Path) -> Result<Self> {
        let mut config: Self = read_config(path)?;
        validate_pipelines(&config.pipelines)?;
        let credentials = read_properties(&config.clickhouse_properties_file, "ClickHouse")?;
        config.clickhouse_username = credentials
            .get("username")
            .cloned()
            .context("ClickHouse username property is required")?;
        config.clickhouse_password = credentials.get("password").cloned().unwrap_or_default();
        Ok(config)
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

fn read_config<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read config from {}", path.display()))?;
    serde_json::from_str(&contents).with_context(|| format!("invalid config in {}", path.display()))
}

fn validate_pipelines(pipelines: &[Pipeline]) -> Result<()> {
    if pipelines.is_empty() {
        bail!("pipelines must contain at least one pipeline")
    }
    pipelines.iter().try_for_each(Pipeline::validate)?;
    Ok(())
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
