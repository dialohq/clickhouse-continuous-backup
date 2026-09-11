use anyhow::Result;
use durable_clickhouse_backup::{config::BackupConfig, model::BackupOutput};
use serde_json::json;

pub struct BackupClient {
    connect_url: String,
    clickhouse_url: String,
    kafka: String,
}

impl BackupClient {
    pub fn new(connect_url: String, clickhouse_url: String, kafka: String) -> Self {
        Self {
            connect_url,
            clickhouse_url,
            kafka,
        }
    }

    pub async fn run(&self, run_id: &str) -> Result<BackupOutput> {
        durable_clickhouse_backup::run(&self.config(run_id)?).await
    }

    pub fn config(&self, run_id: &str) -> Result<BackupConfig> {
        let config = json!({
            "connectUrl": self.connect_url,
            "clickhouseUrl": self.clickhouse_url,
            "namedCollection": "durable_backups",
            "pathPrefix": "durable-e2e",
            "archiveExtension": "tar.zst",
            "pauseTimeoutSeconds": 30,
            "kafkaBootstrapServers": self.kafka,
            "kafkaPropertiesFile": null,
            "catalogTopic": "durable-clickhouse-sink.backup-catalog",
            "maxIncrementalsPerFull": 2,
            "maxBackupBandwidth": 262144,
            "pipelines": [{
                "connector": "durable-clickhouse-sink-records",
                "database": "durable_e2e",
                "state_table": "durable_clickhouse_sink_records_state",
                "keeper_path": "/durable-clickhouse-sink/e2e/records",
                "table": "records",
                "topic": "records.input",
                "partitions": 3
            }],
            "timeouts": {
                "clickhouseConnectSeconds": 10,
                "connectConnectSeconds": 10,
                "connectRequestSeconds": 30,
                "connectPollSeconds": 1,
                "kafkaMetadataSeconds": 10,
                "kafkaCatalogReadSeconds": 15,
                "kafkaTransactionSeconds": 30,
                "kafkaMaxPollSeconds": 86400,
                "kafkaReplayPollSeconds": 15,
                "recoveryCatchupSeconds": 3600,
                "controllerRetrySeconds": 15
            }
        });
        let mut config: BackupConfig = serde_json::from_value(config)?;
        config.run_id = run_id.to_owned();
        config.clickhouse_username = "default".to_owned();
        Ok(config)
    }
}
