use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, bail};
use rdkafka::{
    ClientConfig, Message, Offset, TopicPartitionList,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord, Producer},
};

use crate::{
    config::RuntimeTimeouts,
    model::{BackupManifest, ChainHead},
};

const CHAIN_HEAD_KEY: &str = "__durable_clickhouse_sink_chain_head";

pub(crate) trait BackupMetadataStorage {
    /// Acquires exclusive backup ownership and returns the latest committed chain state.
    async fn acquire_chain_head(&self) -> Result<Option<ChainHead>>;

    /// Makes the immutable manifest and replacement chain state durable as one commit.
    async fn commit_backup(&self, manifest: &BackupManifest, head: &ChainHead) -> Result<()>;
}

pub(crate) trait BackupMetadataReader {
    async fn load_manifest(&self, backup_id: &str) -> Result<Option<BackupManifest>>;
}

pub(crate) struct KafkaBackupMetadataStorage {
    producer: FutureProducer,
    consumer: StreamConsumer,
    topic: String,
    acquire_timeout: Duration,
    metadata_timeout: Duration,
    read_timeout: Duration,
    transaction_timeout: Duration,
}

impl KafkaBackupMetadataStorage {
    pub(crate) fn new(
        bootstrap_servers: &str,
        properties: &HashMap<String, String>,
        topic: String,
        run_id: &str,
        timeouts: &RuntimeTimeouts,
    ) -> Result<Self> {
        let lock_group = format!("{topic}.backup-lock");
        if lock_group.len() > 255 {
            bail!("backup metadata topic is too long to derive the lock group")
        }
        let mut common = ClientConfig::new();
        for (key, value) in properties {
            common.set(key, value);
        }
        common.set("bootstrap.servers", bootstrap_servers);
        let producer = common
            .clone()
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set(
                "transactional.id",
                format!("durable-clickhouse-backup-{run_id}"),
            )
            .create()?;
        let consumer = common
            .set("group.id", lock_group)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .set("isolation.level", "read_committed")
            .set("enable.partition.eof", "true")
            .set(
                "max.poll.interval.ms",
                timeouts.kafka_max_poll.as_millis().to_string(),
            )
            .create()?;
        Ok(Self {
            producer,
            consumer,
            topic,
            acquire_timeout: timeouts.kafka_catalog_acquire,
            metadata_timeout: timeouts.kafka_metadata,
            read_timeout: timeouts.kafka_catalog_read,
            transaction_timeout: timeouts.kafka_transaction,
        })
    }

    async fn read(&self, key: &str) -> Result<Option<String>> {
        let metadata = self
            .consumer
            .fetch_metadata(Some(&self.topic), self.metadata_timeout)?;
        let [topic] = metadata.topics() else {
            bail!("Kafka did not return exactly one backup metadata topic")
        };
        if let Some(error) = topic.error() {
            bail!("Kafka backup-metadata lookup failed: {error:?}")
        }
        if topic.partitions().len() != 1 {
            bail!("the Kafka backup metadata topic must have exactly one partition")
        }
        self.consumer.subscribe(&[&self.topic])?;
        let first = tokio::time::timeout(self.acquire_timeout, self.consumer.recv())
            .await
            .context("timed out acquiring exclusive backup metadata ownership")?;
        let (low, high) = self
            .consumer
            .fetch_watermarks(&self.topic, 0, self.metadata_timeout)?;
        if low < 0 || high < low {
            bail!("Kafka returned invalid backup-metadata watermarks: {low}..{high}")
        }
        let mut value = None;
        match first {
            Ok(message) if inspect(&message, key, high, &mut value) => return Ok(value),
            Ok(_) => {}
            Err(rdkafka::error::KafkaError::PartitionEOF(_)) if high == 0 => return Ok(None),
            Err(error) => return Err(error.into()),
        }
        loop {
            match tokio::time::timeout(self.read_timeout, self.consumer.recv()).await {
                Ok(Ok(message)) => {
                    if inspect(&message, key, high, &mut value) {
                        break;
                    }
                }
                Ok(Err(rdkafka::error::KafkaError::PartitionEOF(_))) => break,
                Ok(Err(error)) => return Err(error.into()),
                Err(_) => bail!("timed out reading Kafka backup metadata"),
            }
        }
        Ok(value)
    }

    async fn publish(&self, records: &[(&str, &str)]) -> Result<()> {
        self.producer.init_transactions(self.transaction_timeout)?;
        self.producer.begin_transaction()?;
        for &(key, value) in records {
            if let Err((error, _)) = self
                .producer
                .send(
                    FutureRecord::to(&self.topic)
                        .partition(0)
                        .key(key)
                        .payload(value),
                    self.transaction_timeout,
                )
                .await
            {
                self.producer
                    .abort_transaction(self.transaction_timeout)
                    .ok();
                return Err(error).context("failed to publish Kafka backup metadata");
            }
        }
        self.producer.commit_transaction(self.transaction_timeout)?;
        Ok(())
    }
}

impl BackupMetadataStorage for KafkaBackupMetadataStorage {
    async fn acquire_chain_head(&self) -> Result<Option<ChainHead>> {
        self.read(CHAIN_HEAD_KEY)
            .await?
            .map(|value| serde_json::from_str(&value).context("invalid backup chain head"))
            .transpose()
    }

    async fn commit_backup(&self, manifest: &BackupManifest, head: &ChainHead) -> Result<()> {
        let manifest_key = manifest.backup.id.to_string();
        let manifest_json = serde_json::to_string(manifest)?;
        let head_json = serde_json::to_string(head)?;
        self.publish(&[
            (&manifest_key, &manifest_json),
            (CHAIN_HEAD_KEY, &head_json),
        ])
        .await
    }
}

pub(crate) struct KafkaBackupMetadataReader {
    consumer: StreamConsumer,
    topic: String,
    metadata_timeout: Duration,
    read_timeout: Duration,
}

impl KafkaBackupMetadataReader {
    pub(crate) fn new(
        bootstrap_servers: &str,
        properties: &HashMap<String, String>,
        topic: String,
        identity: &str,
        timeouts: &RuntimeTimeouts,
    ) -> Result<Self> {
        let mut config = ClientConfig::new();
        for (name, value) in properties {
            config.set(name, value);
        }
        let consumer = config
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", format!("{topic}.recovery-{identity}"))
            .set("enable.auto.commit", "false")
            .set("isolation.level", "read_committed")
            .set("enable.partition.eof", "true")
            .set(
                "max.poll.interval.ms",
                timeouts.kafka_max_poll.as_millis().to_string(),
            )
            .create()?;
        Ok(Self {
            consumer,
            topic,
            metadata_timeout: timeouts.kafka_metadata,
            read_timeout: timeouts.kafka_catalog_read,
        })
    }
}

impl BackupMetadataReader for KafkaBackupMetadataReader {
    async fn load_manifest(&self, backup_id: &str) -> Result<Option<BackupManifest>> {
        let metadata = self
            .consumer
            .fetch_metadata(Some(&self.topic), self.metadata_timeout)?;
        let [metadata] = metadata.topics() else {
            bail!("Kafka did not return exactly one backup metadata topic")
        };
        if let Some(error) = metadata.error() {
            bail!("Kafka backup-metadata lookup failed: {error:?}")
        }
        if metadata.partitions().len() != 1 {
            bail!("the Kafka backup metadata topic must have exactly one partition")
        }
        let (low, high) = self
            .consumer
            .fetch_watermarks(&self.topic, 0, self.metadata_timeout)?;
        if low < 0 || high < low {
            bail!("Kafka returned invalid backup-metadata watermarks: {low}..{high}")
        }
        if low == high {
            return Ok(None);
        }
        let mut assignment = TopicPartitionList::new();
        assignment.add_partition_offset(&self.topic, 0, Offset::Offset(low))?;
        self.consumer.assign(&assignment)?;
        let mut value = None;
        loop {
            match tokio::time::timeout(self.read_timeout, self.consumer.recv()).await {
                Ok(Ok(message)) => {
                    if inspect(&message, backup_id, high, &mut value) {
                        break;
                    }
                }
                Ok(Err(rdkafka::error::KafkaError::PartitionEOF(_))) => break,
                Ok(Err(error)) => return Err(error.into()),
                Err(_) => bail!("timed out reading Kafka backup metadata"),
            }
        }
        value
            .map(|value| serde_json::from_str(&value).context("invalid backup manifest"))
            .transpose()
    }
}

fn inspect<M: Message>(message: &M, key: &str, high: i64, value: &mut Option<String>) -> bool {
    if message.key() == Some(key.as_bytes()) {
        *value = message
            .payload()
            .map(|payload| String::from_utf8_lossy(payload).into_owned());
    }
    message.offset() + 1 >= high
}
