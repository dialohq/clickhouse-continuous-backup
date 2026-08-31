use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, bail};
use rdkafka::{
    ClientConfig, Message,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord, Producer},
};

use crate::{
    config::RuntimeTimeouts,
    model::{BackupChainState, BackupManifest},
};

const CHAIN_STATE_KEY: &str = "__durable_clickhouse_sink_chain_state";

pub(crate) trait BackupMetadataStorage {
    type Lease<'a>: BackupMetadataLease
    where
        Self: 'a;

    async fn acquire(&self) -> Result<Self::Lease<'_>>;
}

pub(crate) trait BackupMetadataLease {
    fn chain_state(&self) -> Option<&BackupChainState>;

    async fn commit(self, manifest: &BackupManifest, chain_state: &BackupChainState) -> Result<()>;
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

pub(crate) struct KafkaBackupMetadataLease<'a> {
    storage: &'a KafkaBackupMetadataStorage,
    chain_state: Option<BackupChainState>,
}

impl BackupMetadataStorage for KafkaBackupMetadataStorage {
    type Lease<'a> = KafkaBackupMetadataLease<'a>;

    async fn acquire(&self) -> Result<Self::Lease<'_>> {
        let chain_state = self
            .read(CHAIN_STATE_KEY)
            .await?
            .map(|value| serde_json::from_str(&value).context("invalid backup chain state"))
            .transpose()?;
        Ok(KafkaBackupMetadataLease {
            storage: self,
            chain_state,
        })
    }
}

impl BackupMetadataLease for KafkaBackupMetadataLease<'_> {
    fn chain_state(&self) -> Option<&BackupChainState> {
        self.chain_state.as_ref()
    }

    async fn commit(self, manifest: &BackupManifest, chain_state: &BackupChainState) -> Result<()> {
        let manifest_key = manifest.backup.id.to_string();
        let manifest_json = serde_json::to_string(manifest)?;
        let chain_state_json = serde_json::to_string(chain_state)?;
        self.storage
            .publish(&[
                (&manifest_key, &manifest_json),
                (CHAIN_STATE_KEY, &chain_state_json),
            ])
            .await
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
