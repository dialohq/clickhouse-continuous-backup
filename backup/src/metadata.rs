use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, bail};
use futures::{TryStreamExt, stream::try_unfold};
use rdkafka::{
    ClientConfig, Message, Offset, TopicPartitionList,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord, Producer},
};

use crate::{
    config::RuntimeTimeouts,
    model::{BackupChain, BackupChainState, BackupManifest},
};

const CHAIN_STATE_KEY: &str = "__durable_clickhouse_sink_chain_state";

pub(crate) trait BackupMetadataStorage {
    type Lease<'a>: BackupMetadataLease
    where
        Self: 'a;

    async fn acquire(&self) -> Result<Self::Lease<'_>>;
}

pub(crate) trait BackupMetadataLease {
    fn chain(&self) -> Option<&BackupChain>;

    fn chain_state(&self) -> Option<&BackupChainState> {
        self.chain().map(|chain| &chain.state)
    }

    async fn commit(self, manifest: &BackupManifest, chain_state: &BackupChainState) -> Result<()>;
}

pub(crate) trait BackupMetadataReader {
    async fn load_manifest(&self, backup_id: &str) -> Result<Option<BackupManifest>>;
}

pub(crate) struct KafkaBackupMetadataStorage {
    producer: FutureProducer,
    consumer: StreamConsumer,
    topic: String,
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
            metadata_timeout: timeouts.kafka_metadata,
            read_timeout: timeouts.kafka_catalog_read,
            transaction_timeout: timeouts.kafka_transaction,
        })
    }

    async fn read(&self, key: &str) -> Result<Option<BackupChain>> {
        let metadata = self
            .consumer
            .fetch_metadata(Some(&self.topic), self.metadata_timeout)?;
        let [topic] = metadata.topics() else {
            bail!("Kafka did not return exactly one backup metadata topic")
        };
        if let Some(error) = topic.error() {
            let t = self.topic.clone();
            bail!("Kafka backup-metadata lookup failed for topic {t}: {error:?}")
        }
        if topic.partitions().len() != 1 {
            let l = topic.partitions().len();
            bail!("the Kafka backup metadata topic must have exactly one partition, got: {l}")
        }
        self.consumer.subscribe(&[&self.topic])?;
        let (low, high) = self
            .consumer
            .fetch_watermarks(&self.topic, 0, self.metadata_timeout)?;
        if low < 0 || high < low {
            bail!("Kafka returned invalid backup-metadata watermarks: {low}..{high}")
        }
        let records = try_unfold(high == 0, move |done| async move {
            if done {
                return Ok::<_, anyhow::Error>(None);
            }
            let item = tokio::time::timeout(self.read_timeout, self.consumer.recv())
                .await
                .context("timed out acquiring exclusive backup metadata ownership")?;
            match item {
                Ok(msg) => {
                    // high is exclusive, still have to check since offsets can be skipped
                    // and records could be added since the check
                    if msg.offset() >= high {
                        return Ok(None);
                    }
                    let done = msg.offset() + 1 >= high;
                    Ok(Some((msg, done)))
                }
                Err(rdkafka::error::KafkaError::PartitionEOF(_)) => Ok(None),
                Err(err) => bail!("Error reading messages from metadata topic-partition: {err}"),
            }
        })
        .try_collect::<Vec<_>>()
        .await?;

        let Some(payload) = records
            .iter()
            .rev()
            .find(|msg| msg.key() == Some(key.as_bytes()))
            .and_then(|msg| msg.payload())
        else {
            return Ok(None);
        };
        let state: BackupChainState =
            serde_json::from_slice(payload).context("Invalid backup chain state")?;

        let mut chain: Vec<BackupManifest> = Vec::new();
        let mut id = state.tip.id;
        loop {
            if chain.iter().any(|manifest| manifest.backup.id == id) {
                bail!("Cycle in backup chain at {id}")
            }
            let manifest_key = id.to_string();
            let payload = records
                .iter()
                .rev()
                .find(|msg| msg.key() == Some(manifest_key.as_bytes()))
                .and_then(|msg| msg.payload())
                .with_context(|| format!("Missing backup manifest {id}"))?;
            let manifest: BackupManifest =
                serde_json::from_slice(payload).context("Invalid backup manifest")?;
            if manifest.backup.id != id {
                bail!("Backup manifest ID does not match catalog key {id}")
            }
            let parent = manifest.backup.parent.as_ref().map(|parent| parent.id);
            chain.push(manifest);
            match parent {
                Some(parent) => id = parent,
                None => {
                    return Ok(Some(BackupChain {
                        state,
                        manifests: chain,
                    }));
                }
            }
        }
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
    manifest_chain: Option<BackupChain>,
}

impl BackupMetadataStorage for KafkaBackupMetadataStorage {
    type Lease<'a> = KafkaBackupMetadataLease<'a>;

    async fn acquire(&self) -> Result<Self::Lease<'_>> {
        let chain_state = self.read(CHAIN_STATE_KEY).await?;
        Ok(KafkaBackupMetadataLease {
            storage: self,
            manifest_chain: chain_state,
        })
    }
}

impl BackupMetadataLease for KafkaBackupMetadataLease<'_> {
    fn chain(&self) -> Option<&BackupChain> {
        self.manifest_chain.as_ref()
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
