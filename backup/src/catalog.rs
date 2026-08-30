use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, bail};
use rdkafka::{
    ClientConfig, Message, Offset, TopicPartitionList,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord, Producer},
};

use crate::config::RuntimeTimeouts;

pub struct Catalog {
    producer: FutureProducer,
    consumer: StreamConsumer,
    topic: String,
    acquire_timeout: Duration,
    metadata_timeout: Duration,
    read_timeout: Duration,
    transaction_timeout: Duration,
}

impl Catalog {
    pub fn new(
        bootstrap_servers: &str,
        properties: &HashMap<String, String>,
        topic: String,
        run_id: &str,
        timeouts: &RuntimeTimeouts,
    ) -> Result<Self> {
        let lock_group = format!("{topic}.backup-lock");
        if lock_group.len() > 255 {
            bail!("backup catalog topic is too long to derive the lock group")
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

    pub async fn get(&self, key: &str) -> Result<Option<String>> {
        let metadata = self
            .consumer
            .fetch_metadata(Some(&self.topic), self.metadata_timeout)?;
        let [topic] = metadata.topics() else {
            bail!("Kafka did not return exactly one backup catalog topic")
        };
        if let Some(error) = topic.error() {
            bail!("Kafka backup-catalog metadata failed: {error:?}")
        }
        if topic.partitions().len() != 1 {
            bail!("the backup catalog topic must have exactly one partition")
        }
        self.consumer.subscribe(&[&self.topic])?;
        let first = tokio::time::timeout(self.acquire_timeout, self.consumer.recv())
            .await
            .context("timed out acquiring the backup lock")?;
        let (low, high) = self
            .consumer
            .fetch_watermarks(&self.topic, 0, self.metadata_timeout)?;
        if low < 0 || high < low {
            bail!("Kafka returned invalid backup-catalog watermarks: {low}..{high}")
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
                Err(_) => bail!("timed out reading the backup catalog"),
            }
        }
        Ok(value)
    }

    pub async fn publish(&self, records: &[(&str, &str)]) -> Result<()> {
        if records.is_empty() {
            bail!("at least one backup catalog record is required")
        }
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
                return Err(error).context("failed to publish backup catalog record");
            }
        }
        self.producer.commit_transaction(self.transaction_timeout)?;
        Ok(())
    }
}

pub async fn lookup(
    bootstrap_servers: &str,
    properties: &HashMap<String, String>,
    topic: &str,
    key: &str,
    identity: &str,
    timeouts: &RuntimeTimeouts,
) -> Result<Option<String>> {
    let mut config = ClientConfig::new();
    for (name, value) in properties {
        config.set(name, value);
    }
    let consumer: StreamConsumer = config
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
    let metadata = consumer.fetch_metadata(Some(topic), timeouts.kafka_metadata)?;
    let [metadata] = metadata.topics() else {
        bail!("Kafka did not return exactly one recovery topic")
    };
    if let Some(error) = metadata.error() {
        bail!("Kafka recovery-topic metadata failed: {error:?}")
    }
    if metadata.partitions().len() != 1 {
        bail!("the recovery topic must have exactly one partition")
    }
    let (low, high) = consumer.fetch_watermarks(topic, 0, timeouts.kafka_metadata)?;
    if low < 0 || high < low {
        bail!("Kafka returned invalid recovery-topic watermarks: {low}..{high}")
    }
    if low == high {
        return Ok(None);
    }
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(topic, 0, Offset::Offset(low))?;
    consumer.assign(&assignment)?;
    let mut value = None;
    loop {
        match tokio::time::timeout(timeouts.kafka_catalog_read, consumer.recv()).await {
            Ok(Ok(message)) => {
                if inspect(&message, key, high, &mut value) {
                    return Ok(value);
                }
            }
            Ok(Err(rdkafka::error::KafkaError::PartitionEOF(_))) => return Ok(value),
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => bail!("timed out reading recovery catalog"),
        }
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
