use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, bail};
use rdkafka::{
    ClientConfig, Message,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord, Producer},
};

pub struct Catalog {
    producer: FutureProducer,
    consumer: StreamConsumer,
    topic: String,
}

impl Catalog {
    pub fn new(
        bootstrap_servers: &str,
        properties: &HashMap<String, String>,
        topic: String,
        run_id: &str,
    ) -> Result<Self> {
        let lock_group = format!("{topic}.backup-lock");
        if lock_group.len() > 255 {
            bail!("recovery topic is too long to derive the backup lock group")
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
                format!("durable-clickhouse-recovery-{run_id}"),
            )
            .create()?;
        let consumer = common
            .set("group.id", lock_group)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .set("isolation.level", "read_committed")
            .set("enable.partition.eof", "true")
            .set("max.poll.interval.ms", "86400000")
            .create()?;
        Ok(Self {
            producer,
            consumer,
            topic,
        })
    }

    pub async fn get(&self, key: &str) -> Result<Option<String>> {
        let metadata = self
            .consumer
            .fetch_metadata(Some(&self.topic), Duration::from_secs(15))?;
        let [topic] = metadata.topics() else {
            bail!("Kafka did not return exactly one recovery topic")
        };
        if let Some(error) = topic.error() {
            bail!("Kafka recovery-topic metadata failed: {error:?}")
        }
        if topic.partitions().len() != 1 {
            bail!("the recovery topic must have exactly one partition")
        }
        self.consumer.subscribe(&[&self.topic])?;
        let first = tokio::time::timeout(Duration::from_secs(30), self.consumer.recv())
            .await
            .context("timed out acquiring the backup lock")?;
        let (low, high) =
            self.consumer
                .fetch_watermarks(&self.topic, 0, Duration::from_secs(15))?;
        if low < 0 || high < low {
            bail!("Kafka returned invalid recovery-topic watermarks: {low}..{high}")
        }
        let mut value = None;
        match first {
            Ok(message) if inspect(&message, key, high, &mut value) => return Ok(value),
            Ok(_) => {}
            Err(rdkafka::error::KafkaError::PartitionEOF(_)) if high == 0 => return Ok(None),
            Err(error) => return Err(error.into()),
        }
        loop {
            match tokio::time::timeout(Duration::from_secs(15), self.consumer.recv()).await {
                Ok(Ok(message)) => {
                    if inspect(&message, key, high, &mut value) {
                        break;
                    }
                }
                Ok(Err(rdkafka::error::KafkaError::PartitionEOF(_))) => break,
                Ok(Err(error)) => return Err(error.into()),
                Err(_) => bail!("timed out reading recovery catalog"),
            }
        }
        Ok(value)
    }

    pub async fn publish(&self, records: &[(&str, &str)]) -> Result<()> {
        if records.is_empty() {
            bail!("at least one recovery catalog record is required")
        }
        self.producer.init_transactions(Duration::from_secs(30))?;
        self.producer.begin_transaction()?;
        for &(key, value) in records {
            if let Err((error, _)) = self
                .producer
                .send(
                    FutureRecord::to(&self.topic)
                        .partition(0)
                        .key(key)
                        .payload(value),
                    Duration::from_secs(30),
                )
                .await
            {
                self.producer
                    .abort_transaction(Duration::from_secs(30))
                    .ok();
                return Err(error).context("failed to publish recovery catalog record");
            }
        }
        self.producer.commit_transaction(Duration::from_secs(30))?;
        Ok(())
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
