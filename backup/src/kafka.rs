use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, bail};
use rdkafka::{
    ClientConfig,
    consumer::{BaseConsumer, Consumer},
};

use crate::model::ConnectorCheckpoint;

pub struct KafkaLog {
    consumer: BaseConsumer,
}

impl KafkaLog {
    pub fn new(bootstrap_servers: &str, properties: &HashMap<String, String>) -> Result<Self> {
        let mut config = ClientConfig::new();
        for (key, value) in properties {
            config.set(key, value);
        }
        config.set("bootstrap.servers", bootstrap_servers);
        Ok(Self {
            consumer: config.create()?,
        })
    }

    pub fn verify(&self, checkpoints: &[ConnectorCheckpoint]) -> Result<()> {
        for checkpoint in checkpoints {
            let metadata = self
                .consumer
                .fetch_metadata(Some(&checkpoint.topic), Duration::from_secs(15))?;
            let [topic] = metadata.topics() else {
                bail!(
                    "Kafka did not return exactly one topic: {}",
                    checkpoint.topic
                )
            };
            if let Some(error) = topic.error() {
                bail!("Kafka metadata failed for {}: {error:?}", checkpoint.topic)
            }
            verify_partition_count(
                &checkpoint.topic,
                checkpoint.partitions,
                topic.partitions().len(),
            )?;
            for offset in &checkpoint.offsets {
                let partition = offset.partition.kafka_partition as i32;
                let (low, high) = self.consumer.fetch_watermarks(
                    &checkpoint.topic,
                    partition,
                    Duration::from_secs(15),
                )?;
                verify_watermarks(
                    &checkpoint.topic,
                    partition,
                    offset.offset.kafka_offset,
                    low,
                    high,
                )?;
            }
        }
        Ok(())
    }
}

fn verify_partition_count(topic: &str, expected: u32, actual: usize) -> Result<()> {
    if actual != expected as usize {
        bail!(
            "Kafka topic partition count changed for {topic}: expected {expected}, found {actual}"
        )
    }
    Ok(())
}

fn verify_watermarks(topic: &str, partition: i32, offset: u64, low: i64, high: i64) -> Result<()> {
    let offset = i64::try_from(offset).context("Kafka offset exceeds the supported range")?;
    if low < 0 || high < low {
        bail!("Kafka returned invalid watermarks for {topic}-{partition}: {low}..{high}")
    }
    if offset < low {
        bail!(
            "Kafka retention removed required replay records for {topic}-{partition}: recovery offset {offset}, log start {low}"
        )
    }
    if offset > high {
        bail!(
            "recovery offset is beyond the Kafka log end for {topic}-{partition}: recovery offset {offset}, log end {high}"
        )
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{verify_partition_count, verify_watermarks};

    #[test]
    fn accepts_offsets_inside_log_including_end() {
        assert!(verify_watermarks("events", 0, 5, 5, 10).is_ok());
        assert!(verify_watermarks("events", 0, 10, 5, 10).is_ok());
    }

    #[test]
    fn rejects_retention_loss_log_rollback_and_invalid_watermarks() {
        assert!(verify_watermarks("events", 0, 4, 5, 10).is_err());
        assert!(verify_watermarks("events", 0, 11, 5, 10).is_err());
        assert!(verify_watermarks("events", 0, 5, -1, 10).is_err());
        assert!(verify_watermarks("events", 0, 5, 10, 9).is_err());
        assert!(verify_watermarks("events", 0, u64::MAX, 0, 10).is_err());
    }

    #[test]
    fn rejects_partition_count_changes() {
        assert!(verify_partition_count("events", 3, 3).is_ok());
        assert!(verify_partition_count("events", 3, 4).is_err());
    }
}
