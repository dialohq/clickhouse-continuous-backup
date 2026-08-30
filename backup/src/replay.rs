use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, bail};
use rdkafka::{
    ClientConfig, Message, Offset, TopicPartitionList,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    consumer::{Consumer, StreamConsumer},
    error::RDKafkaErrorCode,
    message::{Header, Headers, OwnedHeaders, Timestamp},
    producer::{FutureProducer, FutureRecord, Producer},
};

use crate::{config::RuntimeTimeouts, recovery_resource::RecoveryOffset};

pub struct KafkaReplay {
    bootstrap_servers: String,
    properties: HashMap<String, String>,
    metadata_timeout: Duration,
    poll_timeout: Duration,
    transaction_timeout: Duration,
    max_poll_interval: Duration,
    replication_factor: i32,
    retention_ms: u64,
    batch_records: usize,
}

impl KafkaReplay {
    pub fn new(
        bootstrap_servers: String,
        properties: HashMap<String, String>,
        replication_factor: i32,
        retention_ms: u64,
        batch_records: usize,
        timeouts: &RuntimeTimeouts,
    ) -> Self {
        Self {
            bootstrap_servers,
            properties,
            metadata_timeout: timeouts.kafka_metadata,
            poll_timeout: timeouts.kafka_replay_poll,
            transaction_timeout: timeouts.kafka_transaction,
            max_poll_interval: timeouts.kafka_max_poll,
            replication_factor,
            retention_ms,
            batch_records,
        }
    }

    pub fn verify_ranges(
        &self,
        starts: &[RecoveryOffset],
        targets: &[RecoveryOffset],
    ) -> Result<()> {
        let consumer: StreamConsumer = self.consumer_config("range-validation")?.create()?;
        for target in targets {
            let start = starts
                .iter()
                .find(|start| start.topic == target.topic && start.partition == target.partition)
                .context("target offset has no matching checkpoint")?;
            let partition = i32::try_from(target.partition).context("partition exceeds i32")?;
            let (low, high) =
                consumer.fetch_watermarks(&target.topic, partition, self.metadata_timeout)?;
            validate_range(start.offset, target.offset, low, high)?;
        }
        Ok(())
    }

    pub fn verify_starts(&self, starts: &[RecoveryOffset]) -> Result<()> {
        let consumer: StreamConsumer = self.consumer_config("start-validation")?.create()?;
        for start in starts {
            let partition = i32::try_from(start.partition).context("partition exceeds i32")?;
            let (low, high) =
                consumer.fetch_watermarks(&start.topic, partition, self.metadata_timeout)?;
            validate_range(start.offset, start.offset, low, high)?;
        }
        Ok(())
    }

    pub fn verify_replay_retained(&self, offsets: &[RecoveryOffset]) -> Result<()> {
        let consumer: StreamConsumer = self
            .consumer_config("replay-retention-validation")?
            .create()?;
        for offset in offsets {
            let partition = i32::try_from(offset.partition).context("partition exceeds i32")?;
            let (low, high) =
                consumer.fetch_watermarks(&offset.topic, partition, self.metadata_timeout)?;
            if low != 0 || high < i64::try_from(offset.offset)? {
                bail!("bounded replay topic no longer contains its complete log")
            }
        }
        Ok(())
    }

    pub async fn copy(
        &self,
        identity: &str,
        scope: &str,
        source_topic: &str,
        replay_topic: &str,
        starts: &[RecoveryOffset],
        targets: &[RecoveryOffset],
    ) -> Result<Vec<RecoveryOffset>> {
        let starts = offsets_for_topic(source_topic, starts)?;
        let targets = offsets_for_topic(source_topic, targets)?;
        if starts.len() != targets.len() {
            bail!("source start and target partition sets differ")
        }
        let created = self.ensure_topic(replay_topic, starts.len()).await?;
        let group = kafka_identity("durable-clickhouse-replay", identity, scope)?;
        let transaction = kafka_identity("durable-clickhouse-copy", identity, scope)?;
        let consumer: StreamConsumer = self.consumer_config(&group)?.create()?;
        let producer: FutureProducer = self
            .common_config()
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("transactional.id", transaction)
            .create()?;
        producer.init_transactions(self.transaction_timeout)?;

        for (start, target) in starts.iter().zip(&targets) {
            if start.partition != target.partition {
                bail!("source start and target partitions differ")
            }
            self.copy_partition(
                &consumer,
                &producer,
                source_topic,
                replay_topic,
                start,
                target,
                created,
            )
            .await?;
        }
        self.committed_end_offsets(replay_topic, starts.len()).await
    }

    async fn committed_end_offsets(
        &self,
        topic: &str,
        partitions: usize,
    ) -> Result<Vec<RecoveryOffset>> {
        let mut offsets = Vec::with_capacity(partitions);
        for partition in 0..partitions {
            offsets.push(RecoveryOffset {
                topic: topic.to_owned(),
                partition: partition as u32,
                offset: self
                    .last_committed_record(topic, partition as i32)
                    .await?
                    .map_or(0, |offset| offset + 1),
            });
        }
        Ok(offsets)
    }

    async fn ensure_topic(&self, topic: &str, partitions: usize) -> Result<bool> {
        let admin: AdminClient<_> = self.common_config().create()?;
        let partitions = i32::try_from(partitions).context("partition count exceeds i32")?;
        let retention = self.retention_ms.to_string();
        let topic_spec = NewTopic::new(
            topic,
            partitions,
            TopicReplication::Fixed(self.replication_factor),
        )
        .set("cleanup.policy", "delete")
        .set("retention.ms", &retention);
        let results = admin
            .create_topics([&topic_spec], &AdminOptions::new())
            .await?;
        let [result] = results.as_slice() else {
            bail!("Kafka returned an unexpected create-topics result")
        };
        match result {
            Ok(_) => Ok(true),
            Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {
                let metadata = admin
                    .inner()
                    .fetch_metadata(Some(topic), self.metadata_timeout)?;
                let [metadata] = metadata.topics() else {
                    bail!("Kafka did not return replay-topic metadata")
                };
                if metadata.partitions().len() != partitions as usize {
                    bail!("existing replay topic has a different partition count")
                }
                Ok(false)
            }
            Err((_, error)) => Err(anyhow::anyhow!("failed to create replay topic: {error:?}")),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn copy_partition(
        &self,
        consumer: &StreamConsumer,
        producer: &FutureProducer,
        source_topic: &str,
        replay_topic: &str,
        start: &RecoveryOffset,
        target: &RecoveryOffset,
        topic_was_created: bool,
    ) -> Result<()> {
        let partition = i32::try_from(start.partition).context("partition exceeds i32")?;
        let mut query = TopicPartitionList::new();
        query.add_partition(source_topic, partition);
        let committed = consumer.committed_offsets(query, self.metadata_timeout)?;
        let committed = committed
            .find_partition(source_topic, partition)
            .context("Kafka omitted a committed offset")?
            .offset();
        let mut next = match committed {
            Offset::Offset(offset) => u64::try_from(offset).context("negative committed offset")?,
            Offset::Invalid => {
                if !topic_was_created
                    && self
                        .last_committed_record(replay_topic, partition)
                        .await?
                        .is_some()
                {
                    bail!("replay progress expired while committed replay records still exist")
                }
                start.offset
            }
            offset => bail!("unexpected committed source offset: {offset:?}"),
        };
        if next < start.offset || next > target.offset {
            bail!("replay consumer offset is outside the requested range")
        }
        if topic_was_created && next != start.offset {
            bail!("replay topic was recreated after source progress was committed")
        }
        while next < target.offset {
            self.validate_source_range(consumer, source_topic, partition, next, target.offset)?;
            let mut assignment = TopicPartitionList::new();
            assignment.add_partition_offset(
                source_topic,
                partition,
                Offset::Offset(i64::try_from(next)?),
            )?;
            consumer.assign(&assignment)?;
            producer.begin_transaction()?;
            let transaction = self
                .copy_batch(
                    consumer,
                    producer,
                    source_topic,
                    replay_topic,
                    partition,
                    next,
                    target.offset,
                )
                .await;
            let advanced = match transaction {
                Ok(advanced) => advanced,
                Err(error) => {
                    producer.abort_transaction(self.transaction_timeout).ok();
                    return Err(error);
                }
            };
            let mut offsets = TopicPartitionList::new();
            offsets.add_partition_offset(
                source_topic,
                partition,
                Offset::Offset(i64::try_from(advanced)?),
            )?;
            let group = consumer
                .group_metadata()
                .context("Kafka consumer has no group metadata")?;
            if let Err(error) =
                producer.send_offsets_to_transaction(&offsets, &group, self.transaction_timeout)
            {
                producer.abort_transaction(self.transaction_timeout).ok();
                return Err(error.into());
            }
            producer.commit_transaction(self.transaction_timeout)?;
            next = advanced;
        }
        Ok(())
    }

    fn validate_source_range(
        &self,
        consumer: &impl Consumer,
        topic: &str,
        partition: i32,
        start: u64,
        target: u64,
    ) -> Result<()> {
        let (low, high) = consumer.fetch_watermarks(topic, partition, self.metadata_timeout)?;
        validate_range(start, target, low, high)
    }

    async fn last_committed_record(&self, topic: &str, partition: i32) -> Result<Option<u64>> {
        let consumer: StreamConsumer =
            self.consumer_config("replay-record-validation")?.create()?;
        let mut assignment = TopicPartitionList::new();
        assignment.add_partition_offset(topic, partition, Offset::Beginning)?;
        consumer.assign(&assignment)?;
        let mut last = None;
        loop {
            match tokio::time::timeout(self.poll_timeout, consumer.recv()).await {
                Ok(Ok(message)) => last = Some(u64::try_from(message.offset())?),
                Ok(Err(rdkafka::error::KafkaError::PartitionEOF(_))) => return Ok(last),
                Ok(Err(error)) => return Err(error.into()),
                Err(_) => bail!("timed out validating existing replay records"),
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn copy_batch(
        &self,
        consumer: &StreamConsumer,
        producer: &FutureProducer,
        source_topic: &str,
        replay_topic: &str,
        partition: i32,
        start: u64,
        target: u64,
    ) -> Result<u64> {
        let mut next = start;
        let mut copied = 0;
        while next < target && copied < self.batch_records {
            match tokio::time::timeout(self.poll_timeout, consumer.recv()).await {
                Ok(Ok(message)) => {
                    if message.topic() != source_topic || message.partition() != partition {
                        bail!("Kafka returned a message outside the assigned partition")
                    }
                    let offset = u64::try_from(message.offset())?;
                    if offset < next {
                        continue;
                    }
                    if offset > next {
                        self.validate_source_range(
                            consumer,
                            source_topic,
                            partition,
                            next,
                            target,
                        )?;
                    }
                    if offset >= target {
                        return Ok(target);
                    }
                    send_message(
                        producer,
                        replay_topic,
                        partition,
                        &message,
                        self.transaction_timeout,
                    )
                    .await?;
                    next = offset + 1;
                    copied += 1;
                }
                Ok(Err(rdkafka::error::KafkaError::PartitionEOF(_))) => {
                    self.validate_source_range(consumer, source_topic, partition, next, target)?;
                    return Ok(target);
                }
                Ok(Err(error)) => return Err(error.into()),
                Err(_) => {
                    bail!("timed out reading {source_topic}-{partition} at source offset {next}")
                }
            }
        }
        Ok(next)
    }

    fn common_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        for (key, value) in &self.properties {
            config.set(key, value);
        }
        config.set("bootstrap.servers", &self.bootstrap_servers);
        config
    }

    fn consumer_config(&self, group: &str) -> Result<ClientConfig> {
        let mut config = self.common_config();
        config
            .set("group.id", group)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("isolation.level", "read_committed")
            .set("enable.partition.eof", "true")
            .set(
                "max.poll.interval.ms",
                self.max_poll_interval.as_millis().to_string(),
            );
        Ok(config)
    }
}

async fn send_message(
    producer: &FutureProducer,
    topic: &str,
    partition: i32,
    message: &rdkafka::message::BorrowedMessage<'_>,
    timeout: Duration,
) -> Result<()> {
    let mut record = FutureRecord::<[u8], [u8]>::to(topic).partition(partition);
    if let Some(key) = message.key() {
        record = record.key(key);
    }
    if let Some(payload) = message.payload() {
        record = record.payload(payload);
    }
    if let Some(timestamp) = timestamp(message.timestamp()) {
        record = record.timestamp(timestamp);
    }
    if let Some(headers) = message.headers() {
        let mut owned = OwnedHeaders::new_with_capacity(headers.count());
        for index in 0..headers.count() {
            let header = headers.get(index);
            owned = owned.insert(Header {
                key: header.key,
                value: header.value,
            });
        }
        record = record.headers(owned);
    }
    producer
        .send(record, timeout)
        .await
        .map_err(|(error, _)| error)?;
    Ok(())
}

fn timestamp(timestamp: Timestamp) -> Option<i64> {
    match timestamp {
        Timestamp::NotAvailable => None,
        Timestamp::CreateTime(value) | Timestamp::LogAppendTime(value) => Some(value),
    }
}

fn offsets_for_topic(topic: &str, offsets: &[RecoveryOffset]) -> Result<Vec<RecoveryOffset>> {
    let offsets = offsets
        .iter()
        .filter(|offset| offset.topic == topic)
        .cloned()
        .collect::<Vec<_>>();
    if offsets.is_empty() {
        bail!("offset vector has no entries for {topic}")
    }
    Ok(offsets)
}

fn validate_range(start: u64, target: u64, low: i64, high: i64) -> Result<()> {
    if low < 0 || high < low {
        bail!("Kafka returned invalid watermarks for the source partition")
    }
    let start = i64::try_from(start).context("source offset exceeds i64")?;
    let target = i64::try_from(target).context("target offset exceeds i64")?;
    if start < low {
        bail!("Kafka retention removed records required for replay")
    }
    if target > high {
        bail!("Kafka log end moved behind the requested replay target")
    }
    Ok(())
}

pub fn kafka_identity(prefix: &str, identity: &str, topic: &str) -> Result<String> {
    let compact_identity = identity.replace('-', "");
    let direct = format!("{prefix}-{compact_identity}-{topic}");
    if direct.len() <= 249
        && topic
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '-')
    {
        return Ok(direct);
    }
    let topic_hash = topic.bytes().fold(2_166_136_261_u32, |hash, byte| {
        (hash ^ u32::from(byte)).wrapping_mul(16_777_619)
    });
    let value = format!("{prefix}-{compact_identity}-{topic_hash:08x}");
    if value.len() > 249 {
        bail!("derived Kafka identity is too long")
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_may_equal_log_start_or_end() {
        assert!(validate_range(10, 10, 10, 10).is_ok());
        assert!(validate_range(5, 10, 5, 10).is_ok());
    }

    #[test]
    fn target_rejects_retention_future_and_invalid_watermarks() {
        assert!(validate_range(10, 10, 11, 12).is_err());
        assert!(validate_range(0, 10, 0, 9).is_err());
        assert!(validate_range(0, 10, -1, 9).is_err());
        assert!(validate_range(0, 10, 10, 9).is_err());
    }

    #[test]
    fn derived_identities_are_stable_safe_and_topic_specific() {
        let one = kafka_identity("prefix", "a-b-c", "one.input").unwrap();
        let two = kafka_identity("prefix", "a-b-c", "two.input").unwrap();
        assert_eq!(one, kafka_identity("prefix", "a-b-c", "one.input").unwrap());
        assert_ne!(one, two);
        assert!(
            one.chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '-')
        );
    }

    #[test]
    fn replay_range_rejects_retention_and_truncation() {
        assert!(validate_range(10, 20, 10, 20).is_ok());
        assert!(validate_range(10, 20, 11, 20).is_err());
        assert!(validate_range(10, 20, 10, 19).is_err());
        assert!(validate_range(10, 20, -1, 20).is_err());
    }
}
