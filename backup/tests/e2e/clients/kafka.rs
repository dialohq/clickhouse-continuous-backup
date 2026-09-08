use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use rdkafka::{
    ClientConfig, Message, Offset, TopicPartitionList,
    admin::{
        AdminClient, AdminOptions, AlterConfig, NewTopic, ResourceSpecifier, TopicReplication,
    },
    client::DefaultClientContext,
    consumer::{Consumer, StreamConsumer},
    error::RDKafkaErrorCode,
    producer::{FutureProducer, FutureRecord},
};

#[derive(Clone)]
pub struct KafkaClient {
    producer: FutureProducer,
    bootstrap_servers: String,
}

impl KafkaClient {
    pub fn new(bootstrap_servers: String) -> Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("enable.idempotence", "true")
            .set("message.timeout.ms", "30000")
            .create()
            .context("create Kafka producer")?;
        Ok(Self {
            producer,
            bootstrap_servers,
        })
    }

    pub async fn create_catalog(&self, topic: &str) -> Result<()> {
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .create()?;
        let results = admin
            .create_topics(
                &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))
                    .set("cleanup.policy", "compact")
                    .set("retention.ms", "-1")
                    .set("retention.bytes", "-1")],
                &AdminOptions::new().request_timeout(Some(Duration::from_secs(15))),
            )
            .await?;
        for result in results {
            match result {
                Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
                Err((topic, error)) => bail!("create catalog {topic}: {error}"),
            }
        }
        Ok(())
    }

    pub async fn read_json_key(
        &self,
        topic: &str,
        key: &str,
        timeout: Duration,
    ) -> Result<serde_json::Value> {
        Ok(self.read_json_key_with_offset(topic, key, timeout).await?.1)
    }

    pub async fn read_json_key_with_offset(
        &self,
        topic: &str,
        key: &str,
        timeout: Duration,
    ) -> Result<(i64, serde_json::Value)> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("group.id", "e2e-catalog-reader")
            .set("enable.auto.commit", "false")
            .set("isolation.level", "read_committed")
            .create()?;
        let mut assignment = TopicPartitionList::new();
        assignment.add_partition_offset(topic, 0, Offset::Beginning)?;
        consumer.assign(&assignment)?;
        tokio::time::timeout(timeout, async {
            loop {
                let message = consumer.recv().await?;
                if message.key() == Some(key.as_bytes()) {
                    let value = serde_json::from_slice(
                        message.payload().context("catalog record has no payload")?,
                    )?;
                    return Ok((message.offset(), value));
                }
            }
        })
        .await
        .with_context(|| format!("timed out reading catalog key {key}"))?
    }

    pub async fn delete_catalog_prefix(&self, topic: &str, before: i64) -> Result<()> {
        ensure!(before > 0, "catalog prefix boundary must be positive");
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .create()?;
        let options = AdminOptions::new().request_timeout(Some(Duration::from_secs(15)));
        // Redpanda requires delete in cleanup.policy for DeleteRecords. Only
        // this test's catalog is changed; retain unlimited time/size retention.
        let config = AlterConfig::new(ResourceSpecifier::Topic(topic))
            .set("cleanup.policy", "compact,delete")
            .set("retention.ms", "-1")
            .set("retention.bytes", "-1");
        for result in admin.alter_configs(&[config], &options).await? {
            result
                .map_err(|(resource, error)| anyhow::anyhow!("configure {resource:?}: {error}"))?;
        }
        let mut offsets = TopicPartitionList::new();
        offsets.add_partition_offset(topic, 0, Offset::Offset(before))?;
        let result = admin.delete_records(&offsets, &options).await?;
        let partition = result
            .find_partition(topic, 0)
            .context("missing prefix deletion result")?;
        partition.error()?;
        ensure!(
            partition.offset() == Offset::Offset(before),
            "unexpected catalog low watermark"
        );
        Ok(())
    }

    pub async fn produce_json(&self, topic: &str, records: &[(String, String)]) -> Result<()> {
        for (key, value) in records {
            self.producer
                .send(
                    FutureRecord::to(topic).key(key).payload(value),
                    Duration::from_secs(30),
                )
                .await
                .map_err(|(error, _)| error)
                .with_context(|| format!("deliver Kafka record {key:?} to {topic}"))?;
        }
        Ok(())
    }
}
