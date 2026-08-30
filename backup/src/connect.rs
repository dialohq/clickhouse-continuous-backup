use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::{Client, Method};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::time::{Instant, sleep};

use crate::{
    config::RuntimeTimeouts,
    model::{ConnectOffsets, KafkaOffset},
};

#[derive(Clone)]
pub struct Connect {
    client: Client,
    base_url: String,
    poll_interval: Duration,
}

#[derive(Deserialize)]
struct Status {
    connector: Component,
    tasks: Vec<Component>,
}

#[derive(Deserialize)]
struct Component {
    state: String,
}

impl Connect {
    pub fn new(base_url: String, timeouts: &RuntimeTimeouts) -> Result<Self> {
        Ok(Self {
            client: Client::builder()
                .connect_timeout(timeouts.connect_connect)
                .timeout(timeouts.connect_request)
                .build()?,
            base_url: base_url.trim_end_matches('/').to_owned(),
            poll_interval: timeouts.connect_poll,
        })
    }

    pub async fn require_running(&self, connector: &str) -> Result<()> {
        let status = self.status(connector).await?;
        if status.tasks.is_empty()
            || status.connector.state != "RUNNING"
            || status.tasks.iter().any(|task| task.state != "RUNNING")
        {
            bail!("connector must be fully running before backup: {connector}")
        }
        Ok(())
    }

    pub async fn require_stopped(&self, connector: &str, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let status = self.status(connector).await?;
            if status.connector.state != "STOPPED" {
                bail!("connector must be stopped before restoring offsets: {connector}")
            }
            if status.tasks.iter().all(|task| task.state == "STOPPED") {
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!("connector tasks did not stop: {connector}")
            }
            sleep(self.poll_interval).await;
        }
    }

    pub async fn wait_running(&self, connector: &str, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let status = self.status(connector).await?;
            if status.connector.state == "RUNNING"
                && !status.tasks.is_empty()
                && status.tasks.iter().all(|task| task.state == "RUNNING")
            {
                return Ok(());
            }
            if status.connector.state == "FAILED"
                || status.tasks.iter().any(|task| task.state == "FAILED")
            {
                bail!("connector failed while waiting to run: {connector}")
            }
            if Instant::now() >= deadline {
                bail!("connector did not become fully running: {connector}")
            }
            sleep(self.poll_interval).await;
        }
    }

    pub async fn pause(&self, connector: &str) -> Result<()> {
        self.request(Method::PUT, &format!("connectors/{connector}/pause"), None)
            .await?;
        Ok(())
    }

    pub async fn resume(&self, connector: &str) -> Result<()> {
        self.request(Method::PUT, &format!("connectors/{connector}/resume"), None)
            .await?;
        Ok(())
    }

    pub async fn stop(&self, connector: &str) -> Result<()> {
        self.request(Method::PUT, &format!("connectors/{connector}/stop"), None)
            .await?;
        Ok(())
    }

    pub async fn wait_paused(&self, connector: &str, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let status = self.status(connector).await?;
            if status.connector.state == "PAUSED"
                && status.tasks.iter().all(|task| task.state == "PAUSED")
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!("connector did not pause: {connector}")
            }
            sleep(self.poll_interval).await;
        }
    }

    pub async fn require_paused(&self, connector: &str) -> Result<()> {
        let status = self.status(connector).await?;
        if status.connector.state != "PAUSED"
            || status.tasks.iter().any(|task| task.state != "PAUSED")
        {
            bail!("connector moved while snapshotting: {connector}")
        }
        Ok(())
    }

    pub async fn offsets(&self, connector: &str) -> Result<Vec<KafkaOffset>> {
        let value = self
            .request(
                Method::GET,
                &format!("connectors/{connector}/offsets"),
                None,
            )
            .await?;
        let offsets: ConnectOffsets = serde_json::from_value(value)
            .with_context(|| format!("connector returned invalid offsets: {connector}"))?;
        validate_offsets(&offsets.offsets)
            .with_context(|| format!("connector returned invalid offsets: {connector}"))?;
        Ok(offsets.offsets)
    }

    pub async fn patch_offsets(&self, connector: &str, offsets: &[KafkaOffset]) -> Result<()> {
        self.request(
            Method::PATCH,
            &format!("connectors/{connector}/offsets"),
            Some(json!({"offsets": offsets})),
        )
        .await?;
        Ok(())
    }

    pub async fn config(&self, connector: &str) -> Result<Map<String, Value>> {
        let value = self
            .request(Method::GET, &format!("connectors/{connector}/config"), None)
            .await?;
        value
            .as_object()
            .cloned()
            .with_context(|| format!("connector returned an invalid config: {connector}"))
    }

    pub async fn create_stopped(
        &self,
        connector: &str,
        config: &Map<String, Value>,
    ) -> Result<bool> {
        let response = self
            .client
            .post(format!("{}/connectors", self.base_url))
            .json(&json!({
                "name": connector,
                "config": config,
                "initial_state": "STOPPED"
            }))
            .send()
            .await?;
        if response.status().is_success() {
            return Ok(true);
        }
        if response.status() == reqwest::StatusCode::CONFLICT {
            let existing = self.config(connector).await?;
            if equivalent_config(existing, config.clone()) {
                return Ok(false);
            }
            bail!("existing recovery connector has a different config: {connector}")
        }
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!("Kafka Connect returned {status}: {}", body.trim_end())
    }

    pub async fn wait_offsets(
        &self,
        connector: &str,
        expected: &[KafkaOffset],
        timeout: Duration,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if equal_offsets(&self.offsets(connector).await?, expected) {
                return Ok(());
            }
            let status = self.status(connector).await?;
            if status.connector.state == "FAILED"
                || status.tasks.iter().any(|task| task.state == "FAILED")
            {
                bail!("connector failed while replaying: {connector}")
            }
            if Instant::now() >= deadline {
                bail!("connector did not reach the expected offsets: {connector}")
            }
            sleep(self.poll_interval).await;
        }
    }

    async fn status(&self, connector: &str) -> Result<Status> {
        let value = self
            .request(Method::GET, &format!("connectors/{connector}/status"), None)
            .await?;
        serde_json::from_value(value)
            .with_context(|| format!("invalid connector status: {connector}"))
    }

    async fn request(&self, method: Method, path: &str, body: Option<Value>) -> Result<Value> {
        let request = self
            .client
            .request(method, format!("{}/{path}", self.base_url));
        let response = match body {
            Some(body) => request.json(&body),
            None => request,
        }
        .send()
        .await?
        .error_for_status()?;
        let bytes = response.bytes().await?;
        if bytes.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_slice(&bytes).context("Kafka Connect returned invalid JSON")
    }
}

fn equivalent_config(mut left: Map<String, Value>, mut right: Map<String, Value>) -> bool {
    left.remove("name");
    right.remove("name");
    left == right
}

fn equal_offsets(left: &[KafkaOffset], right: &[KafkaOffset]) -> bool {
    let mut left = left.to_vec();
    let mut right = right.to_vec();
    let key = |offset: &KafkaOffset| {
        (
            offset.partition.kafka_topic.clone(),
            offset.partition.kafka_partition,
            offset.offset.kafka_offset,
        )
    };
    left.sort_by_key(&key);
    right.sort_by_key(key);
    left == right
}

pub fn validate_offsets(offsets: &[KafkaOffset]) -> Result<()> {
    let mut partitions = offsets
        .iter()
        .map(|offset| {
            (
                &offset.partition.kafka_topic,
                offset.partition.kafka_partition,
            )
        })
        .collect::<Vec<_>>();
    partitions.sort_unstable();
    if partitions.windows(2).any(|pair| pair[0] == pair[1]) {
        bail!("duplicate topic partition")
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connector_assigned_name_does_not_change_config_identity() {
        let desired = serde_json::from_value(json!({
            "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
            "topics": "recovery-topic"
        }))
        .unwrap();
        let existing = serde_json::from_value(json!({
            "name": "recovery-connector",
            "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
            "topics": "recovery-topic"
        }))
        .unwrap();

        assert!(equivalent_config(existing, desired));
    }

    #[test]
    fn material_config_difference_is_not_equivalent() {
        let left = serde_json::from_value(json!({"topics": "one"})).unwrap();
        let right = serde_json::from_value(json!({"topics": "two"})).unwrap();

        assert!(!equivalent_config(left, right));
    }
}
