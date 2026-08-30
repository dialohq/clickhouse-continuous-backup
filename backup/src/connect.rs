use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::{Client, Method};
use serde::Deserialize;
use serde_json::{Value, json};
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
