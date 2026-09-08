use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::Client;
use serde_json::Value;
use tokio::time::{Instant, sleep};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorState {
    Running,
    Paused,
}

impl ConnectorState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Running => "RUNNING",
            Self::Paused => "PAUSED",
        }
    }
}

pub struct ConnectClient {
    base_url: String,
    client: Client,
}

impl ConnectClient {
    pub fn new(base_url: String) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_owned(),
            client: Client::new(),
        }
    }

    pub async fn pause(&self, connector: &str) -> Result<()> {
        self.action(connector, "pause").await
    }

    pub async fn resume(&self, connector: &str) -> Result<()> {
        self.action(connector, "resume").await
    }

    pub async fn wait_for_state(
        &self,
        connector: &str,
        expected: ConnectorState,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if self
                .is_fully_in_state(connector, expected)
                .await
                .unwrap_or(false)
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!("connector {connector} did not become {}", expected.as_str());
            }
            sleep(Duration::from_millis(250)).await;
        }
    }

    async fn action(&self, connector: &str, action: &str) -> Result<()> {
        self.client
            .put(format!("{}/connectors/{connector}/{action}", self.base_url))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    pub async fn is_fully_in_state(
        &self,
        connector: &str,
        expected: ConnectorState,
    ) -> Result<bool> {
        let status: Value = self
            .client
            .get(format!("{}/connectors/{connector}/status", self.base_url))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("decode Kafka Connect status")?;
        let expected = expected.as_str();
        let connector_matches =
            status.pointer("/connector/state").and_then(Value::as_str) == Some(expected);
        let tasks = status
            .get("tasks")
            .and_then(Value::as_array)
            .context("connector status has no tasks")?;
        Ok(connector_matches
            && !tasks.is_empty()
            && tasks
                .iter()
                .all(|task| task.get("state").and_then(Value::as_str) == Some(expected)))
    }
}
