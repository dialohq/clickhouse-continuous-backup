use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::Client;
use tokio::time::{Instant, sleep};

pub struct ClickHouseClient {
    url: String,
    client: Client,
}

impl ClickHouseClient {
    pub fn new(url: String) -> Result<Self> {
        Ok(Self {
            url,
            client: Client::builder().timeout(Duration::from_secs(10)).build()?,
        })
    }

    pub async fn query(&self, query: &str) -> Result<String> {
        self.query_with_timeout(query, Duration::from_secs(10))
            .await
    }

    pub async fn query_with_timeout(&self, query: &str, timeout: Duration) -> Result<String> {
        let response = self
            .client
            .post(&self.url)
            .timeout(timeout)
            .basic_auth("default", Some(""))
            .body(query.to_owned())
            .send()
            .await
            .context("send ClickHouse query")?;
        let status = response.status();
        let body = response.text().await.context("read ClickHouse response")?;
        if !status.is_success() {
            bail!("ClickHouse query failed ({status}): {}", body.trim());
        }
        Ok(body.trim().to_owned())
    }

    pub async fn wait_for_u64(&self, query: &str, expected: u64, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.query(query).await.ok().and_then(|v| v.parse().ok()) == Some(expected) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!("timed out waiting for {expected} from ClickHouse query: {query}");
            }
            sleep(Duration::from_millis(250)).await;
        }
    }
}
