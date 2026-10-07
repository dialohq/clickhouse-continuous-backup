use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use reqwest::{Client, Response, StatusCode};
use serde::Serialize;
use tokio::{task::AbortHandle, time::sleep};
use uuid::Uuid;

use super::api::{PauseRequest, PauseResponse, RenewRequest, ResumeRequest};
use crate::config::RuntimeTimeouts;

/// Calls the pause server and keeps every token it has not resumed yet renewed.
pub(crate) struct PauseClient {
    client: Client,
    base_url: Arc<str>,
    /// The renewal task of each token not resumed yet.
    outstanding: Mutex<BTreeMap<Uuid, AbortHandle>>,
}

impl PauseClient {
    /// `request_timeout` must cover the server waiting for connectors to pause or resume.
    pub(crate) fn new(
        base_url: String,
        timeouts: &RuntimeTimeouts,
        request_timeout: Duration,
    ) -> Result<Self> {
        Ok(Self {
            client: Client::builder()
                .connect_timeout(timeouts.connect_connect)
                .timeout(request_timeout)
                .build()?,
            base_url: base_url.trim_end_matches('/').into(),
            outstanding: Mutex::default(),
        })
    }

    pub(crate) async fn pause(&self, request: &PauseRequest) -> Result<PauseResponse> {
        let response: PauseResponse = post(&self.client, &self.base_url, "pause", request)
            .await?
            .json()
            .await
            .context("pause server returned an invalid /pause response")?;
        let renewal = tokio::spawn(renew_periodically(
            self.client.clone(),
            self.base_url.clone(),
            response.token,
            Duration::from_secs(response.ttl_seconds) / 3,
        ));
        self.outstanding()
            .insert(response.token, renewal.abort_handle());
        Ok(response)
    }

    pub(crate) async fn resume(&self, token: Uuid) -> Result<()> {
        post(
            &self.client,
            &self.base_url,
            "resume",
            &ResumeRequest { token },
        )
        .await?;
        if let Some(renewal) = self.outstanding().remove(&token) {
            renewal.abort();
        }
        Ok(())
    }

    /// Resumes every token not resumed yet, e.g. after the backup failed or was interrupted.
    pub(crate) async fn resume_outstanding(&self) -> Result<()> {
        let tokens = self.outstanding().keys().copied().collect::<Vec<_>>();
        let mut failures = Vec::new();
        for token in tokens {
            if let Err(error) = self.resume(token).await {
                failures.push(format!("{token}: {error:#}"));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            bail!("failed to resume pauses: {}", failures.join(", "))
        }
    }

    fn outstanding(&self) -> MutexGuard<'_, BTreeMap<Uuid, AbortHandle>> {
        self.outstanding
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for PauseClient {
    /// Stops renewing, so tokens left behind expire on the server.
    fn drop(&mut self) {
        for renewal in self.outstanding().values() {
            renewal.abort();
        }
    }
}

/// Renews the token until the server no longer knows it or the task is aborted.
async fn renew_periodically(client: Client, base_url: Arc<str>, token: Uuid, every: Duration) {
    loop {
        sleep(every).await;
        let result = client
            .post(format!("{base_url}/renew"))
            .json(&RenewRequest { token })
            .send()
            .await;
        match result {
            Ok(response) if response.status() == StatusCode::NOT_FOUND => {
                eprintln!("pause {token} is gone from the pause server, no longer renewing it");
                return;
            }
            Ok(response) if !response.status().is_success() => {
                eprintln!("failed to renew pause {token}: {}", response.status());
            }
            Ok(_) => {}
            Err(error) => eprintln!("failed to renew pause {token}: {error:#}"),
        }
    }
}

async fn post(
    client: &Client,
    base_url: &str,
    path: &str,
    body: &impl Serialize,
) -> Result<Response> {
    let response = client
        .post(format!("{base_url}/{path}"))
        .json(body)
        .send()
        .await
        .with_context(|| format!("pause server /{path} request failed"))?;
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let body = response.text().await.unwrap_or_default();
    bail!(
        "pause server /{path} returned {status}: {}",
        body.trim_end()
    )
}
