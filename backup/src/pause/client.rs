use std::{
    collections::BTreeSet,
    sync::{Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use reqwest::{Client, Response};
use serde::Serialize;
use uuid::Uuid;

use super::api::{PauseRequest, PauseResponse, ResumeRequest};
use crate::config::RuntimeTimeouts;

/// Calls the pause server and remembers the tokens it has not resumed yet.
pub(crate) struct PauseClient {
    client: Client,
    base_url: String,
    outstanding: Mutex<BTreeSet<Uuid>>,
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
            base_url: base_url.trim_end_matches('/').to_owned(),
            outstanding: Mutex::default(),
        })
    }

    pub(crate) async fn pause(&self, request: &PauseRequest) -> Result<PauseResponse> {
        let response: PauseResponse = self
            .post("pause", request)
            .await?
            .json()
            .await
            .context("pause server returned an invalid /pause response")?;
        self.outstanding().insert(response.token);
        Ok(response)
    }

    pub(crate) async fn resume(&self, token: Uuid) -> Result<()> {
        self.post("resume", &ResumeRequest { token }).await?;
        self.outstanding().remove(&token);
        Ok(())
    }

    /// Resumes every token not resumed yet, e.g. after the backup failed or was interrupted.
    pub(crate) async fn resume_outstanding(&self) -> Result<()> {
        let tokens = self.outstanding().iter().copied().collect::<Vec<_>>();
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

    async fn post(&self, path: &str, body: &impl Serialize) -> Result<Response> {
        let response = self
            .client
            .post(format!("{}/{path}", self.base_url))
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

    fn outstanding(&self) -> MutexGuard<'_, BTreeSet<Uuid>> {
        self.outstanding
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}
