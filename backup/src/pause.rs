mod api;
mod client;
mod handlers;
mod labels;
mod leader;
mod offsets;
mod registry;

pub(crate) use api::{PauseRequest, PipelineOffsets};
pub(crate) use client::PauseClient;

use std::{
    sync::{Arc, PoisonError, RwLock},
    time::Duration,
};

use anyhow::{Context, Result};
use axum::{
    Router,
    http::StatusCode,
    routing::{get, post},
};
use tokio::{
    net::TcpListener,
    signal::unix::{SignalKind, signal},
    sync::Mutex,
};
use tokio_util::sync::CancellationToken;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::{Level, info};

use crate::{clickhouse::ClickHouse, config::PauseServerConfig, connect::Connect, model::Pipeline};
use labels::PodLabels;
use registry::TokenStore;

#[derive(Clone)]
struct AppState {
    connect: Connect,
    clickhouse: ClickHouse,
    pipelines: Arc<[Pipeline]>,
    pause_timeout: Duration,
    ttl: Duration,
    term: Arc<RwLock<Option<Arc<Term>>>>,
}

impl AppState {
    fn current_term(&self) -> Option<Arc<Term>> {
        self.term
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn set_term(&self, term: Option<Arc<Term>>) {
        *self.term.write().unwrap_or_else(PoisonError::into_inner) = term;
    }
}

struct Term {
    store: Mutex<TokenStore>,
    ended: CancellationToken,
}

pub async fn run(config: &PauseServerConfig) -> Result<()> {
    let state = AppState {
        connect: Connect::new(config.connect_url.clone(), &config.timeouts)?,
        clickhouse: ClickHouse::new(
            config.clickhouse_url.clone(),
            config.clickhouse_username.clone(),
            config.clickhouse_password.clone(),
            &config.timeouts,
        )?,
        pipelines: config.pipelines.clone().into(),
        pause_timeout: config.pause_timeout,
        ttl: config.pause_ttl,
        term: Arc::default(),
    };
    let labels = PodLabels::from_env().await?;
    match &labels {
        Some(labels) => info!(pod = labels.pod(), "routing to the leader by pod label"),
        None => info!("POD_NAME is not set, not labelling pods"),
    }

    let shutdown = CancellationToken::new();
    let mut terminate = signal(SignalKind::terminate()).context("failed to listen for SIGTERM")?;
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            tokio::select! {
                _ = terminate.recv() => info!("received SIGTERM, shutting down"),
                _ = tokio::signal::ctrl_c() => info!("received Ctrl-C, shutting down"),
            }
            shutdown.cancel();
        }
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/pause", post(handlers::pause))
        .route("/resume", post(handlers::resume))
        .route("/renew", post(handlers::renew))
        .with_state(state.clone())
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        );
    let listener = TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("failed to bind pause server to {}", config.listen))?;
    info!(
        listen = %config.listen,
        ttl_seconds = config.pause_ttl.as_secs(),
        pipelines = config.pipelines.len(),
        "pause server listening"
    );
    let serving = async {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown.clone().cancelled_owned())
            .await
            .context("pause server failed")
    };
    let leading = leader::lead(&state, config, labels.as_ref(), &shutdown);
    let (served, ()) = tokio::join!(serving, leading);
    served
}

async fn health() -> StatusCode {
    StatusCode::OK
}
