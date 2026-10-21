mod api;
mod client;
mod handlers;
mod offsets;
mod registry;

pub(crate) use api::{PauseRequest, PipelineOffsets};
pub(crate) use client::PauseClient;

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use axum::{
    Router,
    http::StatusCode,
    routing::{get, post},
};
use tokio::{net::TcpListener, sync::Mutex};
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::{Level, error, info};

use crate::{clickhouse::ClickHouse, config::PauseServerConfig, connect::Connect, model::Pipeline};
use registry::TokenStore;

#[derive(Clone)]
struct AppState {
    connect: Connect,
    clickhouse: ClickHouse,
    pipelines: Arc<[Pipeline]>,
    pause_timeout: Duration,
    ttl: Duration,
    /// Held for the whole of each request and sweep, so they run one at a time.
    store: Arc<Mutex<TokenStore>>,
}

pub async fn run(config: &PauseServerConfig) -> Result<()> {
    let store = TokenStore::connect(&config.keeper_address, &config.keeper_path).await?;
    store
        .lock()
        .await
        .inspect_err(|error| error!("cannot start the pause server: {error:#}"))?;
    info!(address = %config.keeper_address, lock = %store.lock_path(), "acquired the Keeper lock");
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
        store: Arc::new(Mutex::new(store.clone())),
    };
    // Clean up what expired while no server was running before taking requests.
    let tokens = handlers::sweep(&state, &store).await?;
    info!(
        tokens,
        ttl_seconds = config.pause_ttl.as_secs(),
        pipelines = config.pipelines.len(),
        "loaded pauses"
    );
    tokio::spawn(handlers::sweep_periodically(state.clone()));
    let app = Router::new()
        .route("/health", get(health))
        .route("/pause", post(handlers::pause))
        .route("/resume", post(handlers::resume))
        .route("/renew", post(handlers::renew))
        .with_state(state)
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        );
    let listener = TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("failed to bind pause server to {}", config.listen))?;
    info!(listen = %config.listen, "pause server listening");
    // Without the session the lock is gone and another server may take over, so this one stops.
    tokio::select! {
        result = axum::serve(listener, app) => result.context("pause server failed"),
        state = store.session_ended() => {
            error!(%state, "lost the Keeper session and with it the lock");
            bail!("lost the Keeper session ({state}), so another pause server may hold the lock")
        }
    }
}

async fn health() -> StatusCode {
    StatusCode::OK
}
