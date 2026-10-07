mod handlers;
mod offsets;
mod registry;

use std::{path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use axum::{
    Router,
    http::StatusCode,
    routing::{get, post},
};
use tokio::{net::TcpListener, sync::Mutex};

use crate::{clickhouse::ClickHouse, config::PauseServerConfig, connect::Connect, model::Pipeline};
use registry::{Pauses, lock_pauses_file};

#[derive(Clone)]
struct AppState {
    connect: Connect,
    clickhouse: ClickHouse,
    pipelines: Arc<[Pipeline]>,
    pause_timeout: Duration,
    /// Held for the whole of each `/pause` and `/resume` request, so they run one at a time.
    pauses: Arc<Mutex<Pauses>>,
    pauses_file: Arc<Path>,
}

pub async fn run(config: &PauseServerConfig) -> Result<()> {
    // Kept until the server stops; the OS releases it if the process dies.
    let _lock = lock_pauses_file(&config.pauses_file)?;
    let pauses = Pauses::load(&config.pauses_file)?;
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
        pauses: Arc::new(Mutex::new(pauses)),
        pauses_file: config.pauses_file.as_path().into(),
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/pause", post(handlers::pause))
        .route("/resume", post(handlers::resume))
        .with_state(state);
    let listener = TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("failed to bind pause server to {}", config.listen))?;
    axum::serve(listener, app)
        .await
        .context("pause server failed")
}

async fn health() -> StatusCode {
    StatusCode::OK
}
