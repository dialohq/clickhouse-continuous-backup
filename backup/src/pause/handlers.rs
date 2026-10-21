use std::collections::BTreeSet;

use anyhow::{Context, Result};
use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use chrono::{DateTime, TimeDelta, Utc};
use futures::future::{join_all, try_join_all};
use tokio::time::{MissedTickBehavior, interval};
use tracing::{Span, debug, error, field, info, instrument, warn};
use uuid::Uuid;

use super::{
    AppState,
    api::{PauseRequest, PauseResponse, RenewRequest, ResumeRequest},
    offsets::read_offsets,
    registry::{Pause, TokenStore},
};

/// How often expired pauses are resumed.
const SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

#[instrument(skip_all, fields(request = ?request, token = field::Empty))]
pub(super) async fn pause(
    State(state): State<AppState>,
    Json(request): Json<PauseRequest>,
) -> Response {
    let store = state.store.lock().await;
    debug!("acquired the pause lock");
    let snapshot = match store.read().await {
        Ok(snapshot) => snapshot,
        Err(error) => return reject(StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")),
    };
    let pipelines = state
        .pipelines
        .iter()
        .filter(|pipeline| request.matches(pipeline))
        .collect::<Vec<_>>();
    if pipelines.is_empty() {
        return reject(
            StatusCode::NOT_FOUND,
            "no pipeline matches the request".to_owned(),
        );
    }
    let connectors = pipelines
        .iter()
        .map(|pipeline| pipeline.connector.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    info!(?connectors, "pausing pipelines");

    let statuses = match try_join_all(
        connectors
            .iter()
            .map(|connector| state.connect.status(connector)),
    )
    .await
    {
        Ok(statuses) => statuses,
        Err(error) => return reject(StatusCode::BAD_GATEWAY, format!("{error:#}")),
    };
    for (connector, status) in connectors.iter().zip(&statuses) {
        debug!(
            connector,
            running = status.is_running(),
            paused = status.is_paused(),
            failed = status.is_failed(),
            held = snapshot.is_held(connector),
            "connector status"
        );
    }
    if let Some((connector, _)) = connectors
        .iter()
        .zip(&statuses)
        .find(|(_, status)| status.is_failed())
    {
        return reject(
            StatusCode::CONFLICT,
            format!("connector has failed: {connector}"),
        );
    }
    // A connector we don't hold must be running: if it's paused or stopped, someone outside this
    // server did it, and a later resume from here would undo their decision.
    if let Some((connector, _)) = connectors
        .iter()
        .zip(&statuses)
        .find(|(connector, status)| !status.is_running() && !snapshot.is_held(connector))
    {
        return reject(
            StatusCode::CONFLICT,
            format!("connector is not running and was not paused by this server: {connector}"),
        );
    }

    // Register the pause before touching Connect, so every connector it pauses is accounted for.
    // It holds all of them, including ones another token already holds, so releasing that token
    // can't resume them under this one.
    // Its expiry also covers this request being dropped or the server dying midway: the token
    // lapses on its own and a later sweep resumes whatever it paused.
    let token = Uuid::new_v4();
    Span::current().record("token", field::display(token));
    let pause = Pause {
        connectors: connectors
            .iter()
            .map(|&connector| connector.to_owned())
            .collect(),
        expires_at: expiry(state.ttl),
    };
    if let Err(error) = store.save(&token, &pause).await {
        return reject(StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}"));
    }
    info!("registered pause");

    let running = connectors
        .iter()
        .zip(&statuses)
        .filter(|(_, status)| !status.is_paused())
        .map(|(connector, _)| *connector)
        .collect::<Vec<_>>();
    debug!(?running, "connectors that need pausing");

    let result = async {
        try_join_all(
            running
                .iter()
                .map(|connector| pause_connector(&state, connector)),
        )
        .await?;
        try_join_all(
            pipelines
                .iter()
                .map(|pipeline| read_offsets(&state.connect, &state.clickhouse, pipeline)),
        )
        .await
    }
    .await;
    match result {
        Ok(watermark) => {
            // The TTL counts from the response, not from the start of the request.
            let pause = Pause {
                expires_at: expiry(state.ttl),
                ..pause
            };
            if let Err(error) = store.save(&token, &pause).await {
                error!("failed to save the expiry of the pause: {error:#}");
            }
            info!(expires_at = %pause.expires_at, "paused");
            Json(PauseResponse {
                token,
                ttl_seconds: state.ttl.as_secs(),
                watermark,
            })
            .into_response()
        }
        Err(error) => {
            // The ones no other token held, which this request is the one to resume.
            let released = connectors
                .iter()
                .filter(|connector| !snapshot.is_held(connector))
                .collect::<Vec<_>>();
            if let Err(error) = store.remove(&token).await {
                error!("failed to remove the pause after a failed /pause: {error:#}");
            }
            // Best effort: a failure cancels the other pauses, any of which may already be paused.
            warn!(?released, "resuming connectors after a failed /pause");
            let resumes = join_all(
                released
                    .iter()
                    .map(|connector| state.connect.resume(connector)),
            )
            .await;
            for (connector, resume) in released.iter().zip(resumes) {
                if let Err(error) = resume {
                    error!(connector, "best-effort resume failed: {error:#}");
                }
            }
            reject(StatusCode::BAD_GATEWAY, format!("{error:#}"))
        }
    }
}

#[instrument(skip_all, fields(token = %request.token))]
pub(super) async fn resume(
    State(state): State<AppState>,
    Json(request): Json<ResumeRequest>,
) -> Response {
    let store = state.store.lock().await;
    debug!("acquired the pause lock");
    let snapshot = match store.read().await {
        Ok(snapshot) => snapshot,
        Err(error) => return reject(StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")),
    };
    let Some(connectors) = snapshot
        .get(&request.token)
        .map(|pause| pause.connectors.clone())
    else {
        return reject(StatusCode::NOT_FOUND, "unknown token".to_owned());
    };
    info!(?connectors, "resuming pause");
    let statuses = match try_join_all(
        connectors
            .iter()
            .map(|connector| state.connect.status(connector)),
    )
    .await
    {
        Ok(statuses) => statuses,
        Err(error) => return reject(StatusCode::BAD_GATEWAY, format!("{error:#}")),
    };
    if let Some((connector, _)) = connectors
        .iter()
        .zip(&statuses)
        .find(|(_, status)| status.is_failed())
    {
        return reject(
            StatusCode::CONFLICT,
            format!("connector has failed: {connector}"),
        );
    }

    // The token is only removed once its connectors are running again, so a failed resume can be
    // retried with the same token.
    if let Err(error) =
        resume_connectors(&state, &snapshot.held_only_by(&request.token, Utc::now())).await
    {
        return reject(StatusCode::BAD_GATEWAY, format!("{error:#}"));
    }
    if let Err(error) = store.remove(&request.token).await {
        return reject(StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}"));
    }
    info!("resumed");
    StatusCode::OK.into_response()
}

#[instrument(skip_all, fields(token = %request.token))]
pub(super) async fn renew(
    State(state): State<AppState>,
    Json(request): Json<RenewRequest>,
) -> Response {
    let store = state.store.lock().await;
    let snapshot = match store.read().await {
        Ok(snapshot) => snapshot,
        Err(error) => return reject(StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")),
    };
    let Some(pause) = snapshot.get(&request.token) else {
        return reject(StatusCode::NOT_FOUND, "unknown token".to_owned());
    };
    // Once expired, the sweep resumes it, so it can't be brought back.
    if pause.expires_at <= Utc::now() {
        return reject(StatusCode::NOT_FOUND, "token expired".to_owned());
    }
    let expires_at = expiry(state.ttl);
    let renewed = Pause {
        connectors: pause.connectors.clone(),
        expires_at,
    };
    if let Err(error) = store.save(&request.token, &renewed).await {
        return reject(StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}"));
    }
    debug!(%expires_at, "renewed");
    StatusCode::OK.into_response()
}

/// Reads every pause from Keeper, resumes the expired ones and removes them.
///
/// Returns how many pauses are left. An expired pause that can't be resumed or removed stays in
/// Keeper, so its connectors stay held, and the next sweep tries again.
pub(super) async fn sweep(state: &AppState, store: &TokenStore) -> Result<usize> {
    let snapshot = store.read().await?;
    let now = Utc::now();
    let mut left = snapshot.len();
    for token in snapshot.expired(now) {
        let connectors = snapshot.held_only_by(&token, now);
        warn!(%token, ?connectors, "pause expired without being resumed or renewed, resuming it");
        let result = async {
            resume_connectors(state, &connectors).await?;
            store.remove(&token).await
        }
        .await;
        match result {
            Ok(()) => {
                left -= 1;
                info!(%token, "resumed expired pause");
            }
            Err(error) => error!(%token, "failed to resume expired pause, retrying: {error:#}"),
        }
    }
    Ok(left)
}

/// Sweeps every `SWEEP_INTERVAL`; requests leave expired pauses to it.
pub(super) async fn sweep_periodically(state: AppState) {
    let mut ticks = interval(SWEEP_INTERVAL);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        let store = state.store.lock().await;
        if let Err(error) = sweep(&state, &store).await {
            error!("failed to sweep pauses: {error:#}");
        }
    }
}

/// Resumes the connectors and waits for them to run.
async fn resume_connectors(state: &AppState, connectors: &[String]) -> Result<()> {
    try_join_all(
        connectors
            .iter()
            .map(|connector| resume_connector(state, connector)),
    )
    .await?;
    Ok(())
}

/// Logs the failure and turns it into the response the client sees.
fn reject(status: StatusCode, message: String) -> Response {
    if status.is_server_error() {
        error!(%status, "{message}");
    } else {
        warn!(%status, "{message}");
    }
    (status, message).into_response()
}

fn expiry(ttl: std::time::Duration) -> DateTime<Utc> {
    TimeDelta::from_std(ttl)
        .ok()
        .and_then(|ttl| Utc::now().checked_add_signed(ttl))
        .unwrap_or(DateTime::<Utc>::MAX_UTC)
}

#[instrument(skip(state))]
async fn resume_connector(state: &AppState, connector: &str) -> Result<()> {
    debug!("resuming connector");
    state
        .connect
        .resume(connector)
        .await
        .with_context(|| format!("failed to resume {connector}"))?;
    state
        .connect
        .wait_running(connector, state.pause_timeout)
        .await?;
    info!("connector is running");
    Ok(())
}

#[instrument(skip(state))]
async fn pause_connector(state: &AppState, connector: &str) -> Result<()> {
    debug!("pausing connector");
    state
        .connect
        .pause(connector)
        .await
        .with_context(|| format!("failed to pause {connector}"))?;
    state
        .connect
        .wait_paused(connector, state.pause_timeout)
        .await?;
    info!("connector is paused");
    Ok(())
}
