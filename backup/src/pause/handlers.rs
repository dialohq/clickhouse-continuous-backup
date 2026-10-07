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
use tokio::time::sleep;
use tracing::{Span, debug, error, field, info, instrument, warn};
use uuid::Uuid;

use super::{
    AppState,
    api::{PauseRequest, PauseResponse, RenewRequest, ResumeRequest},
    offsets::read_offsets,
    registry::{Pause, Pauses},
};

#[instrument(skip_all, fields(request = ?request, token = field::Empty))]
pub(super) async fn pause(
    State(state): State<AppState>,
    Json(request): Json<PauseRequest>,
) -> Response {
    let mut pauses = state.pauses.lock().await;
    debug!("acquired the pause lock");
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
            held = pauses.is_held(connector),
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
        .find(|(connector, status)| !status.is_running() && !pauses.is_held(connector))
    {
        return reject(
            StatusCode::CONFLICT,
            format!("connector is not running and was not paused by this server: {connector}"),
        );
    }

    // Register the pause before touching Connect, so every connector it pauses is accounted for.
    // It holds all of them, including ones another token already holds, so releasing that token
    // can't resume them under this one.
    // The expiry also covers a crash during this request: the reloaded token lapses on its own.
    let token = Uuid::new_v4();
    Span::current().record("token", field::display(token));
    pauses.hold(
        token,
        Pause {
            connectors: connectors
                .iter()
                .map(|&connector| connector.to_owned())
                .collect(),
            expires_at: expiry(state.ttl),
        },
    );
    if let Err(error) = pauses.save(&state.pauses_file) {
        pauses.release(&token);
        return reject(StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}"));
    }
    info!("registered pause");
    // Started now rather than on success: if the client disconnects, axum drops this handler
    // midway and the expiry is what resumes whatever it already paused. It can't act before this
    // request releases the lock.
    let expiry_task = tokio::spawn(expire_when_due(state.clone(), token));
    pauses.track_expiry(token, expiry_task.abort_handle());

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
            let expires_at = expiry(state.ttl);
            pauses.renew(&token, expires_at);
            if let Err(error) = pauses.save(&state.pauses_file) {
                error!("failed to save the expiry of the pause: {error:#}");
            }
            info!(%expires_at, "paused");
            Json(PauseResponse {
                token,
                ttl_seconds: state.ttl.as_secs(),
                watermark,
            })
            .into_response()
        }
        Err(error) => {
            let released = pauses.held_only_by(&token);
            if let Err(error) = forget(&state, &mut pauses, &token) {
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
    let mut pauses = state.pauses.lock().await;
    debug!("acquired the pause lock");
    let Some(connectors) = pauses
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
    if let Err(error) = resume_held_connectors(&state, &pauses, &request.token).await {
        return reject(StatusCode::BAD_GATEWAY, format!("{error:#}"));
    }
    if let Err(error) = forget(&state, &mut pauses, &request.token) {
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
    let mut pauses = state.pauses.lock().await;
    let Some(previous) = pauses.get(&request.token).map(|pause| pause.expires_at) else {
        return reject(StatusCode::NOT_FOUND, "unknown token".to_owned());
    };
    let expires_at = expiry(state.ttl);
    pauses.renew(&request.token, expires_at);
    if let Err(error) = pauses.save(&state.pauses_file) {
        pauses.renew(&request.token, previous);
        return reject(StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}"));
    }
    debug!(%expires_at, "renewed");
    StatusCode::OK.into_response()
}

/// Waits until the token expires, then resumes its connectors as `/resume` would.
///
/// A `/renew` moves the expiry while this sleeps, so it checks again under the lock before acting.
#[instrument(skip(state))]
pub(super) async fn expire_when_due(state: AppState, token: Uuid) {
    loop {
        let Some(expires_at) = state
            .pauses
            .lock()
            .await
            .get(&token)
            .map(|pause| pause.expires_at)
        else {
            return;
        };
        debug!(%expires_at, "waiting for expiry");
        sleep((expires_at - Utc::now()).to_std().unwrap_or_default()).await;

        let mut pauses = state.pauses.lock().await;
        match pauses.get(&token) {
            None => {
                debug!("pause is gone, nothing to expire");
                return;
            }
            Some(pause) if pause.expires_at > Utc::now() => {
                debug!("pause was renewed");
                continue;
            }
            Some(_) => {}
        }
        warn!("pause expired without being resumed or renewed, resuming its connectors");
        let result = match resume_held_connectors(&state, &pauses, &token).await {
            Ok(()) => forget(&state, &mut pauses, &token),
            Err(error) => Err(error),
        };
        match result {
            Ok(()) => {
                info!("resumed expired pause");
                return;
            }
            Err(error) => {
                let retry_at = expiry(state.ttl / 3);
                error!(%retry_at, "failed to resume expired pause: {error:#}");
                pauses.renew(&token, retry_at);
            }
        }
    }
}

/// Resumes the token's connectors that no other token holds and waits for them to run.
async fn resume_held_connectors(state: &AppState, pauses: &Pauses, token: &Uuid) -> Result<()> {
    try_join_all(
        pauses
            .held_only_by(token)
            .iter()
            .map(|connector| resume_connector(state, connector)),
    )
    .await?;
    Ok(())
}

/// Removes the token, saves the file and stops the token's expiry task.
///
/// On a failed save the token is kept, as the file still has it, and so is its expiry task.
/// Called from the expiry task itself it aborts that task too, which then returns without awaiting.
fn forget(state: &AppState, pauses: &mut Pauses, token: &Uuid) -> Result<()> {
    let Some(pause) = pauses.get(token).cloned() else {
        return Ok(());
    };
    pauses.release(token);
    if let Err(error) = pauses.save(&state.pauses_file) {
        pauses.hold(*token, pause);
        return Err(error);
    }
    pauses.stop_expiry(token);
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
