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
use uuid::Uuid;

use super::{
    AppState,
    api::{PauseRequest, PauseResponse, RenewRequest, ResumeRequest},
    offsets::read_offsets,
    registry::{Pause, Pauses},
};

pub(super) async fn pause(
    State(state): State<AppState>,
    Json(request): Json<PauseRequest>,
) -> Response {
    let mut pauses = state.pauses.lock().await;
    let pipelines = state
        .pipelines
        .iter()
        .filter(|pipeline| request.matches(pipeline))
        .collect::<Vec<_>>();
    if pipelines.is_empty() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let connectors = pipelines
        .iter()
        .map(|pipeline| pipeline.connector.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    let statuses = match try_join_all(
        connectors
            .iter()
            .map(|connector| state.connect.status(connector)),
    )
    .await
    {
        Ok(statuses) => statuses,
        Err(error) => return (StatusCode::BAD_GATEWAY, format!("{error:#}")).into_response(),
    };
    if let Some((connector, _)) = connectors
        .iter()
        .zip(&statuses)
        .find(|(_, status)| status.is_failed())
    {
        return (
            StatusCode::CONFLICT,
            format!("connector has failed: {connector}"),
        )
            .into_response();
    }
    // A connector we don't hold must be running: if it's paused or stopped, someone outside this
    // server did it, and a later resume from here would undo their decision.
    if let Some((connector, _)) = connectors
        .iter()
        .zip(&statuses)
        .find(|(connector, status)| !status.is_running() && !pauses.is_held(connector))
    {
        return (
            StatusCode::CONFLICT,
            format!("connector is not running and was not paused by this server: {connector}"),
        )
            .into_response();
    }

    // Register the pause before touching Connect, so every connector it pauses is accounted for.
    // It holds all of them, including ones another token already holds, so releasing that token
    // can't resume them under this one.
    // The expiry also covers a crash during this request: the reloaded token lapses on its own.
    let token = Uuid::new_v4();
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
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")).into_response();
    }
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
            pauses.renew(&token, expiry(state.ttl));
            if let Err(error) = pauses.save(&state.pauses_file) {
                eprintln!("failed to save the expiry of pause {token}: {error:#}");
            }
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
                eprintln!("failed to remove pause {token} after a failed /pause: {error:#}");
            }
            // Best effort: a failure cancels the other pauses, any of which may already be paused.
            join_all(
                released
                    .iter()
                    .map(|connector| state.connect.resume(connector)),
            )
            .await;
            (StatusCode::BAD_GATEWAY, format!("{error:#}")).into_response()
        }
    }
}

pub(super) async fn resume(
    State(state): State<AppState>,
    Json(request): Json<ResumeRequest>,
) -> Response {
    let mut pauses = state.pauses.lock().await;
    let Some(connectors) = pauses
        .get(&request.token)
        .map(|pause| pause.connectors.clone())
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let statuses = match try_join_all(
        connectors
            .iter()
            .map(|connector| state.connect.status(connector)),
    )
    .await
    {
        Ok(statuses) => statuses,
        Err(error) => return (StatusCode::BAD_GATEWAY, format!("{error:#}")).into_response(),
    };
    if let Some((connector, _)) = connectors
        .iter()
        .zip(&statuses)
        .find(|(_, status)| status.is_failed())
    {
        return (
            StatusCode::CONFLICT,
            format!("connector has failed: {connector}"),
        )
            .into_response();
    }

    // The token is only removed once its connectors are running again, so a failed resume can be
    // retried with the same token.
    if let Err(error) = resume_held_connectors(&state, &pauses, &request.token).await {
        return (StatusCode::BAD_GATEWAY, format!("{error:#}")).into_response();
    }
    if let Err(error) = forget(&state, &mut pauses, &request.token) {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")).into_response();
    }
    StatusCode::OK.into_response()
}

pub(super) async fn renew(
    State(state): State<AppState>,
    Json(request): Json<RenewRequest>,
) -> Response {
    let mut pauses = state.pauses.lock().await;
    let Some(previous) = pauses.get(&request.token).map(|pause| pause.expires_at) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    pauses.renew(&request.token, expiry(state.ttl));
    if let Err(error) = pauses.save(&state.pauses_file) {
        pauses.renew(&request.token, previous);
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")).into_response();
    }
    StatusCode::OK.into_response()
}

/// Waits until the token expires, then resumes its connectors as `/resume` would.
///
/// A `/renew` moves the expiry while this sleeps, so it checks again under the lock before acting.
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
        sleep((expires_at - Utc::now()).to_std().unwrap_or_default()).await;

        let mut pauses = state.pauses.lock().await;
        match pauses.get(&token) {
            None => return,
            Some(pause) if pause.expires_at > Utc::now() => continue,
            Some(_) => {}
        }
        eprintln!("pause {token} expired, resuming its connectors");
        let result = match resume_held_connectors(&state, &pauses, &token).await {
            Ok(()) => forget(&state, &mut pauses, &token),
            Err(error) => Err(error),
        };
        match result {
            Ok(()) => return,
            Err(error) => {
                eprintln!("failed to resume expired pause {token}, retrying: {error:#}");
                pauses.renew(&token, expiry(state.ttl / 3));
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

fn expiry(ttl: std::time::Duration) -> DateTime<Utc> {
    TimeDelta::from_std(ttl)
        .ok()
        .and_then(|ttl| Utc::now().checked_add_signed(ttl))
        .unwrap_or(DateTime::<Utc>::MAX_UTC)
}

async fn resume_connector(state: &AppState, connector: &str) -> Result<()> {
    state
        .connect
        .resume(connector)
        .await
        .with_context(|| format!("failed to resume {connector}"))?;
    state
        .connect
        .wait_running(connector, state.pause_timeout)
        .await
}

async fn pause_connector(state: &AppState, connector: &str) -> Result<()> {
    state
        .connect
        .pause(connector)
        .await
        .with_context(|| format!("failed to pause {connector}"))?;
    state
        .connect
        .wait_paused(connector, state.pause_timeout)
        .await
}
