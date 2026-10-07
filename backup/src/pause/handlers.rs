use std::collections::BTreeSet;

use anyhow::{Context, Result};
use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures::future::{join_all, try_join_all};
use uuid::Uuid;

use super::{
    AppState,
    api::{PauseRequest, PauseResponse, ResumeRequest},
    offsets::read_offsets,
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
    let token = Uuid::new_v4();
    pauses.hold(
        token,
        connectors
            .iter()
            .map(|&connector| connector.to_owned())
            .collect(),
    );
    if let Err(error) = pauses.save(&state.pauses_file) {
        pauses.release(&token);
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")).into_response();
    }

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
        Ok(watermark) => Json(PauseResponse { token, watermark }).into_response(),
        Err(error) => {
            let released = pauses.release(&token).unwrap_or_default();
            if let Err(error) = pauses.save(&state.pauses_file) {
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
    let Some(connectors) = pauses.connectors(&request.token).map(<[String]>::to_vec) else {
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
    let released = pauses.held_only_by(&request.token);
    let resumes = released
        .iter()
        .map(|connector| resume_connector(&state, connector));
    if let Err(error) = try_join_all(resumes).await {
        return (StatusCode::BAD_GATEWAY, format!("{error:#}")).into_response();
    }
    pauses.release(&request.token);
    if let Err(error) = pauses.save(&state.pauses_file) {
        // Keep the token in memory as the file still has it, so the resume can be retried.
        pauses.hold(request.token, connectors);
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")).into_response();
    }
    StatusCode::OK.into_response()
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
