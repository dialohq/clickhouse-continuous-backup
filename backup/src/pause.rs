use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures::future::{join_all, try_join_all};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, sync::Mutex};
use uuid::Uuid;

use crate::{
    clickhouse::ClickHouse,
    config::PauseServerConfig,
    connect::{Connect, validate_offsets},
    model::{KafkaOffset, KafkaOffsetValue, KafkaPartition, KeeperRow, Pipeline},
};

#[derive(Clone)]
struct AppState {
    connect: Connect,
    clickhouse: ClickHouse,
    pipelines: Arc<[Pipeline]>,
    pause_timeout: Duration,
    /// Held for the whole of each `/pause` and `/resume` request, so they run one at a time.
    pauses: Arc<Mutex<Pauses>>,
}

/// Pauses handed out by `/pause`, kept in memory until `/resume` releases them.
///
/// A connector can be held by several tokens and is only resumed once the last one releases it.
/// Connectors paused outside this server are never held, so they are never resumed here.
#[derive(Default)]
struct Pauses {
    tokens: HashMap<Uuid, Vec<String>>,
    holders: HashMap<String, HashSet<Uuid>>,
}

impl Pauses {
    fn is_held(&self, connector: &str) -> bool {
        self.holders.contains_key(connector)
    }

    fn connectors(&self, token: &Uuid) -> Option<&[String]> {
        self.tokens.get(token).map(Vec::as_slice)
    }

    fn hold(&mut self, token: Uuid, connectors: Vec<String>) {
        for connector in &connectors {
            self.holders
                .entry(connector.clone())
                .or_default()
                .insert(token);
        }
        self.tokens.insert(token, connectors);
    }

    /// The token's connectors that no other token holds, i.e. the ones releasing it would resume.
    fn held_only_by(&self, token: &Uuid) -> Vec<String> {
        self.connectors(token)
            .unwrap_or_default()
            .iter()
            .filter(|connector| {
                self.holders
                    .get(*connector)
                    .is_none_or(|holders| holders.len() == 1)
            })
            .cloned()
            .collect()
    }

    /// Removes the token and returns its connectors that no other token holds any more.
    fn release(&mut self, token: &Uuid) -> Option<Vec<String>> {
        let connectors = self.tokens.remove(token)?;
        let released = connectors
            .into_iter()
            .filter(|connector| match self.holders.get_mut(connector) {
                Some(holders) => {
                    holders.remove(token);
                    if holders.is_empty() {
                        self.holders.remove(connector);
                        true
                    } else {
                        false
                    }
                }
                None => true,
            })
            .collect();
        Some(released)
    }
}

/// Selects pipelines by exactly one key, e.g. `{"topic": "records.input"}`.
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum PauseRequest {
    Topic(String),
    Connector(String),
    Table(String),
}

#[derive(Serialize)]
struct PauseResponse {
    token: Uuid,
    watermark: Vec<PipelineOffsets>,
}

/// Exact offsets of one pipeline, with the Connect offsets and KeeperMap rows they came from.
#[derive(Serialize)]
struct PipelineOffsets {
    connector: String,
    offsets: Vec<KafkaOffset>,
    connect_offsets: Vec<KafkaOffset>,
    keeper_rows: Vec<KeeperRow>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResumeRequest {
    token: Uuid,
}

impl PauseRequest {
    fn matches(&self, pipeline: &Pipeline) -> bool {
        match self {
            Self::Topic(topic) => pipeline.topic == *topic,
            Self::Connector(connector) => pipeline.connector == *connector,
            Self::Table(table) => pipeline.table == *table,
        }
    }
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
        pauses: Arc::default(),
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/pause", post(pause))
        .route("/resume", post(resume))
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

async fn pause(State(state): State<AppState>, Json(request): Json<PauseRequest>) -> Response {
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
                .map(|pipeline| read_offsets(&state, pipeline)),
        )
        .await
    }
    .await;
    match result {
        Ok(watermark) => Json(PauseResponse { token, watermark }).into_response(),
        Err(error) => {
            let released = pauses.release(&token).unwrap_or_default();
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

async fn resume(State(state): State<AppState>, Json(request): Json<ResumeRequest>) -> Response {
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

async fn read_offsets(state: &AppState, pipeline: &Pipeline) -> Result<PipelineOffsets> {
    let connect_offsets = state.connect.offsets(&pipeline.connector).await?;
    let keeper_rows = state
        .clickhouse
        .keeper_rows(&pipeline.database, &pipeline.state_table)
        .await
        .with_context(|| format!("failed to read KeeperMap state: {}", pipeline.connector))?;
    let offsets = exact_offsets(pipeline, &connect_offsets, &keeper_rows)
        .with_context(|| format!("inconsistent offsets: {}", pipeline.connector))?;
    Ok(PipelineOffsets {
        connector: pipeline.connector.clone(),
        offsets,
        connect_offsets,
        keeper_rows,
    })
}

/// Derives exact offsets from KeeperMap and cross-checks them against Connect's offsets.
///
/// Copied from `backup::checkpoint`; keep the validation in sync.
fn exact_offsets(
    pipeline: &Pipeline,
    observed: &[KafkaOffset],
    rows: &[KeeperRow],
) -> Result<Vec<KafkaOffset>> {
    validate_offsets(observed)?;
    let prefix = format!("{}-", pipeline.topic);
    if rows.iter().any(|row| !row.key.starts_with(&prefix)) {
        bail!("KeeperMap state contains an unexpected topic")
    }
    if rows.iter().any(|row| row.min_offset > row.max_offset) {
        bail!("KeeperMap state contains an invalid offset range")
    }
    if rows.iter().any(|row| row.max_offset > i64::MAX as u64) {
        bail!("KeeperMap state exceeds the connector's signed offset range")
    }
    let relevant = rows
        .iter()
        .filter_map(|row| {
            row.key
                .strip_prefix(&prefix)
                .map(|partition| (partition, row))
        })
        .map(|(partition, row)| {
            let partition = partition
                .parse::<u32>()
                .context("KeeperMap state key has an invalid partition")?;
            if partition >= pipeline.partitions {
                bail!("KeeperMap state contains an out-of-range partition")
            }
            if row.state != "AFTER_PROCESSING" {
                bail!("KeeperMap state is not safely committed: {}", row.key)
            }
            Ok((partition, row))
        })
        .collect::<Result<HashMap<_, _>>>()?;
    if relevant.len()
        != rows
            .iter()
            .filter(|row| row.key.starts_with(&prefix))
            .count()
    {
        bail!("KeeperMap state contains duplicate partitions")
    }

    let mut offsets = Vec::with_capacity(pipeline.partitions as usize);
    for partition in 0..pipeline.partitions {
        let exact = relevant
            .get(&partition)
            .map(|row| {
                row.max_offset
                    .checked_add(1)
                    .context("KeeperMap offset overflow")
            })
            .transpose()?
            .unwrap_or(0);
        if let Some(committed) = observed.iter().find(|offset| {
            offset.partition.kafka_topic == pipeline.topic
                && offset.partition.kafka_partition == partition
        }) && committed.offset.kafka_offset > exact
        {
            bail!(
                "Kafka Connect offset is ahead of ClickHouse KeeperMap state: {}-{partition}",
                pipeline.topic
            )
        }
        offsets.push(KafkaOffset {
            partition: KafkaPartition {
                kafka_topic: pipeline.topic.clone(),
                kafka_partition: partition,
            },
            offset: KafkaOffsetValue {
                kafka_offset: exact,
            },
        });
    }
    if observed.iter().any(|offset| {
        offset.partition.kafka_topic != pipeline.topic
            || offset.partition.kafka_partition >= pipeline.partitions
    }) {
        bail!("connector returned an unexpected topic partition")
    }
    Ok(offsets)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn pipeline() -> Pipeline {
        Pipeline {
            connector: "records".to_owned(),
            database: "records".to_owned(),
            state_table: "records_state".to_owned(),
            keeper_path: "/durable-clickhouse-sink/default/records".to_owned(),
            table: "records".to_owned(),
            topic: "records.input".to_owned(),
            partitions: 3,
        }
    }

    fn offset(partition: u32, value: u64) -> KafkaOffset {
        KafkaOffset {
            partition: KafkaPartition {
                kafka_topic: "records.input".to_owned(),
                kafka_partition: partition,
            },
            offset: KafkaOffsetValue {
                kafka_offset: value,
            },
        }
    }

    fn row(partition: u32, max: u64, state: &str) -> KeeperRow {
        KeeperRow {
            key: format!("records.input-{partition}"),
            min_offset: max,
            max_offset: max,
            state: state.to_owned(),
        }
    }

    #[test]
    fn connector_is_released_only_by_its_last_holder() {
        let (first, second) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let mut pauses = Pauses::default();
        pauses.hold(first, vec!["a".to_owned(), "b".to_owned()]);
        pauses.hold(second, vec!["b".to_owned()]);

        assert_eq!(pauses.held_only_by(&first), vec!["a".to_owned()]);
        assert_eq!(pauses.release(&first), Some(vec!["a".to_owned()]));
        assert_eq!(pauses.held_only_by(&second), vec!["b".to_owned()]);
        assert!(!pauses.is_held("a"));
        assert!(pauses.is_held("b"));
        assert_eq!(pauses.release(&second), Some(vec!["b".to_owned()]));
        assert!(!pauses.is_held("b"));
    }

    #[test]
    fn unknown_or_released_token_is_not_found() {
        let token = Uuid::from_u128(1);
        let mut pauses = Pauses::default();
        assert_eq!(pauses.release(&token), None);
        pauses.hold(token, vec!["a".to_owned()]);
        assert!(pauses.release(&token).is_some());
        assert_eq!(pauses.release(&token), None);
        assert_eq!(pauses.connectors(&token), None);
    }

    #[test]
    fn keeper_offsets_are_exact_even_when_connect_lags() {
        let offsets = exact_offsets(
            &pipeline(),
            &[offset(0, 8)],
            &[row(0, 9, "AFTER_PROCESSING")],
        )
        .unwrap();
        assert_eq!(offsets, vec![offset(0, 10), offset(1, 0), offset(2, 0)]);
    }

    #[test]
    fn connect_may_lag_keeper_by_any_amount_but_never_lead() {
        for max_offset in [0, 1, 2, 31, 1024, u32::MAX as u64] {
            let exact = max_offset + 1;
            for observed in [0, 1, exact / 2, exact] {
                let offsets = exact_offsets(
                    &pipeline(),
                    &[offset(0, observed)],
                    &[row(0, max_offset, "AFTER_PROCESSING")],
                )
                .unwrap();
                assert_eq!(offsets[0], offset(0, exact));
            }
            assert!(
                exact_offsets(
                    &pipeline(),
                    &[offset(0, exact + 1)],
                    &[row(0, max_offset, "AFTER_PROCESSING")],
                )
                .is_err()
            );
        }
    }

    #[test]
    fn partitions_are_derived_independently_from_unordered_state() {
        let offsets = exact_offsets(
            &pipeline(),
            &[offset(2, 90), offset(0, 10)],
            &[
                row(2, 99, "AFTER_PROCESSING"),
                row(0, 10, "AFTER_PROCESSING"),
            ],
        )
        .unwrap();
        assert_eq!(offsets, vec![offset(0, 11), offset(1, 0), offset(2, 100)]);
    }

    #[test]
    fn rejects_connect_ahead_of_clickhouse() {
        let error = exact_offsets(
            &pipeline(),
            &[offset(0, 11)],
            &[row(0, 9, "AFTER_PROCESSING")],
        )
        .unwrap_err();
        assert!(error.to_string().contains("ahead"));
    }

    #[test]
    fn rejects_unfinished_keeper_state() {
        let error = exact_offsets(
            &pipeline(),
            &[offset(0, 9)],
            &[row(0, 9, "BEFORE_PROCESSING")],
        )
        .unwrap_err();
        assert!(error.to_string().contains("not safely committed"));
    }

    #[test]
    fn rejects_out_of_range_and_duplicate_keeper_partitions() {
        assert!(exact_offsets(&pipeline(), &[], &[row(3, 9, "AFTER_PROCESSING")]).is_err());
        assert!(
            exact_offsets(
                &pipeline(),
                &[],
                &[
                    row(0, 9, "AFTER_PROCESSING"),
                    row(0, 10, "AFTER_PROCESSING")
                ]
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_keeper_rows_for_another_topic() {
        let mut unexpected = row(0, 9, "AFTER_PROCESSING");
        unexpected.key = "other-0".to_owned();
        assert!(exact_offsets(&pipeline(), &[], &[unexpected]).is_err());
    }

    #[test]
    fn rejects_unexpected_connect_topic_and_offset_overflow() {
        let mut unexpected = offset(0, 1);
        unexpected.partition.kafka_topic = "other".to_owned();
        assert!(exact_offsets(&pipeline(), &[unexpected], &[]).is_err());
        assert!(exact_offsets(&pipeline(), &[offset(3, 1)], &[]).is_err());
        assert!(exact_offsets(&pipeline(), &[], &[row(0, u64::MAX, "AFTER_PROCESSING")]).is_err());
    }

    #[test]
    fn rejects_invalid_keeper_offset_range() {
        let mut invalid = row(0, 9, "AFTER_PROCESSING");
        invalid.min_offset = 10;
        assert!(exact_offsets(&pipeline(), &[], &[invalid]).is_err());
    }
}
