use std::{
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::timing::TestReport;
use crate::{Component, DatabaseEngine, DnvrBackend, TestEnvironment, clients::ConnectorState};
use anyhow::Result;
use tokio::time::sleep;

#[tokio::test]
async fn ingests_exactly_once_across_connect_restarts() -> Result<()> {
    let mut report = TestReport::new("crash_recovery")?;
    let timings = report.timings();
    let project_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_owned();
    let env = timings
        .measure(
            "environment startup",
            TestEnvironment::start(DnvrBackend::new(
                project_root,
                timings.clone(),
                DatabaseEngine::Replicated,
            )?),
        )
        .await?;
    timings
        .measure(
            "waiting for connector Running",
            env.connect.wait_for_state(
                "durable-clickhouse-sink-records",
                ConnectorState::Running,
                Duration::from_secs(60),
            ),
        )
        .await?;
    let prefix = format!(
        "rust-crash-e2e-{}-",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let payload = "x".repeat(2048);
    let records: Vec<_> = (1..=1_000)
        .map(|index| {
            let key = format!("{prefix}{index}");
            let value = serde_json::json!({
                "record_key": key,
                "recorded_at": "2026-09-05 12:00:00.000",
                "payload": payload,
            })
            .to_string();
            (format!("{prefix}{index}"), value)
        })
        .collect();

    let producer = tokio::spawn({
        let kafka = env.kafka.clone();
        let timings = timings.clone();
        async move {
            timings
                .measure(
                    "produce 1000 Kafka records",
                    kafka.produce_json("records.input", &records),
                )
                .await
        }
    });
    for restart in 1..=3 {
        timings
            .measure("delay before restart", async {
                sleep(Duration::from_millis(500)).await;
                Ok(())
            })
            .await?;
        timings
            .measure(
                format!("restart Connect {restart}/3"),
                env.restart(Component::Connect),
            )
            .await?;
        timings
            .measure(
                "waiting for connector Running",
                env.connect.wait_for_state(
                    "durable-clickhouse-sink-records",
                    ConnectorState::Running,
                    Duration::from_secs(60),
                ),
            )
            .await?;
    }
    timings
        .measure("join producer", async { producer.await? })
        .await?;

    let predicate = format!("startsWith(record_key, '{prefix}')");
    timings
        .measure(
            "waiting for row count",
            env.clickhouse.wait_for_u64(
                &format!("SELECT count() FROM durable_e2e.records WHERE {predicate}"),
                1_000,
                Duration::from_secs(120),
            ),
        )
        .await?;
    timings
        .measure(
            "waiting for unique keys",
            env.clickhouse.wait_for_u64(
                &format!("SELECT uniqExact(record_key) FROM durable_e2e.records WHERE {predicate}"),
                1_000,
                Duration::from_secs(120),
            ),
        )
        .await?;
    timings.measure("stop environment", env.stop()).await?;
    report.mark_success();
    Ok(())
}
