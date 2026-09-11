use std::{
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::timing::TestReport;
use crate::{DnvrBackend, TestEnvironment};
use anyhow::Result;

#[tokio::test]
async fn ingests_records() -> Result<()> {
    let mut report = TestReport::new("ingestion")?;
    let timings = report.timings();
    let project_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_owned();
    let env = timings
        .measure(
            "environment startup",
            TestEnvironment::start(DnvrBackend::new(project_root, timings.clone())?),
        )
        .await?;
    let prefix = format!(
        "rust-e2e-{}-",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let records: Vec<_> = (1..=20)
        .map(|index| {
            let key = format!("{prefix}{index}");
            let value = serde_json::json!({
                "record_key": key.clone(),
                "recorded_at": "2026-09-05 12:00:00.000",
                "payload": format!("value-{index}"),
            })
            .to_string();
            (key, value)
        })
        .collect();

    timings
        .measure(
            "produce 20 Kafka records",
            env.kafka.produce_json("records.input", &records),
        )
        .await?;
    timings.measure("waiting for row count", env.clickhouse
        .wait_for_u64(
            &format!(
                "SELECT count() FROM durable_e2e.records WHERE startsWith(record_key, '{prefix}')"
            ),
            20,
            Duration::from_secs(60),
        )).await?;
    timings.measure("waiting for unique keys", env.clickhouse.wait_for_u64(
        &format!("SELECT uniqExact(record_key) FROM durable_e2e.records WHERE startsWith(record_key, '{prefix}')"),
        20,
        Duration::from_secs(60),
    )).await?;
    timings.measure("stop environment", env.stop()).await?;
    report.mark_success();
    Ok(())
}
