use std::{path::PathBuf, time::Duration};

use crate::timing::TestReport;
use crate::{DnvrBackend, TestEnvironment, clients::ConnectorState};
use anyhow::{Result, ensure};

const CONNECTOR: &str = "durable-clickhouse-sink-records";

#[tokio::test]
async fn backup_refuses_a_paused_connector_without_resuming_it() -> Result<()> {
    let mut report = TestReport::new("backup_preflight")?;
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

    timings
        .measure(
            "waiting for connector Running",
            env.connect
                .wait_for_state(CONNECTOR, ConnectorState::Running, Duration::from_secs(60)),
        )
        .await?;
    timings
        .measure("pause connector", env.connect.pause(CONNECTOR))
        .await?;
    timings
        .measure(
            "waiting for connector Paused",
            env.connect
                .wait_for_state(CONNECTOR, ConnectorState::Paused, Duration::from_secs(30)),
        )
        .await?;

    let backup = timings
        .measure(
            "backup preflight (expected refusal)",
            env.backup.run("paused-connector"),
        )
        .await;
    let still_paused = timings
        .measure(
            "waiting for connector Paused",
            env.connect
                .wait_for_state(CONNECTOR, ConnectorState::Paused, Duration::from_secs(5)),
        )
        .await;

    // Restore the environment even when one of the assertions below fails.
    timings
        .measure("resume connector", env.connect.resume(CONNECTOR))
        .await?;
    timings
        .measure(
            "waiting for connector Running",
            env.connect
                .wait_for_state(CONNECTOR, ConnectorState::Running, Duration::from_secs(30)),
        )
        .await?;

    let failure = format!(
        "{:#}",
        backup.expect_err("backup unexpectedly accepted a paused connector")
    );
    ensure!(
        failure.contains("connector must be fully running before backup"),
        "backup failed for the wrong reason: {}",
        failure
    );
    still_paused?;
    timings.measure("stop environment", env.stop()).await?;
    report.mark_success();
    Ok(())
}
