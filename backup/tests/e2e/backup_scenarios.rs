use std::{fmt::Write, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use durable_clickhouse_backup::model::{BackupKind, BackupOutput, BackupParent};
use tokio::time::{sleep, timeout};

use crate::{
    DnvrBackend, EnvironmentBackend, TestEnvironment,
    clients::ConnectorState,
    timing::{TestReport, Timings},
};

const CONNECTOR: &str = "durable-clickhouse-sink-records";
const CATALOG: &str = "durable-clickhouse-sink.backup-catalog";
const WAIT: Duration = Duration::from_secs(120);
const BACKUP_TIMEOUT: Duration = Duration::from_secs(180);

async fn start(timings: &Timings) -> Result<TestEnvironment<DnvrBackend>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_owned();
    let env = timings
        .measure(
            "environment startup",
            TestEnvironment::start(DnvrBackend::new(root, timings.clone())?),
        )
        .await?;
    timings
        .measure(
            "waiting for connector Running",
            env.connect
                .wait_for_state(CONNECTOR, ConnectorState::Running, WAIT),
        )
        .await?;
    timings
        .measure("create backup catalog", env.kafka.create_catalog(CATALOG))
        .await?;
    Ok(env)
}

// Deterministic pseudo-random payloads keep compression from reducing the
// throttled upload to an instant. They are test data, not security randomness.
fn records(first: u64, last: u64, large: bool) -> Vec<(String, String)> {
    (first..=last)
        .map(|index| {
            let key = format!("record-{index}");
            let mut payload = format!("value-{index}");
            if large {
                payload.clear();
                let mut state = index;
                for _ in 0..256 {
                    state = state.wrapping_add(0x9e3779b97f4a7c15);
                    let mut value = state;
                    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
                    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
                    write!(payload, "{:016x}", value ^ (value >> 31)).unwrap();
                }
            }
            let value = serde_json::json!({
                "record_key": key,
                "recorded_at": "2026-08-29 12:00:00.000",
                "payload": payload,
            })
            .to_string();
            (key, value)
        })
        .collect()
}

async fn ingest<B: EnvironmentBackend>(
    env: &TestEnvironment<B>,
    timings: &Timings,
    first: u64,
    last: u64,
    large: bool,
) -> Result<()> {
    let records = records(first, last, large);
    timings
        .measure(
            format!("produce records {first}..={last}"),
            env.kafka.produce_json("records.input", &records),
        )
        .await?;
    timings
        .measure(
            format!("wait for {last} rows"),
            env.clickhouse
                .wait_for_u64("SELECT count() FROM durable_e2e.records", last, WAIT),
        )
        .await?;
    timings
        .measure(
            format!("wait for {last} unique keys"),
            env.clickhouse.wait_for_u64(
                "SELECT uniqExact(record_key) FROM durable_e2e.records",
                last,
                WAIT,
            ),
        )
        .await
}

async fn backup<B: EnvironmentBackend>(
    env: &TestEnvironment<B>,
    timings: &Timings,
    run_id: &str,
) -> Result<BackupOutput> {
    timings
        .measure(format!("backup {run_id}"), async {
            timeout(BACKUP_TIMEOUT, env.backup.run(run_id))
                .await
                .context("backup timed out")?
        })
        .await
}

fn check_backup(
    output: &BackupOutput,
    kind: BackupKind,
    position: u32,
    parent: Option<&BackupOutput>,
) -> Result<()> {
    let reference = &output.manifest.backup;
    ensure!(
        output.details.status == "BACKUP_CREATED",
        "backup did not complete: {:?}",
        output.details
    );
    ensure!(
        reference.kind == kind && reference.position == position,
        "unexpected backup kind/position: {reference:?}"
    );
    ensure!(
        reference.parent
            == parent.map(|parent| BackupParent {
                id: parent.manifest.backup.id,
                name: parent.manifest.backup.name.clone(),
            }),
        "incorrect backup parent"
    );
    ensure!(
        reference.name.starts_with("S3("),
        "backup destination is not S3"
    );
    ensure!(!reference.id.is_nil(), "backup ID is nil");
    ensure!(
        output.details.id == reference.id && output.details.name == reference.name,
        "backup details disagree with manifest"
    );
    if let Some(parent) = parent {
        ensure!(
            reference.chain_id == parent.manifest.backup.chain_id,
            "incremental changed chain"
        );
    }
    Ok(())
}

#[tokio::test]
async fn bad_credentials_do_not_pause_connector_or_create_backup() -> Result<()> {
    let mut report = TestReport::new("bad_credentials")?;
    let timings = report.timings();
    let env = start(&timings).await?;
    let query = "SELECT count() FROM system.backups WHERE status = 'BACKUP_CREATED'";
    let before = env.clickhouse.query(query).await?;
    let mut config = env.backup.config("bad-credentials")?;
    config.clickhouse_username = "e2e_nonexistent_user".to_owned();
    config.clickhouse_password = "incorrect-password".to_owned();
    let result = timings
        .measure("backup with bad credentials (expected refusal)", async {
            timeout(BACKUP_TIMEOUT, durable_clickhouse_backup::run(&config))
                .await
                .context("backup timed out")?
        })
        .await;
    let error = format!(
        "{:#}",
        result.expect_err("backup accepted invalid credentials")
    );
    ensure!(
        error.contains("AUTHENTICATION_FAILED"),
        "backup failed for the wrong reason: {error}"
    );
    ensure!(
        env.connect
            .is_fully_in_state(CONNECTOR, ConnectorState::Running)
            .await?,
        "backup left connector non-running"
    );
    ensure!(
        env.clickhouse.query(query).await? == before,
        "failed backup created an archive"
    );
    timings.measure("stop environment", env.stop()).await?;
    report.mark_success();
    Ok(())
}

#[tokio::test]
async fn incremental_backup_refuses_a_missing_parent_manifest() -> Result<()> {
    let mut report = TestReport::new("missing_parent")?;
    let timings = report.timings();
    let env = start(&timings).await?;
    ingest(&env, &timings, 1, 50, false).await?;
    let base = backup(&env, &timings, "base").await?;
    check_backup(&base, BackupKind::Full, 0, None)?;
    ingest(&env, &timings, 51, 100, false).await?;

    let state_key = "__durable_clickhouse_sink_chain_state";
    let (base_offset, _) = env
        .kafka
        .read_json_key_with_offset(CATALOG, &base.manifest.backup.id.to_string(), WAIT)
        .await?;
    let (state_offset, state) = env
        .kafka
        .read_json_key_with_offset(CATALOG, state_key, WAIT)
        .await?;
    ensure!(
        base_offset < state_offset,
        "base manifest must precede chain state"
    );
    timings
        .measure(
            "delete catalog prefix containing base manifest",
            env.kafka.delete_catalog_prefix(CATALOG, state_offset),
        )
        .await?;
    ensure!(
        env.kafka.read_json_key(CATALOG, state_key, WAIT).await? == state,
        "prefix deletion removed the chain state"
    );

    let query = "SELECT count() FROM system.backups WHERE status = 'BACKUP_CREATED'";
    let before = env.clickhouse.query(query).await?;
    timings
        .measure("incremental backup rejects missing parent", async {
            let result = timeout(BACKUP_TIMEOUT, env.backup.run("missing-parent-incremental"))
                .await
                .context("incremental backup timed out")?;
            let error = result.expect_err("incremental backup accepted a missing parent manifest");
            ensure!(
                format!("{error:#}").contains(&format!(
                    "Missing backup manifest {}",
                    base.manifest.backup.id
                )),
                "incremental backup failed for the wrong reason: {error:#}"
            );
            Ok(())
        })
        .await?;
    ensure!(
        env.clickhouse.query(query).await? == before,
        "failed attempt created a backup"
    );
    ensure!(
        env.kafka.read_json_key(CATALOG, state_key, WAIT).await? == state,
        "failed attempt changed the chain state"
    );
    ensure!(
        env.connect
            .is_fully_in_state(CONNECTOR, ConnectorState::Running)
            .await?,
        "failed attempt left the connector non-running"
    );
    timings.measure("stop environment", env.stop()).await?;
    report.mark_success();
    Ok(())
}

#[tokio::test]
async fn full_incremental_backups_preserve_snapshots_and_roll_over() -> Result<()> {
    let mut report = TestReport::new("backup_chain")?;
    let timings = report.timings();
    let env = start(&timings).await?;
    ingest(&env, &timings, 1, 1100, true).await?;

    let base = {
        let base = backup(&env, &timings, "base");
        tokio::pin!(base);
        let observe_upload = timings.measure("wait for upload with ingestion resumed", async {
            let query = "SELECT count() FROM system.backups WHERE status = 'CREATING_BACKUP'";
            timeout(Duration::from_secs(60), async {
                loop {
                    let active = env.clickhouse.query(query).await? == "1";
                    if active
                        && env
                            .connect
                            .is_fully_in_state(CONNECTOR, ConnectorState::Running)
                            .await?
                    {
                        return Ok::<_, anyhow::Error>(());
                    }
                    sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .context("did not observe an upload with the connector running")?
        });
        tokio::select! {
            result = &mut base => {
                let output = result?;
                bail!("backup completed before concurrent ingestion could be tested: {:?}", output.details);
            }
            result = observe_upload => result?,
        }
        tokio::select! {
            result = &mut base => {
                result?;
                bail!("backup completed before new records were ingested");
            }
            result = ingest(&env, &timings, 1101, 1150, false) => result?,
        }
        base.await?
    };
    check_backup(&base, BackupKind::Full, 0, None)?;
    ensure!(
        serde_json::to_value(&base.manifest.backup)?
            .get("parent")
            .is_none(),
        "full manifest includes a parent"
    );

    timings
        .measure(
            "restore immutable base snapshot",
            env.clickhouse.query_with_timeout(
                &format!(
                    "RESTORE TABLE durable_e2e.records AS durable_e2e.base_snapshot_probe FROM {}",
                    base.manifest.backup.name
                ),
                WAIT,
            ),
        )
        .await?;
    let snapshot = env.clickhouse.query("SELECT count(), uniqExact(record_key), countIf(toUInt64(substring(record_key, 8)) > 1100) FROM durable_e2e.base_snapshot_probe").await?;
    let dropped = env
        .clickhouse
        .query("DROP TABLE durable_e2e.base_snapshot_probe SYNC")
        .await;
    ensure!(
        snapshot == "1100\t1100\t0",
        "base snapshot includes missing, duplicate, or post-snapshot records: {snapshot}"
    );
    dropped?;

    let first = backup(&env, &timings, "incremental-one").await?;
    check_backup(&first, BackupKind::Incremental, 1, Some(&base))?;
    ingest(&env, &timings, 1151, 1200, false).await?;
    let second = backup(&env, &timings, "incremental-two").await?;
    check_backup(&second, BackupKind::Incremental, 2, Some(&first))?;

    let connectors = &second.manifest.connectors;
    ensure!(
        connectors.len() == 1 && connectors[0].name == CONNECTOR,
        "unexpected connectors in manifest"
    );
    let checkpoint = &connectors[0];
    ensure!(
        !checkpoint.keeper.rows.is_empty(),
        "manifest has no Keeper checkpoints"
    );
    ensure!(
        checkpoint
            .keeper
            .rows
            .iter()
            .all(|row| row.state == "AFTER_PROCESSING"),
        "unfinished Keeper checkpoint"
    );
    ensure!(
        checkpoint.offsets.len() == 3 && !checkpoint.observed_connect_offsets.is_empty(),
        "missing partition offsets"
    );
    for observed in &checkpoint.observed_connect_offsets {
        let exact = checkpoint
            .offsets
            .iter()
            .find(|exact| exact.partition == observed.partition)
            .context("observed partition has no exact offset")?;
        ensure!(
            exact.offset.kafka_offset >= observed.offset.kafka_offset,
            "Connect offset leads exact checkpoint"
        );
    }
    let published = timings
        .measure(
            "read committed Kafka catalog manifest",
            env.kafka.read_json_key(
                CATALOG,
                &second.manifest.backup.id.to_string(),
                Duration::from_secs(30),
            ),
        )
        .await?;
    ensure!(
        published == serde_json::to_value(&second.manifest)?,
        "Kafka catalog differs from returned manifest"
    );

    ingest(&env, &timings, 1201, 1250, false).await?;
    let rollover = backup(&env, &timings, "rollover").await?;
    check_backup(&rollover, BackupKind::Full, 0, None)?;
    ensure!(
        rollover.manifest.backup.chain_id != base.manifest.backup.chain_id,
        "rollover reused the old chain"
    );
    timings.measure("stop environment", env.stop()).await?;
    report.mark_success();
    Ok(())
}
