# Backup and recovery

## Backup protocol

[Backup procedure](backup-procedure.md) defines the complete ordered protocol,
artifact ownership, commit point, failure outcomes, and corresponding Rust
functions.

The chart does not delete external object-store data. Retention must delete a
closed `chains/<chain-id>/` prefix as a unit, never individual members of a live
chain. Keep at least the number of complete chains required by the recovery
policy. Kafka input topics, Connect internal topics,
and recovery topic require their own replication and disaster-recovery policy.

## Failure behavior

The Rust recovery process handles `SIGINT` and `SIGTERM` and attempts to resume
all connectors that were verified running at the start. It aggregates resume
failures instead of hiding them behind the original error. No process can
recover from `SIGKILL`, abrupt node loss, or loss of network access to Connect.
Alert on failed Jobs and paused connectors. Before retrying a failed hard-kill
case, inspect `system.backups`, confirm no backup is active, and resume the
chart-managed connectors through the Connect API.

Backups use ClickHouse's direct `S3(named_collection, path)` destination. The
named collection is configured on ClickHouse, not by this chart, and owns the
Ceph RGW/S3 credentials. This avoids local S3-disk metadata that would be absent
on a clean disaster-recovery server.

## Exact disaster recovery

Preserve the Helm release name, `stateNamespace`, pipeline and connector names,
input topic names and partition counts, target database/table names,
KeeperMap table names, and Kafka Connect internal-topic identities.

Restore into an empty ClickHouse database backed by an isolated Keeper:

1. Retrieve the chosen recovery manifest by its target-data backup ID from the
   recovery topic.
2. Restore the manifest's `backup.name`. ClickHouse follows its incremental
   dependencies automatically.
3. Restore `checkpoint_backup.name` into the same database and table names.
4. Stop every managed connector with `PUT /connectors/<name>/stop` and wait for
   `STOPPED`.
5. Run `/bin/durable-clickhouse-recovery restore-offsets` with the manifest on
   stdin and the restored ClickHouse endpoint in `CLICKHOUSE_URL`.
6. Point the existing Helm release at the restored ClickHouse and resume the
   connectors.

Keep exclusive control of the Connect REST API from step 4 until the recovery
command completes. The command repeatedly verifies `STOPPED`, but Connect does
not provide an administrative lock that can prevent a concurrent resume.

The recovery command requires `CONNECT_URL`, `CLICKHOUSE_URL`,
`CLICKHOUSE_USERNAME`, `CLICKHOUSE_PASSWORD`, `CONNECTOR_NAMES`,
`EXPECTED_BACKUP_NAME`, `RECOVERY_MANIFEST_FILE`, and
`STOP_TIMEOUT_SECONDS`, plus `KAFKA_BOOTSTRAP_SERVERS` and optional
`KAFKA_PROPERTIES_FILE`. `RUNTIME_TIMEOUTS` must contain the JSON value rendered
by the chart's `timeouts` settings. Before it touches Kafka Connect, it reads
every restored KeeperMap table, requires exact logical equality with the
manifest, and proves that every exact replay offset remains in Kafka.
It then patches the exact derived offsets through Kafka Connect's standard
offset API and reads them back for equality. It never resumes a connector. A
partial API failure leaves all connectors stopped and is safe to retry.

This ordering closes the dangerous cases: restoring data without its KeeperMap
checkpoint cannot rewind Kafka, and restoring the checkpoint while selecting a
different target-data archive is rejected by `EXPECTED_BACKUP_NAME`.

## Drill acceptance criteria

A restore drill is successful only when:

- the target-data backup and independent KeeperMap checkpoint both restore;
- restored row counts and content-level aggregates match the recovery point;
- the recovery command proves restored KeeperMap equality and exact Connect
  offset read-back;
- records retained after the recovery point replay exactly once;
- the old ClickHouse target stops advancing after cutover;
- the measured restore and replay time satisfies the RTO, and the selected
  recovery-point age satisfies the RPO.

The repository RKE2 E2E test performs this sequence with Redpanda, two isolated
ClickHouse/Keeper instances, and MinIO.
