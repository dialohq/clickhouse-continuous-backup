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

Restore into a clean ClickHouse database backed by an isolated Keeper:

1. Retrieve the chosen recovery manifest by its target-data backup ID from the
   recovery topic.
2. Create the empty target tables with the desired production engines through
   the normal schema tool.
3. Restore the manifest's `backup.name` into those empty tables with
   `allow_different_table_def = true`. ClickHouse follows incremental
   dependencies automatically. The archive contains non-replicated snapshot
   metadata while the destination may use another compatible MergeTree engine.
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
by the chart's `timeouts` settings. Before it touches Kafka Connect offsets, it
proves every exact replay offset remains in Kafka, creates missing KeeperMap
tables, and rehydrates the manifest rows. Existing exact subsets are completed;
conflicting or extra rows are rejected. It reads KeeperMap back for exact
equality, patches offsets through Kafka Connect's standard API, and verifies
their read-back. It never resumes a connector. Partial work is idempotent and
safe to retry while connectors remain stopped.

`EXPECTED_BACKUP_NAME` binds the manifest to the selected target-data archive.
The recovery topic is the authoritative KeeperMap checkpoint and must be
protected and retained accordingly.

## Drill acceptance criteria

A restore drill is successful only when:

- the target-data backup restores into the intended production table engines;
- KeeperMap is reconstructed exactly from the selected recovery manifest;
- restored row counts and content-level aggregates match the recovery point;
- the recovery command proves restored KeeperMap equality and exact Connect
  offset read-back;
- records retained after the recovery point replay exactly once;
- the old ClickHouse target stops advancing after cutover;
- the measured restore and replay time satisfies the RTO, and the selected
  recovery-point age satisfies the RPO.

The repository RKE2 E2E test performs this sequence with Redpanda, two isolated
ClickHouse/Keeper instances, and MinIO.
