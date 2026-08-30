# Backup and recovery

## Recovery-point protocol

The optional CronJob requires every managed connector and task to be `RUNNING`,
acquires the recovery topic's single-partition consumer-group lock, pauses all
connectors, and waits for `PAUSED`. A paused task has completed its active
`put`, so both the target table and the connector's KeeperMap state are stable.

Kafka Connect's committed offset is observational only: it can lag a completed
ClickHouse write. The authoritative next offset for partition `p` is:

```text
KeeperMap["<canonical-topic>-<p>"].maxOffset + 1
```

An absent KeeperMap row means offset zero. A backup is refused if a row is not
`AFTER_PROCESSING`, a row names a partition outside the configured range, or
Kafka Connect reports an offset ahead of KeeperMap. The versioned recovery manifest
records the derived offsets, the observed Connect offsets, and every KeeperMap
row so the relationship can be checked again during restore.

While delivery remains paused, the Job creates two native ClickHouse backups:

- the target MergeTree tables, as a full or incremental backup;
- all connector KeeperMap tables, as an independent full checkpoint.

Separating these artifacts is deliberate. MergeTree data files are immutable,
whereas KeeperMap uses append-only files. ClickHouse issue
[#112403](https://github.com/ClickHouse/ClickHouse/issues/112403) confirms that
incremental backup deduplication can mishandle append-only files with different
base coverage. Incremental mode therefore rejects non-MergeTree target engines,
and KeeperMap never depends on an incremental base.

The Job accepts each artifact only after `system.backups` confirms its exact ID,
destination, and `BACKUP_CREATED` status. It then publishes the recovery point
and the next chain head in one Kafka transaction. The transaction is
`read_committed`, idempotent, and confined to the required one-partition
compacted recovery topic. A backup is successful only after that transaction
commits. Archives without a committed manifest are harmless orphans, not
recovery points.

All archives use unique UTC/pod-UID names beneath
`<pathPrefix>/chains/<chain-id>/`. `maxIncrementalsPerFull=N` produces one full
event-data backup, at most `N` incrementals, and then starts a new full chain.
`0` is the safe full-only default. An incremental points at the immediately
preceding event-data backup; restoring its tip follows the native ClickHouse
dependency chain. Every dependency must remain available.

The chart does not delete external object-store data. Retention must delete a
closed `chains/<chain-id>/` prefix as a unit, never individual members of a live
chain. Keep at least the number of complete chains required by the recovery
policy. Kafka's canonical topics, Streams changelogs, Connect internal topics,
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
canonical topic names and partition counts, target database/table names,
KeeperMap table names, and Kafka Connect internal-topic identities.

Restore into an empty ClickHouse database backed by an isolated Keeper:

1. Retrieve the chosen recovery manifest by its event-data backup ID from the
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

The recovery command requires `CONNECT_URL`, `CLICKHOUSE_URL`,
`CLICKHOUSE_USERNAME`, `CLICKHOUSE_PASSWORD`, `CONNECTOR_NAMES`,
`EXPECTED_BACKUP_NAME`, `RECOVERY_MANIFEST_FILE`, and
`STOP_TIMEOUT_SECONDS`. Before it touches Kafka Connect, it reads every restored
KeeperMap table and requires exact logical equality with the manifest.
It then patches the exact derived offsets through Kafka Connect's standard
offset API and reads them back for equality. It never resumes a connector. A
partial API failure leaves all connectors stopped and is safe to retry.

This ordering closes the dangerous cases: restoring data without its KeeperMap
checkpoint cannot rewind Kafka, and restoring the checkpoint while selecting a
different event-data archive is rejected by `EXPECTED_BACKUP_NAME`.

## Drill acceptance criteria

A restore drill is successful only when:

- the event-data backup and independent KeeperMap checkpoint both restore;
- restored row counts and content-level aggregates match the recovery point;
- the recovery command proves restored KeeperMap equality and exact Connect
  offset read-back;
- records retained after the recovery point replay exactly once;
- the old ClickHouse target stops advancing after cutover;
- the measured restore and replay time satisfies the RTO, and the selected
  recovery-point age satisfies the RPO.

The repository RKE2 E2E test performs this sequence with Redpanda, two isolated
ClickHouse/Keeper instances, and MinIO.
