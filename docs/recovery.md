# Backup and recovery

## Backup model

The optional CronJob pauses every managed connector, waits until every task is
paused, snapshots the committed offsets with Kafka Connect's standard offset
API, synchronously backs up all target databases, and publishes a recovery
manifest keyed by the ClickHouse backup ID. It then resumes the connectors. It
refuses to start unless every connector and task was already running, so it
cannot accidentally resume an operator-paused connector.
Every archive name contains both a UTC timestamp and the backup pod UID, so a
Job retry cannot overwrite a previous attempt. The Job accepts a backup only
after `system.backups` confirms the exact returned backup ID, destination, and
`BACKUP_CREATED` status. A run is successful only after its recovery manifest
has also been acknowledged by Kafka with `acks=all` and an idempotent producer.

The recovery manifest is a versioned JSON value in the release's compacted
`<fullname>.recovery-points` topic. It contains the exact ClickHouse backup name
and committed topic/partition offsets for every connector. The successful Job
emits the same manifest in its JSON output. Kafka is the right home for the
manifest: restoring a tail requires the canonical Kafka log anyway, and no
second copy of the S3/RGW credentials has to be exposed to this chart.

The backup process traps normal exit, `SIGINT`, and `SIGTERM`, terminates an
in-flight ClickHouse request, and resumes every managed connector. No process
can recover from `SIGKILL`, abrupt node loss, or loss of network access to
Connect. Alert on failed backup Jobs and paused connectors. After such a
failure, verify that no backup is still running in ClickHouse, then resume the
chart-managed connectors through the Connect API before retrying the Job.

Backups use ClickHouse's direct `S3(named_collection, path)` engine. They do not
use an S3-backed `Disk`: disk metadata can be local to one ClickHouse server,
which makes a bucket object insufficient for a clean-cluster restore. The named
collection is configured on ClickHouse, outside this chart, and owns RGW/S3
authentication.

The backup includes both target tables and each connector's KeeperMap table.
Kafka user topics, Streams changelogs, internal topics, and recovery-point topic
are not ClickHouse data and require the Kafka platform's own replication and
disaster-recovery policy. The offset manifest can rebuild a lost Connect
consumer position, but it cannot recreate canonical records that Kafka no
longer retains.

## Supported cutover

The restore contract preserves all of these identities:

- Helm release name;
- `stateNamespace` and pipeline names;
- canonical topic names and partition counts;
- Kafka Connect group and internal topics;
- connector names;
- restored Keeper paths and state tables.

Use an isolated ClickHouse and Keeper for the restore drill. Configure the same
S3 named collection there, restore the database, verify the restored rows and
KeeperMap tables, then update the existing Helm release to point at the restored
ClickHouse. This preserves Kafka Connect's committed offsets while the restored
KeeperMap state preserves the ClickHouse side of the protocol.

A fresh Helm release starts with fresh Kafka Connect internal topics and offsets.
Starting it at offset zero against a restored KeeperMap checkpoint is unsafe:
old batches can precede the stored range and the official connector correctly
stops on the mismatch. Use the recovery manifest to restore the matching
position instead.

Kafka Connect requires a connector to be stopped before its offsets can be
altered. After restoring the ClickHouse archive and before allowing delivery to
the restored target:

1. Retrieve the manifest whose key is the ClickHouse backup ID from the
   recovery-point topic.
2. Stop every connector with `PUT /connectors/<name>/stop` and wait for
   `STOPPED`.
3. Run `/bin/durable-clickhouse-restore-offsets` from the chart image with the
   manifest on stdin and `RECOVERY_MANIFEST_FILE=-`.
4. Verify the utility succeeds. It applies offsets through `PATCH
   /connectors/<name>/offsets` and reads them back before returning.
5. Point the release at the restored ClickHouse and resume the connectors.

The utility requires `EXPECTED_BACKUP_NAME` and the exact release connector
list, preventing an offset snapshot from a different archive or release from
being applied. It waits up to `STOP_TIMEOUT_SECONDS` (120 by default) for
asynchronous task shutdown, then requires read-back equality after every patch.
It never resumes connectors. A partial API failure leaves all connectors
stopped and is safe to retry.

## Drill sequence

1. Verify Kafka and connector health and create an on-demand job from the backup
   CronJob.
2. Record the backup name printed by the successful job.
3. Restore into a clean database on a ClickHouse instance with an isolated
   Keeper.
4. Validate schema, row counts, unique event IDs, KeeperMap tables, and the
   latest event timestamp.
5. Stop writes or preserve the canonical log while performing the cutover.
6. Stop the connectors and restore the manifest offsets through the packaged
   recovery utility.
7. Upgrade the same Helm release with the restored ClickHouse host and database.
8. Publish canary records, confirm they appear exactly once on the restored
   target, and confirm the old target no longer advances.
9. Roll back the endpoint only while both sides and Kafka offsets still satisfy
   the same checkpoint contract.

ClickHouse recommends automating restores and practicing them regularly on a
spare cluster. The repository E2E test executes this sequence with RKE2,
Redpanda, two independent ClickHouse/Keeper instances, and MinIO.
