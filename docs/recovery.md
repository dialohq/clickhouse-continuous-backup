# Backup and recovery

## Backup model

The optional CronJob pauses every managed connector, waits until every task is
paused, synchronously backs up all target databases, and then resumes the
connectors. It refuses to start unless every connector and task was already
running, so it cannot accidentally resume an operator-paused connector.
Every archive name contains both a UTC timestamp and the backup pod UID, so a
Job retry cannot overwrite a previous attempt. The Job accepts a backup only
after `system.backups` confirms the exact returned backup ID, destination, and
`BACKUP_CREATED` status.

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
Kafka user topics, Streams changelogs, Kafka Connect internal topics, and
consumer offsets are not ClickHouse data and require the Kafka platform's own
replication and disaster-recovery policy.

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
Starting that release at offset zero against a restored KeeperMap checkpoint is
not a supported recovery path: old batches can precede the stored range and the
official connector correctly stops on the mismatch. Parallel restore testing
therefore requires an explicit Kafka Connect offset export/import procedure,
which this chart does not currently automate.

## Drill sequence

1. Verify Kafka and connector health and create an on-demand job from the backup
   CronJob.
2. Record the backup name printed by the successful job.
3. Restore into a clean database on a ClickHouse instance with an isolated
   Keeper.
4. Validate schema, row counts, unique event IDs, KeeperMap tables, and the
   latest event timestamp.
5. Stop writes or preserve the canonical log while performing the cutover.
6. Upgrade the same Helm release with the restored ClickHouse host and database.
7. Publish canary records, confirm they appear exactly once on the restored
   target, and confirm the old target no longer advances.
8. Roll back the endpoint only while both sides and Kafka offsets still satisfy
   the same checkpoint contract.

ClickHouse recommends automating restores and practicing them regularly on a
spare cluster. The repository E2E test executes this sequence with RKE2,
Redpanda, two independent ClickHouse/Keeper instances, and MinIO.
