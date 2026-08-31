# Backup procedure

A backup Job coordinates Kafka Connect, ClickHouse, Kafka, and object storage.
There is no transaction spanning those systems. The protocol therefore creates
an immutable ClickHouse snapshot during a short ingestion barrier, resumes
delivery, uploads that snapshot, and finally commits a backup manifest.

The backup-catalog transaction is the commit point. An archive is not a
completed backup until its manifest is committed to the backup catalog.
`BackupMetadataStorage` owns this commit boundary; the initial Kafka backend
implements it with the compacted catalog topic.

## Snapshot scope

One physical target table and every connector writing it form one consistency
unit. They are paused together. Unrelated tables are snapshotted and resumed
independently, so the protocol does not claim one globally atomic timestamp
across tables.

## Procedure

1. Load configuration, require every managed connector and task to be
   `RUNNING`, and validate the target and KeeperMap engines.
2. Join the backup catalog's single-partition consumer group. Holding its only
   partition serializes backup Jobs. Read and validate the current chain state.
3. Choose either a new full backup or the next incremental in the current chain.
4. Build deterministic physical-table snapshot groups. For each group:

   1. Request `PAUSED` only for connectors in that group and wait until their
      connector and task states are `PAUSED`. A paused task has completed its
      active `put` invocation.
   2. Read the observational Kafka Connect offsets and authoritative KeeperMap
      rows. The exact next input offset for partition `p` is:

      ```text
      KeeperMap["<input-topic>-<p>"].maxOffset + 1
      ```

      A missing row means offset zero. Reject unfinished rows, invalid or
      duplicate partitions, and Connect offsets ahead of KeeperMap.
   3. Verify every derived offset against Kafka's partition count and current
      log-start/log-end offsets.
   4. Create one copy-on-write `MergeTree` clone for each physical target table.
      ClickHouse clones immutable parts through hard links or object-storage
      metadata indirection; it does not copy the table's bytes during this
      barrier ([ClickHouse table cloning](https://clickhouse.com/blog/table-cloning)).
   5. Re-read the live KeeperMap rows and require every connector to remain
      paused. Any state movement invalidates the snapshot.
   6. Resume every connector in the group immediately.

5. Repeat the Kafka partition and retention checks for all captured offsets.
6. Back up only the immutable target clones to the configured S3 endpoint.
   Snapshot tables are mapped to their original logical names in the native
   archive, preserving incremental file identity across backup runs. The long
   compression and object-store transfer happens while ingestion is running.
7. Require `system.backups` to confirm the exact archive ID, destination, and
   `BACKUP_CREATED` status.
8. Build and validate the backup manifest. It contains every captured
   KeeperMap row, its Keeper path, exact input offsets, observed Connect
   offsets, and the target archive identity.
9. Repeat the Kafka partition and retention checks immediately before commit.
10. In one Kafka transaction, write the manifest keyed by the target backup ID
    and replace the chain-state record. This is the only success boundary.
11. Emit the committed manifest. The Job finalizer removes the temporary
    ClickHouse clones.

Connector resume is attempted after every group barrier and again when the Job
exits successfully, fails, receives `SIGINT`, or receives `SIGTERM`.

## Failure outcomes

- A failure before a group pauses leaves that group running.
- A failure inside a barrier creates no backup and triggers connector
  resume plus temporary-table cleanup.
- A failure after a group resumes cannot change its immutable clone. Later live
  inserts are deliberately outside that backup snapshot.
- A completed archive followed by failed catalog publication is an unreferenced
  object-store orphan, not a completed backup.
- A committed catalog transaction means the target archive was verified and
  the recorded offsets were still replayable immediately before commit.
- `SIGKILL`, node loss, or loss of the Connect API can prevent automatic resume;
  operators must alert on failed Jobs and paused connectors.
- Loss or unauthorized modification of the backup catalog destroys the
  authoritative KeeperMap checkpoint. Protect and retain that compacted topic.

The protocol cannot make Kafka retention, object storage, and ClickHouse
atomic. The backup therefore validates Kafka watermarks again immediately
before committing its manifest.

## Code map

| Function | Phase |
| --- | --- |
| `run` | Preconditions, backup lock, chain planning, signal handling, final resume and cleanup |
| `SnapshotLayout::new` | Physical-table grouping and deterministic clone names |
| `SnapshotLayout::capture_immutable_snapshots_during_short_ingestion_pauses` | Runs the short per-table barriers in step 4 and resumes each group |
| `snapshot::pause_ingestion_and_capture_snapshot` | Pauses and drains ingestion, captures state, clones the table, and proves the checkpoint stayed fixed |
| `snapshot::capture_checkpoints` and `backup::checkpoint` | KeeperMap-to-Kafka offset derivation |
| `KafkaLog::require_offsets_replayable` | Proves every recorded offset still exists in Kafka |
| `ClickHouse::clone_target` | Copy-on-write immutable part snapshot |
| `create_backup` | The complete capture, resume, upload, verification, manifest, and catalog-commit sequence |
| `BackupMetadataStorage` | Backend-neutral ownership, chain-state load, and completed-backup commit boundary |
| `KafkaBackupMetadataStorage` | Compacted-topic locking, lookup, and transactional publication |
| `validate_manifest` | Independently tested manifest validation |
