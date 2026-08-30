# Backup procedure

A backup Job coordinates Kafka Connect, ClickHouse, Kafka, and object storage.
There is no transaction spanning those systems. The protocol therefore creates
an immutable ClickHouse snapshot during a short ingestion barrier, resumes
delivery, uploads that snapshot, and finally commits a recovery manifest.

The recovery-catalog transaction is the commit point. An archive is not a
recovery point until its manifest is committed to the recovery topic.

## Snapshot scope

One physical target table and every connector writing it form one consistency
unit. They are paused together. Unrelated tables are snapshotted and resumed
independently, so the protocol does not claim one globally atomic timestamp
across tables.

## Procedure

1. Load configuration, require every managed connector and task to be
   `RUNNING`, and validate the target and KeeperMap engines.
2. Join the recovery topic's single-partition consumer group. Holding its only
   partition serializes backup Jobs. Read and validate the current chain head.
3. Choose either a new full backup or the next incremental in the current chain.
4. Build deterministic physical-table snapshot groups. For each group:

   1. Request `PAUSED` only for connectors in that group and wait until their
      connector and task states are `PAUSED`. A paused task has completed its
      active `put` invocation.
   2. Read the observational Kafka Connect offsets and authoritative KeeperMap
      rows. The exact next replay offset for partition `p` is:

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
8. Build and validate the recovery manifest. It contains every captured
   KeeperMap row, its Keeper path, exact replay offsets, observed Connect
   offsets, and the target archive identity.
9. Repeat the Kafka partition and retention checks immediately before commit.
10. In one Kafka transaction, write the manifest keyed by the target backup ID
    and replace the chain-head record. This is the only success boundary.
11. Emit the committed manifest. The Job finalizer removes the temporary
    ClickHouse clones.

Connector resume is attempted after every group barrier and again when the Job
exits successfully, fails, receives `SIGINT`, or receives `SIGTERM`.

## Recovery

1. Create the destination database and empty target tables with the desired
   production engines using the normal schema tool.
2. Restore target data into those empty tables with ClickHouse
   `allow_different_table_def = true`. The archive contains non-replicated
   snapshot metadata; the pre-created destination retains its intended engine.
3. Stop every connector in the recovery manifest.
4. Validate the manifest and prove Kafka can still serve every exact offset.
5. Create each missing KeeperMap table with the connector's upstream schema and
   recorded Keeper path.
6. Rehydrate the manifest rows. Existing exact subsets are completed, making
   interrupted recovery retryable; conflicting or extra rows are rejected.
7. Read KeeperMap back and require exact equality with the manifest.
8. Patch Kafka Connect offsets, read them back, and require exact equality.
9. Resume connectors only after the recovery command succeeds.

## Failure outcomes

- A failure before a group pauses leaves that group running.
- A failure inside a barrier creates no recovery point and triggers connector
  resume plus temporary-table cleanup.
- A failure after a group resumes cannot change its immutable clone. Later live
  inserts are deliberately outside that recovery point.
- A completed archive followed by failed catalog publication is an unreferenced
  object-store orphan, not a recovery point.
- A committed catalog transaction means the target archive was verified and
  the recorded offsets were still replayable immediately before commit.
- `SIGKILL`, node loss, or loss of the Connect API can prevent automatic resume;
  operators must alert on failed Jobs and paused connectors.
- Loss or unauthorized modification of the recovery catalog destroys the
  authoritative KeeperMap checkpoint. Protect and retain that compacted topic.

The protocol cannot make Kafka retention, object storage, and ClickHouse
atomic. Recovery therefore validates Kafka watermarks before it writes
KeeperMap state or changes Kafka Connect offsets.

## Code map

| Function | Phase |
| --- | --- |
| `run` | Preconditions, backup lock, chain planning, signal handling, final resume and cleanup |
| `SnapshotLayout::new` | Physical-table grouping and deterministic clone names |
| `SnapshotLayout::create` | Short per-group barriers in step 4 |
| `snapshot::capture_checkpoints` and `backup::checkpoint` | KeeperMap-to-Kafka offset derivation |
| `ClickHouse::clone_target` | Copy-on-write immutable part snapshot |
| `create_archives` | Long target snapshot upload after resume |
| `build_recovery_point` and `validate_recovery_point` | Manifest construction and validation |
| `commit_recovery_point` | Step 10, the commit point |
| `restore::missing_keeper_rows` | Idempotent KeeperMap rehydration planning |
