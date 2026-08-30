# Backup procedure

A backup Job coordinates Kafka Connect, ClickHouse, Kafka, and object storage.
None of those systems provides a transaction spanning all four, so the Job
creates immutable artifacts first and makes them recoverable with one final
Kafka transaction.

The recovery-catalog transaction is the commit point. A ClickHouse archive is
not a recovery point until its manifest is committed to the recovery topic.

## Procedure

1. Load configuration and require every managed connector and task to be
   `RUNNING`.
2. Join the recovery topic's single-partition consumer group. Holding its only
   partition serializes backup Jobs. Read and validate the current chain head.
3. Choose either a new full backup or the next incremental in the current chain.
4. Request `PAUSED` for every connector and wait until every connector and task
   reports `PAUSED`. A paused task has completed its active `put` call.
5. Validate the target table engines and their ClickHouse insert-deduplication
   settings.
6. For each pipeline, read the observational Kafka Connect offsets and the
   authoritative KeeperMap rows. Derive the next replay offset for partition
   `p` as:

   ```text
   KeeperMap["<input-topic>-<p>"].maxOffset + 1
   ```

   A missing row means offset zero. Reject unfinished rows, invalid partitions,
   duplicate partitions, or a Connect offset ahead of KeeperMap.
7. Verify that every derived offset is still between Kafka's log-start and
   log-end offsets and that the topic partition count has not changed.
8. Create and verify the full or incremental backup of the configured target
   tables.
9. Create and verify a separate full backup of every KeeperMap state table.
   KeeperMap never uses the target-data incremental chain because its
   append-only storage is unsafe to combine with incomplete incremental-base
   coverage ([ClickHouse issue #112403](https://github.com/ClickHouse/ClickHouse/issues/112403)).
10. Require every connector to remain paused and compare the live KeeperMap rows
    with the rows captured in step 6. Any movement invalidates the candidate.
11. Build and validate the recovery manifest, then repeat the Kafka retention
    and partition checks from step 7.
12. In one Kafka transaction, write the manifest keyed by the target-data backup
    ID and replace the chain-head record. This transaction is the only success
    boundary.
13. Emit the committed recovery point as the Job output.
14. Resume all connectors whether the procedure succeeded, failed, or received
    `SIGINT`/`SIGTERM`.

## Artifacts and ownership

| Artifact | Contents | Recovery role |
| --- | --- | --- |
| Target-data archive | Configured MergeTree-family tables | Restores rows at the recovery point |
| KeeperMap checkpoint | Connector processing state | Proves the exact next Kafka offset |
| Recovery manifest | Archive identities, KeeperMap rows, exact and observed offsets | Binds data, state, and replay position |
| Chain head | Full base, latest archive, generation, pipeline identity | Selects the next full or incremental backup |

Archive names contain the UTC time and Kubernetes Job UID. Target-data
incrementals point to the immediately preceding archive. Retention must delete a
closed chain as a unit because any missing base makes later incrementals
unrestorable.

## Failure outcomes

- Failure before connector pause leaves delivery running.
- Failure while paused creates no committed recovery point. The Job attempts to
  resume every connector.
- Successful archives followed by a failed catalog transaction are unreferenced
  object-store orphans and must not be offered for recovery.
- A committed catalog transaction means both verified archives exist and the
  recorded offsets were replayable immediately before commit.
- `SIGKILL`, node loss, or loss of the Connect API can prevent automatic resume;
  operators must alert on failed Jobs and paused connectors.

The protocol cannot make Kafka retention, object storage, and ClickHouse
atomic. Restore therefore revalidates the KeeperMap checkpoint and Kafka
watermarks before changing Kafka Connect offsets.

## Code map

The orchestration in `backup/src/backup.rs` follows the numbered procedure:

| Function | Phase |
| --- | --- |
| `run` | Preconditions, backup lock, chain planning, signal handling, unconditional resume |
| `create_recovery_point` | Steps 4–13 in protocol order |
| `pause_delivery` | Step 4 |
| `capture_checkpoints`, `checkpoint`, and `KafkaLog::verify` | Steps 6–7 and 11 |
| `create_archives` | Steps 8–9 |
| `require_stable_checkpoint` | Step 10 |
| `build_recovery_point` and `validate_recovery_point` | Step 11 |
| `commit_recovery_point` | Step 12, the commit point |
| `print_output` | Step 13 |
