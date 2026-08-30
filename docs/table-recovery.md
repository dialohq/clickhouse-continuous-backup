# Declarative table recovery

`TableRecovery` restores one physical table into a different, pre-created
table. The source recovery point and destination are immutable. The controller
never overwrites the source table and never creates the destination schema.

```yaml
apiVersion: chbackup.dialo.ai/v1alpha1
kind: TableRecovery
metadata:
  name: events-at-cutoff
spec:
  source:
    database: events
    table: records
    recoveryPointID: 9f55f211-78d2-43dc-a24b-f8545ec6a097
  destination:
    database: recovery
    table: records_at_cutoff
  targetOffsets:
    - topic: events.input
      partition: 0
      offset: 48102
    - topic: events.input
      partition: 1
      offset: 47791
```

Offsets are exclusive: `48102` means that offset `48101` is the final record
eligible for replay. An explicit vector must name every topic-partition that
wrote the source table exactly once. Every target must be at or after the
backup checkpoint and at or before the current Kafka log end.

Omit `targetOffsets` to restore the backup, start isolated follow connectors at
the manifest offsets, and continue consuming the live log. The terminal phase
is then `Streaming`. An explicit vector produces an exact finite table and
leaves isolated follow connectors `STOPPED` at that vector; its terminal phase
is `Complete`. Resuming those connectors is an explicit cutover decision.

## Procedure encoded by the controller

1. Read the backup manifest by `recoveryPointID` from the compacted backup
   catalog topic and validate its table identity, connector set, and exact
   KeeperMap-derived start offsets.
2. Validate that an explicit target vector is complete, monotonic from the
   checkpoint, and still present in Kafka. No ClickHouse data is changed before
   these checks pass.
3. Require the destination to exist, use a MergeTree-family engine, and be
   empty. Restore only the requested table from the selected ClickHouse backup
   with `RESTORE TABLE source AS destination`. The immutable Kubernetes UID is
   the ClickHouse restore-operation ID, allowing reconciliation to verify
   `RESTORED` in `system.backups` after a lost response.
4. For an explicit target, transactionally copy the raw Kafka range
   `[checkpoint, target)` to a controller-owned bounded topic. Each Kafka
   transaction commits copied records and the corresponding source consumer
   offset atomically. Consumers use `read_committed`, so a controller crash
   exposes either the whole batch or none of it.
5. Run an ordinary official ClickHouse sink connector against the bounded
   topic. Once every committed record is ingested, stop it. Aborted Kafka
   transaction slots are deliberately excluded from the expected connector
   offsets.
6. Create isolated KeeperMap state for the original source offsets and create a
   follow connector in Kafka Connect's `STOPPED` initial state. An empty
   isolated KeeperMap marks incomplete initialization: reconciliation stops the
   connector, writes its exact Kafka offsets, hydrates KeeperMap, and only then
   permits a resume. Recovery consumers use `auto.offset.reset=none`, so
   retention loss fails instead of silently skipping data. For an omitted
   target, initialization happens directly at the backup checkpoint and the
   controller then resumes the connector.
7. Publish the resolved start vector, resolved target vector, replay connector
   names, follow connector names, phase, and condition in `.status`.

The controller does not pause or mutate the production connectors. Recovery
connectors and Keeper paths are derived from the Kubernetes resource UID, so
two recovery drills cannot share offset state. Reconciliation is retryable:
topic names, transactional IDs, consumer groups, Keeper paths, state tables,
and connector names are deterministic.

## Operator workflow

1. Choose a recovery-point ID whose backup chain and Kafka range are retained.
2. Create the empty destination table through the normal schema-management
   tool. Use the intended production engine; archived clone metadata is not a
   schema template.
3. Apply `TableRecovery` and wait for `Complete` or `Streaming`.
4. Verify table aggregates, `.status.resolvedTargetOffsets`, connector states,
   and application-level invariants.
5. For finite PITR, keep follow connectors stopped while investigating. Resume
   only when the recovered table should advance beyond the selected point.

Deleting a `TableRecovery` does not delete recovered data or its follow
connectors. This is intentional: Kubernetes garbage collection must not become
a data-deletion API. Temporary replay topics use the configured Kafka retention
and stopped replay connectors remain available for audit.

## Fail-closed cases

The controller rejects missing or malformed manifests, a partial or duplicate
target vector, targets before the checkpoint or beyond a log end, retention
loss, changed partition counts, non-empty or incompatible destination tables,
conflicting KeeperMap rows, changed recovery-connector configuration, and a
bounded follow connector that moved beyond its target. A bounded replay topic
must retain its complete log until ingestion finishes; an advanced low
watermark makes the result ambiguous and fails the recovery.

Kafka consumer-group offsets are the durable progress record for bounded copy.
If that progress has expired while committed replay records remain, the
controller cannot distinguish a safe continuation from duplication and fails.
Keep Kafka group-offset retention longer than the maximum recovery duration and
the replay-topic retention. If a replay topic is recreated after source
progress was committed, recovery also fails rather than accepting missing data.

There is no transaction spanning ClickHouse restore, Kafka, Kafka Connect, and
the Kubernetes status subresource. The controller orders idempotent operations
and records phases before destructive ambiguity. If the destination becomes
non-empty while the resource still says `Restoring`, the controller continues
only when ClickHouse reports `RESTORED` for that resource's operation ID. A
missing or failed operation may be a partial restore and requires a new empty
destination and a new `TableRecovery`. External writes to a recovery
destination, manual connector resumes, KeeperMap edits, topic deletion, and
credential changes can also invalidate a run; the controller detects the
states it can prove unsafe and otherwise requires exclusive operational control
of recovery-owned resources.
