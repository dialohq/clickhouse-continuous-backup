# Failure modes

The sink fails closed whenever it can prove that delivery or recovery is no
longer safe. A failed check does not authorize an operator to bypass it.

## Detected or contained

| Condition | Behavior |
| --- | --- |
| Missing target table, unsupported engine, or disabled insert deduplication | Connector registration and backup preflight fail. Managed connectors use synchronous inserts. |
| Missing connector/task, task failure, incomplete pause, or KeeperMap movement during archive creation | Backup fails before publishing a recovery point. `SIGINT` and `SIGTERM` trigger a best-effort resume of every managed connector. |
| KeeperMap row is unfinished, malformed, duplicated, or outside the configured topic partitions | Backup and restore fail. |
| Kafka Connect reports an offset ahead of ClickHouse KeeperMap | Backup fails; KeeperMap remains authoritative. |
| Input topic is missing, has a different partition count, was truncated past a saved offset, or has a high watermark below it | Backup checks before and after the archive work; restore checks before changing Connect. The operation fails. |
| Recovery topic has more than one partition | The backup lock and catalog refuse to operate. The managed-topic Job also verifies its partition count. |
| Another release or pipeline topology reuses the recovery topic | The versioned chain head scope differs and backup fails before pausing connectors. |
| Event backup succeeds but checkpoint, validation, or catalog publication fails | No recovery point is committed. Completed S3 objects are unreferenced orphans and delivery resumes when Connect is reachable. |
| Manifest is malformed, names another backup, duplicates partitions/connectors, or disagrees with restored KeeperMap | Restore fails before changing offsets. |
| One of several Connect offset updates fails | Every connector remains stopped. The restore command is retryable and verifies all offset read-backs before returning success. |
| Object-store or ClickHouse backup error | `system.backups` must report the exact ID, destination, sizes, and `BACKUP_CREATED`; otherwise no manifest is committed. |
| Incremental chain limit is reached | The next point starts a new full chain. KeeperMap is always a separate full checkpoint. |

## Boundaries that cannot be made atomic

Kafka, Kafka Connect, ClickHouse, and S3 expose no common transaction. The
protocol therefore cannot atomically pause Connect, create two ClickHouse
archives, test Kafka retention, and publish the Kafka manifest. It orders and
rechecks those actions so every incomplete outcome is either retryable or an
unreferenced archive. Kafka retention can still advance in the interval between
the final watermark check and manifest commit. Prevent that by provisioning and
monitoring retention headroom; do not choose a recovery point whose offsets are
no longer present.

Likewise, Kafka Connect has no transaction spanning offset updates for several
connectors. Recovery keeps all of them stopped, applies idempotent exact values,
and verifies the complete result. A failed partial update must be retried before
any connector is resumed. The command rechecks `STOPPED` before validation,
before offset mutation, and after read-back, but it cannot prevent another
administrator from racing it through the Connect API. Recovery requires
exclusive operational control of that API.

No process can clean up after `SIGKILL`, node loss, or losing network access to
Connect. A connector can remain paused; alert on failed Jobs and non-running
connectors. The chart cannot restore data after simultaneous loss of the
required Kafka records, ClickHouse backups, and recovery catalog.

## Operational limits

- ClickHouse insert-block deduplication has a finite window. The chart rejects a
  zero window but cannot prove that a chosen positive window is large enough.
  Size it for the maximum uncertain retry backlog and prevent unrelated writers
  from evicting connector block IDs.
- The guarantee covers byte-identical connector retries. It does not merge two
  logical events, repair producer loss before Kafka acknowledgement, or cover
  side effects outside Kafka and ClickHouse.
- Schema changes, materialized-view behavior, mutations, deletes, manual offset
  rewinds, manual KeeperMap edits, topic recreation, and ClickHouse corruption
  are outside the automated protocol. Re-run a restore drill after relevant
  version or schema changes.
- Backup dependencies and the recovery catalog must outlive the recovery
  points that reference them. Object-store lifecycle policy is external to the
  chart; delete only closed incremental-chain prefixes.
- Credentials and ACLs can expire or be revoked while a Job is running. The Job
  fails and a new pod reads the rotated Secret; Connect workers require a
  rollout to reload file-provider credentials.

The exact contract and drill procedure are in [Guarantees](guarantees.md),
[Backup and recovery](recovery.md), and [Test plan](testing.md).
