# Failure modes

The sink fails closed whenever it can prove that delivery or backup creation is no
longer safe. A failed check does not authorize an operator to bypass it.

## Detected or contained

| Condition | Behavior |
| --- | --- |
| Missing target table, unsupported engine, or disabled insert deduplication | Connector registration and backup preflight fail. Managed connectors use synchronous inserts. |
| Missing connector/task, task failure, incomplete pause, or KeeperMap movement during the short clone barrier | Backup fails before publishing a manifest. `SIGINT` and `SIGTERM` trigger a best-effort resume of every managed connector. |
| KeeperMap row is unfinished, malformed, duplicated, or outside the configured topic partitions | Backup fails. |
| Kafka Connect reports an offset ahead of ClickHouse KeeperMap | Backup fails; KeeperMap remains authoritative. |
| Input topic is missing, has a different partition count, was truncated past a saved offset, or has a high watermark below it | Backup checks before and after the archive work and fails. |
| Backup catalog topic has more than one partition | The backup lock and catalog refuse to operate. The managed-topic Job also verifies its partition count. |
| Another release or pipeline topology reuses the backup catalog | The chain head scope differs and backup fails before pausing connectors. |
| Target-data backup succeeds but validation or catalog publication fails | No backup manifest is committed. Completed S3 objects are unreferenced orphans. Ingestion is already running. |
| Object-store or ClickHouse backup error | `system.backups` must report the exact ID, destination, sizes, and `BACKUP_CREATED`; otherwise no manifest is committed. |
| Incremental chain limit is reached | The next point starts a new full chain. |
| Declarative recovery target is partial, duplicated, before the checkpoint, beyond a log end, or removed by retention | The controller fails before restoring data. |
| Bounded-copy consumer progress expires while committed replay records remain, or its replay topic is recreated after progress | Recovery fails closed instead of risking a gap or duplicate copy. |
| Source retention passes the next transactional-copy offset, or bounded-topic retention advances before ingestion is verified | Recovery fails instead of accepting an incomplete destination. Recovery connectors disable automatic offset reset. |
| Controller or broker dies during bounded copy | Kafka aborts the open transaction; `read_committed` hides it and reconciliation resumes from the atomically committed source offset. |
| A destination is non-empty while its resource remains in `Restoring` | The controller continues only if `system.backups` confirms `RESTORED` for the resource UID. Missing or failed operation evidence fails closed; use a new empty destination and resource. |

## Boundaries that cannot be made atomic

Kafka, Kafka Connect, ClickHouse, and S3 expose no common transaction. The
protocol therefore cannot atomically snapshot ClickHouse, create an archive,
test Kafka retention, and publish the Kafka manifest. It orders and
rechecks those actions so every incomplete outcome is either retryable or an
unreferenced archive. Kafka retention can still advance in the interval between
the final watermark check and manifest commit. Prevent that by provisioning and
monitoring retention headroom; do not retain a backup checkpoint whose offsets
are no longer present if exact input position matters to downstream tooling.

No process can clean up after `SIGKILL`, node loss, or losing network access to
Connect. A connector can remain paused; alert on failed Jobs and non-running
connectors.

## Operational limits

- ClickHouse insert-block deduplication has a finite window. The chart rejects a
  zero window but cannot prove that a chosen positive window is large enough.
  Size it for the maximum uncertain retry backlog and prevent unrelated writers
  from evicting connector block IDs.
- The guarantee covers byte-identical connector retries. It does not merge two
  logical records, repair producer loss before Kafka acknowledgement, or cover
  side effects outside Kafka and ClickHouse.
- Schema changes, materialized-view behavior, mutations, deletes, manual offset
  rewinds, manual KeeperMap edits, topic recreation, and ClickHouse corruption
  are outside the automated protocol. Re-run archive validation after relevant
  schema changes.
- Backup dependencies and the backup catalog must outlive the manifests that
  reference them. Object-store lifecycle policy is external to the
  chart; delete only closed incremental-chain prefixes.
- Credentials and ACLs can expire or be revoked while a Job is running. The Job
  fails and a new pod reads the rotated Secret; Connect workers require a
  rollout to reload file-provider credentials.
- `Complete` and `Streaming` are terminal reconciliation phases, not continuous
  Kafka Connect health checks. Monitor connector/task state and lag separately.
  Manual changes made after a terminal phase are not reverted by the
  controller.

The exact contract and drill procedure are in [Guarantees](guarantees.md),
[Backup procedure](backup-procedure.md), and [Test plan](testing.md).
