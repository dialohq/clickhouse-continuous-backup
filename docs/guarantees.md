# Guarantees

## Delivery boundary

For every Kafka record consumed by a managed connector, the official
ClickHouse sink records the deterministic batch offset range and processing
state in a ClickHouse `KeeperMap` table. An uncertain insert is retried as the
same ClickHouse block, allowing ClickHouse insert-block deduplication to remove
the transport retry.

At a recovery point, connectors are drained and paused. The manifest derives
the next Kafka offset from the backed-up KeeperMap `maxOffset + 1`, not from a
potentially lagging Kafka Connect commit. Event data and a full KeeperMap
checkpoint are backed up separately. Recovery verifies the restored KeeperMap
against the manifest before patching Connect and verifies Connect's read-back
before it may be resumed.

This is an exactly-once delivery boundary for Kafka records, not a distributed
transaction from a producer's local storage through ClickHouse.

## What is and is not promised

Subject to the documented prerequisites, the chart prevents an uncertain
Kafka-to-ClickHouse transport retry or exact-offset disaster-recovery replay
from creating an extra target row.

It does not guarantee:

- durability before a producer receives a successful Kafka acknowledgement;
- deduplication of two Kafka records that represent the same logical event;
- exactly-once side effects outside Kafka or ClickHouse;
- recovery after loss of both ClickHouse backups and the required Kafka input
  and internal topics;
- correctness after manually rewinding only Connect offsets or only KeeperMap;
- safe incremental backup of target engines outside the MergeTree family;
- automatic object-store retention or deletion of incremental dependencies.

Input topic retention must exceed the maximum age of a recovery point that may
be restored plus the time required to detect the incident, restore ClickHouse,
and replay the tail.
