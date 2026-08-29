# Guarantees

## Logical-event boundary

For a configured horizon `D`, the first record observed for a Kafka key is
published to the canonical topic. The following cases are then distinguished:

- same key and byte-identical value during `D`: discarded as a retry;
- same key and different value during `D`: written to the conflict topic;
- same key after `D`: accepted as a new input under this bounded contract.

The raw topic uses broker append timestamps. The chart requires
`rawRetentionMs < deduplicationRetentionMs`, which ensures no record still
retained in the raw recovery log can outlive its deduplication entry.

The event ID must be globally stable for at least the full horizon. An empty key
or a producer that generates a new key on retry defeats logical deduplication.

## Transaction boundaries

Kafka Streams `exactly_once_v2` atomically commits the consumed raw offset, the
deduplication state update, and the canonical or conflict output. Its local
RocksDB store is a cache; Kafka's changelog is the recoverable copy.

The official ClickHouse sink establishes a second boundary. For each canonical
topic partition it stores the deterministic batch offset range and processing
state in a ClickHouse `KeeperMap` table. An uncertain insert is retried as the
same ClickHouse block, allowing ClickHouse insert-block deduplication to remove
the retry.

These are two coordinated exactly-once scopes, not a distributed transaction
from the producer's local storage through ClickHouse.

## What is and is not promised

The chart guarantees no duplicate canonical record within `D`, and no duplicate
ClickHouse insert caused by replay at either managed processing boundary,
provided all prerequisites and recovery identities are preserved.

It does not guarantee:

- durability before the producer receives a successful Kafka acknowledgement;
- permanent deduplication of an ID reused after `D`;
- semantic equivalence of differently serialized values;
- exactly-once side effects outside Kafka or ClickHouse;
- recovery after loss of both ClickHouse backups and the required Kafka log and
  internal topics;
- correctness after manually rewinding only Kafka consumer offsets or only the
  connector's KeeperMap state.

Required retention relationships are:

```text
maximum producer retry age < raw topic retention < deduplication retention
maximum ClickHouse recovery point age < canonical topic retention
```
