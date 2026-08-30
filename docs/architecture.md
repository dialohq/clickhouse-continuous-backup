# Architecture

## Components

The chart runs one distributed Kafka Connect cluster per Helm release and one
official ClickHouse sink connector per pipeline. Target schemas,
Kafka-compatible brokers, ClickHouse, Keeper, and object storage remain
external.

Each configured topic is a ClickHouse delivery journal. The connector uses
`exactlyOnce=true`, `read_committed` isolation, no connector-side buffering,
and synchronous inserts with `insert_deduplicate=1`. Registration rejects
targets whose MergeTree-family insert-deduplication window is explicitly
disabled. Safety-critical settings cannot be overridden through
`connectorConfig`.

Each connector uses a stable Keeper path and state table derived from
`stateNamespace` and pipeline name:

```text
/durable-clickhouse-sink/<stateNamespace>/<pipeline>
durable_sink_<stateNamespace>_<pipeline>_state
```

The state table is created in the target database. Every recovery point stores
it in an independent full checkpoint; it is never part of the incremental event
table chain.

## Why the target is not ReplacingMergeTree

`ReplacingMergeTree` resolves equal sorting keys during background merges.
Ordinary queries may observe both rows before a merge unless they use `FINAL`.
That is useful for current-state and CDC models, but it is not a transport
commit protocol.

The connector retries uncertain inserts as deterministic ClickHouse blocks, so
ClickHouse insert-block deduplication handles transport retries. Logical
duplicates already present in the input topic are separate records and remain
separate rows. If that is not desired, place the independent Kafka Event
Deduplicator before this chart.

## Failure behavior

| Failure point | Recovery source | Result |
| --- | --- | --- |
| Connect dies before ClickHouse acknowledgement | input topic and KeeperMap | deterministic block retry |
| Connect dies after insert but before offset commit | KeeperMap and ClickHouse block hash | inserted block is not duplicated |
| ClickHouse data loss | event backup, full KeeperMap checkpoint, recovery manifest, and retained input log | verified exact-offset tail replay |

## Boundaries

The chart starts at Kafka. Producer durability, producer retry identity, and
logical-event deduplication belong upstream. The chart neither installs nor
calls the Kafka Event Deduplicator; composition is performed by setting this
chart's `pipelines[].topic` to the deduplicator's output topic.
