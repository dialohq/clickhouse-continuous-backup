# Architecture

## Components

The chart runs one Kafka Streams application per pipeline and one distributed
Kafka Connect cluster per Helm release. Target schemas, Kafka-compatible
brokers, ClickHouse, Keeper, and object storage remain external.

The raw topic is the producer-facing retry boundary. The deduplicator stores a
SHA-256 digest and timestamp for each key in a persistent window store. Its
canonical and conflict outputs are produced with Kafka transactions. Consumers
of those topics use `read_committed` isolation.

The canonical topic is the ClickHouse delivery journal. The official ClickHouse
connector is configured with `exactlyOnce=true`, no connector-side buffering,
and `wait_for_async_insert=1`. Safety-critical connector settings cannot be
overridden through `connectorConfig`.

Each connector uses a stable Keeper path and state table derived from
`stateNamespace` and pipeline name:

```text
/durable-clickhouse-sink/<stateNamespace>/<pipeline>
durable_sink_<stateNamespace>_<pipeline>_state
```

The state table is created in the target database and must be included in its
backup.

## Why the target is not ReplacingMergeTree

`ReplacingMergeTree` resolves rows with the same sorting key during background
merges. Until a merge occurs, ordinary queries can observe duplicates unless
they use `FINAL`. That is useful for current-state and CDC models, but it is not
a transport commit protocol.

This chart deduplicates logical producer retries before the canonical log and
deduplicates deterministic ClickHouse insert retries at the insertion boundary.
The target can therefore remain a normal append-only MergeTree-family table.

## Failure behavior

| Failure point | Recovery source | Result |
| --- | --- | --- |
| producer retries after uncertain Kafka acknowledgement | raw topic key and bounded state | one canonical record |
| deduplicator dies before transaction commit | raw topic | state and output are both retried |
| deduplicator dies after transaction commit | Kafka transaction | committed output is visible once |
| Connect dies before ClickHouse acknowledgement | canonical topic and KeeperMap | deterministic block retry |
| Connect dies after insert but before Kafka offset commit | KeeperMap and ClickHouse block hash | inserted block is not duplicated |
| local deduplicator volume is lost | Streams changelog | state is restored before readiness |
| ClickHouse data loss | S3 backup, recovery manifest, and retained canonical log | rewind and deterministic tail replay |

## Producer boundary

The chart deliberately starts at Kafka. A producer may use a local file,
SQLite, an application outbox, or another durable spool, but it must retain the
record until Kafka acknowledges it with the durability policy required by the
deployment. That producer-to-Kafka transaction belongs in the producing
application because this chart cannot atomically modify application state.
