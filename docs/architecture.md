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

The state table is created in the target database. Every backup manifest stores
its complete rows, Keeper path, and exact input offsets alongside the archive
identity.

## Backup metadata storage

Backup orchestration depends on the `BackupMetadataStorage` contract, which
acquires exclusive ownership of chain planning, loads the latest committed
chain state, and commits an immutable manifest with its replacement chain
state. Archive creation does not depend on Kafka record keys or serialization.

The initial `KafkaBackupMetadataStorage` implementation uses the configured
single-partition compacted topic. Its consumer-group assignment provides
exclusive backup ownership, and one Kafka transaction commits the UUID-keyed
manifest and chain state. The reserved chain-state key is private to this
implementation.

Other implementations may keep manifests in S3 and expose them through
Kubernetes resources, provided they also serialize concurrent backup writers
and never publish chain state that references an unavailable manifest.

## Why the target is not ReplacingMergeTree

`ReplacingMergeTree` resolves equal sorting keys during background merges.
Ordinary queries may observe both rows before a merge unless they use `FINAL`.
That is useful for current-state and CDC models, but it is not a transport
commit protocol.

The connector retries uncertain inserts as deterministic ClickHouse blocks, so
ClickHouse insert-block deduplication handles transport retries. Logical
duplicates already present in the input topic are separate records and remain
separate rows.

## Failure behavior

| Failure point | Durable state | Result |
| --- | --- | --- |
| Connect dies before ClickHouse acknowledgement | input topic and KeeperMap | deterministic block retry |
| Connect dies after insert but before offset commit | KeeperMap and ClickHouse block hash | inserted block is not duplicated |
| Backup Job fails before catalog commit | live tables and input topic | ingestion continues and no completed backup is published |

## Boundaries

The chart starts at Kafka. Producer durability, producer retry identity, and
logical-record identity belong upstream.
