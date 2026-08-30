# Durable ClickHouse Sink

Durable ClickHouse Sink is a Helm chart for append-only delivery from a
Kafka-compatible log to ClickHouse. It prevents transport retries from creating
extra ClickHouse rows and provides bounded logical-event deduplication before
delivery.

It installs neither Kafka nor ClickHouse.

```text
producer -> raw topic -> Kafka Streams deduplicator -> canonical topic
                                                        |
                                         ClickHouse Kafka Connect
                                                        |
                                                   ClickHouse
```

The deduplicator treats the Kafka record key as an immutable event ID and the
value as opaque bytes. Kafka Streams `exactly_once_v2` commits the input offset,
state update, and output record in one Kafka transaction. The official
ClickHouse Kafka Connect sink uses deterministic retries and KeeperMap state at
the Kafka-to-ClickHouse boundary.

This is not `ReplacingMergeTree`-based eventual deduplication. Ordinary
`MergeTree` or `ReplicatedMergeTree` target tables remain append-only and are
query-correct without `FINAL`.

## Requirements

- a Kafka-protocol-compatible cluster with transaction support;
- ClickHouse with ClickHouse Keeper and the `KeeperMap` engine enabled;
- pre-created target databases and tables;
- a stable, non-empty Kafka key for every input record;
- persistent Kafka user, internal, and Streams changelog topics;
- an S3-compatible ClickHouse named collection when chart-managed backups are
  enabled.

## Build and install

The chart, images, validation manifests, and E2E environment are defined with
Nix. Nixidy renders the chart during validation.

```bash
nix build .#chart
helm upgrade --install durable result/durable-clickhouse-sink-*.tgz \
  --namespace durable-clickhouse-sink \
  --create-namespace \
  --values values.yaml
```

Minimal values:

```yaml
stateNamespace: production

kafka:
  bootstrapServers: redpanda.kafka.svc:9093
  existingSecret: durable-kafka-client

clickhouse:
  host: clickhouse.example.internal
  port: 8443
  secure: true
  database: durable_events
  credentialsSecret:
    name: durable-clickhouse-writer

pipelines:
  - name: events
    rawTopic: durable.events.raw
    canonicalTopic: durable.events.canonical
    conflictTopic: durable.events.conflicts
    table: events
    rawRetentionMs: 1209600000
    deduplicationRetentionMs: 2592000000
    canonicalRetentionMs: 7776000000
```

Optional scheduled backups use a ClickHouse S3 named collection configured on
the ClickHouse servers. Ceph RGW and MinIO use the same interface.

```yaml
backup:
  enabled: true
  schedule: "17 2 * * *"
  namedCollection: durable_clickhouse_backups
  pathPrefix: durable-events/production
  maxIncrementalsPerFull: 6
  credentialsSecret:
    name: durable-clickhouse-backup
    usernameKey: username
    passwordKey: password
```

Each run is a Kubernetes Job with `concurrencyPolicy: Forbid`. It verifies all
managed connector tasks are running, pauses and drains them, snapshots their
ClickHouse KeeperMap state, creates and verifies a direct S3 backup, and
publishes a recovery-point manifest to a compacted Kafka topic. Event tables use
one full backup followed by at most `maxIncrementalsPerFull` incremental
backups. Every point also contains an independent full KeeperMap checkpoint.
The safe default is `0`, which creates only full backups. Incremental mode
requires every target table to use a MergeTree-family engine.

The Job resumes connectors on success, failure, or normal pod termination. The
manifest is also printed in the successful Job's JSON output. See the recovery
documentation for the exact-offset restore procedure and hard-kill recovery
boundary.

The immutable `stateNamespace`, release name, pipeline name, topic names, and
Kafka Connect internal topics are recovery identities. Do not rename them as a
routine Helm change.

## Documentation

- [Architecture and failure boundaries](docs/architecture.md)
- [Guarantees](docs/guarantees.md)
- [Schema and event contract](docs/schema.md)
- [Authentication and credential rotation](docs/authentication.md)
- [Backup and recovery](docs/recovery.md)
- [Test plan](docs/testing.md)
- [External design references](docs/references.md)

## Status

This repository remains private while its security and operational contracts
are reviewed. It is intended to become an Apache-2.0 licensed OSS project.
