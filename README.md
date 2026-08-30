# Durable ClickHouse Sink

Durable ClickHouse Sink is a Helm chart for append-only delivery from a
Kafka-compatible log to ClickHouse, with coordinated incremental backup and
exact-offset disaster recovery. It installs neither Kafka nor ClickHouse.

```text
Kafka topic -> official ClickHouse Kafka Connect sink -> ClickHouse
                                                        |
                                  backup + recovery-point manifest
```

The official ClickHouse connector uses deterministic retries and KeeperMap
state at the Kafka-to-ClickHouse boundary. Target tables can remain ordinary
`MergeTree` or `ReplicatedMergeTree` tables and do not need query-time `FINAL`.

## Requirements

- a Kafka-protocol-compatible cluster;
- ClickHouse with ClickHouse Keeper and the `KeeperMap` engine enabled;
- pre-created target databases and tables;
- persistent Kafka input and Connect internal topics;
- an S3-compatible ClickHouse named collection when backups are enabled.

## Build and install

The chart, image, validation manifests, and E2E environment are defined with
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
  database: durable_data
  credentialsSecret:
    name: durable-clickhouse-writer

pipelines:
  - name: records
    topic: durable.records
    table: records
    retentionMs: 7776000000
```

Optional scheduled backups use a ClickHouse S3 named collection configured on
the ClickHouse servers. Ceph RGW and MinIO use the same interface.

```yaml
backup:
  enabled: true
  schedule: "17 2 * * *"
  namedCollection: durable_clickhouse_backups
  pathPrefix: durable-data/production
  maxIncrementalsPerFull: 6
  maxBandwidthBytesPerSecond: 0
  credentialsSecret:
    name: durable-clickhouse-backup
    usernameKey: username
    passwordKey: password
```

Network, Kafka, polling, hook, and backup deadlines have chart defaults under
`timeouts` and `backup`. Override them for the deployment's latency and backup
size; the recovery binary does not carry fallback durations of its own.

Each backup Job briefly pauses all connectors writing one physical table,
creates a copy-on-write target-table clone, captures and verifies their
KeeperMap state, and resumes them. Compression and S3 upload operate on the
immutable clones while ingestion is running. The manifest stores the exact
KeeperMap rows and Kafka offsets. Target tables use one full backup followed by
at most `maxIncrementalsPerFull` incrementals; `0` is the full-only default.

The immutable `stateNamespace`, release name, pipeline names, topic names, and
Kafka Connect internal topics are recovery identities. Do not rename them as a
routine Helm change.

## Documentation

- [Architecture and failure boundaries](docs/architecture.md)
- [Guarantees](docs/guarantees.md)
- [Failure modes and unhandled boundaries](docs/failure-modes.md)
- [Schema contract](docs/schema.md)
- [Authentication and credential rotation](docs/authentication.md)
- [Backup and recovery](docs/recovery.md)
- [Backup procedure](docs/backup-procedure.md)
- [Test plan](docs/testing.md)
- [External design references](docs/references.md)

## Status

This repository remains private while its security and operational contracts
are reviewed. It is intended to become an Apache-2.0 licensed OSS project.
