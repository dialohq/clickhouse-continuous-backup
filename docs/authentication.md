# Authentication and credential rotation

The chart consumes existing Kubernetes Secrets and does not create credentials.
This keeps it compatible with Vault, External Secrets, Sealed Secrets, SOPS, or
an operator-specific secret controller.

## Kafka

`kafka.existingSecret` contains a Java properties file under
`kafka.propertiesKey`. Kafka Connect and topic-management jobs use that file.
It can contain TLS, SASL/SCRAM, or OAuth properties.

```properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="durable-sink" password="...";
ssl.truststore.location=/path/provided-by-a-custom-image
```

When backups are enabled, the same Secret must also contain a librdkafka-format
file under `backup.kafkaPropertiesKey` (`librdkafka.properties` by default).
Keeping separate keys avoids lossy translation of Java JAAS, truststore, and
OAuth settings. For SASL/SCRAM it commonly contains:

```properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.username=durable-sink
sasl.password=...
ssl.ca.location=/etc/ssl/certs/ca-bundle.crt
```

The Kafka principal needs read access to configured input topics and access to
the Connect internal, consumer-offset, and recovery-point resources it owns.
The backup Job needs
read/write access to the recovery-point topic, transactional-ID access for its
manifest transaction, and consumer-group access to the topic-derived
`<recovery-topic>.backup-lock` group. Recovery tooling needs read access.
Topic-management privileges are needed only when `topics.manage=true`.

## ClickHouse writer

`clickhouse.credentialsSecret` contains `clickhouse.properties`:

```properties
username=durable_sink
password=...
```

Use a dedicated writer with `INSERT` on target tables and `CREATE TABLE`,
`SELECT`, and `INSERT` for the KeeperMap state table in each target database.
Registration preflight also reads `system.tables` and
`system.merge_tree_settings`. Grant only the ClickHouse permissions required by
the selected connector version and validate them in a non-production database.

## Backup user and object storage

Backups use a separate `backup.credentialsSecret`. Its `usernameKey` and
`passwordKey` are exposed only to each CronJob pod:

```yaml
stringData:
  username: durable_backup
  password: ...
```

The backup identity needs `SELECT` on target and KeeperMap tables, `CREATE
TABLE` and `DROP TABLE` for temporary copy-on-write snapshots, access to
`system.backups` and `system.tables`, and ClickHouse's `BACKUP` permission for
the snapshot objects. It also reads `system.merge_tree_settings` to reject a
disabled effective replicated deduplication window. The recovery identity needs
`CREATE TABLE`, `SELECT`, and `INSERT` for the restored KeeperMap tables. Neither
identity should be the ingestion writer.

RGW/S3 credentials are not passed through Helm. Configure
`backup.namedCollection` on every source and restore ClickHouse server. The
collection may use fixed access keys for a test such as MinIO, environment or
instance credentials where supported, or credentials managed by the
ClickHouse deployment.

## Rotation boundary

A new CronJob pod reads current Kafka and ClickHouse Secret values, so rotating
backup credentials does not require changing the chart. A projected Secret can
update a file, but a running ClickHouse connector does
not reconstruct its client merely because the file changed. Credential rotation
must trigger a controlled Connect task restart before the old lease expires.
`connect.podAnnotations` is available for a provider-specific reload
controller; the chart itself does not assume one.

Short-lived credentials are safe only when the external issuer, Secret sync,
and restart controller are tested together. Prefer renewable credentials whose
lease comfortably exceeds the maximum restart and incident-recovery time.
