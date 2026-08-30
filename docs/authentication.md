# Authentication and credential rotation

The chart consumes existing Kubernetes Secrets and does not create credentials.
This keeps it compatible with Vault, External Secrets, Sealed Secrets, SOPS, or
an operator-specific secret controller.

## Kafka

`kafka.existingSecret` contains a Java properties file under
`kafka.propertiesKey`. The same file is used by Kafka Streams, Kafka Connect,
and topic-management jobs. It can contain TLS, SASL/SCRAM, or OAuth properties.

```properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="durable-sink" password="...";
ssl.truststore.location=/path/provided-by-a-custom-image
```

The Kafka principal needs read access to raw and canonical topics; write access
to canonical and conflict topics; transactional-ID access for each Streams
application; and access to the Streams changelog, repartition, Connect internal,
consumer-offset, and recovery-point resources it owns. The backup Job needs
write access to the recovery-point topic; recovery tooling needs read access.
Topic-management privileges are needed only when `topics.manage=true`.

## ClickHouse writer

`clickhouse.credentialsSecret` contains `clickhouse.properties`:

```properties
username=durable_sink
password=...
```

Use a dedicated writer with `INSERT` on target tables and `CREATE TABLE`,
`SELECT`, and `INSERT` for the KeeperMap state table in each target database.
Grant only the ClickHouse permissions required by the selected connector
version and validate them in a non-production database.

## Backup user and object storage

Backups use a separate `backup.credentialsSecret`. Its curl config is mounted
only into the CronJob:

```text
user = "durable_backup:..."
```

The backup identity needs the ClickHouse `BACKUP` and object-read permissions
for the configured databases. It should not be the ingestion writer.

RGW/S3 credentials are not passed through Helm. Configure
`backup.namedCollection` on every source and restore ClickHouse server. The
collection may use fixed access keys for a test such as MinIO, environment or
instance credentials where supported, or credentials managed by the
ClickHouse deployment.

## Rotation boundary

A projected Secret can update a file, but a running ClickHouse connector does
not reconstruct its client merely because the file changed. Credential rotation
must trigger a controlled Connect task restart before the old lease expires.
`connect.podAnnotations` and `deduplicator.podAnnotations` are available for a
provider-specific reload controller; the chart itself does not assume one.

Short-lived credentials are safe only when the external issuer, Secret sync,
and restart controller are tested together. Prefer renewable credentials whose
lease comfortably exceeds the maximum restart and incident-recovery time.
