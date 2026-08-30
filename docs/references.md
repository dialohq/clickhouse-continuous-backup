# External design references

The implementation and review should be read against the upstream contracts,
not only this repository's summaries.

## Processing and delivery

- [Kafka Connect user guide and REST API](https://kafka.apache.org/42/kafka-connect/)
- [Kafka Connect offset-management API (KIP-875)](https://cwiki.apache.org/confluence/display/KAFKA/KIP-875%3A+First-class+Offsets+Support+in+Kafka+Connect)
- [ClickHouse Kafka Connect sink documentation](https://clickhouse.com/docs/integrations/connectors/data-ingestion/kafka/kafka-clickhouse-connect-sink)
- [ClickHouse Kafka Connect exactly-once design](https://github.com/ClickHouse/clickhouse-kafka-connect/blob/main/docs/DESIGN.md)
- [ClickHouse insert deduplication on retries and finite-window behavior](https://clickhouse.com/docs/guides/developer/deduplicating-inserts-on-retries)
- [ClickHouse async-insert deduplication issue #110604](https://github.com/ClickHouse/ClickHouse/issues/110604)
- [ClickHouse guidance on deduplication and ReplacingMergeTree visibility](https://clickhouse.com/blog/common-getting-started-issues-with-clickhouse)

## Backups

- [ClickHouse backup and restore overview](https://clickhouse.com/docs/operations/backup/overview)
- [ClickHouse backup and restore through an S3 endpoint](https://clickhouse.com/docs/operations/backup/s3_endpoint)
- [ClickHouse named collections](https://clickhouse.com/docs/operations/named-collections)
- [ClickHouse incremental backup dependency contract](https://github.com/ClickHouse/clickhouse-docs/blob/main/docs/operations_/backup_restore/01_local_disk.md#incremental-backups)
- [ClickHouse copy-on-write table cloning](https://clickhouse.com/blog/table-cloning)
- [ClickHouse external-backup lifecycle ownership](https://clickhouse.com/blog/introducing-external-backups-on-clickhouse-cloud)

## Production experience

- [PostHog architecture: Kafka as the central bus connecting ingestion to storage](https://posthog.com/docs/how-posthog-works)
- [Wix's Kafka journey, including local-disk-first producer durability](https://www.wix.engineering/post/wix-s-journey-into-data-streams)
- [Wix's production migration pipeline: Avro, schema registry, isolated consumer offsets, and validation](https://www.wix.engineering/posts/how-we-built-a-zero-downtime-database-migration-service-at-wix)
- [eBay's Kafka-to-ClickHouse deterministic retry protocol](https://innovation.ebayinc.com/stories/block-aggregator-real-time-data-ingestion-from-kafka-to-clickhouse-with-deterministic-retries/)

These systems are references for failure models and operational practice, not
claims that their architectures are identical to this chart.
