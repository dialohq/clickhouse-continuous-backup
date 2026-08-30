# External design references

The implementation and review should be read against the upstream contracts,
not only this repository's summaries.

## Processing and delivery

- [Apache Kafka Streams core concepts: exactly-once state, offsets, and output](https://kafka.apache.org/33/streams/core-concepts/)
- [Apache Kafka 4.2 Streams configuration](https://kafka.apache.org/42/streams/developer-guide/config-streams/)
- [Kafka Connect user guide and REST API](https://kafka.apache.org/42/kafka-connect/)
- [Kafka Connect offset-management API (KIP-875)](https://cwiki.apache.org/confluence/display/KAFKA/KIP-875%3A+First-class+Offsets+Support+in+Kafka+Connect)
- [ClickHouse Kafka Connect sink documentation](https://clickhouse.com/docs/integrations/connectors/data-ingestion/kafka/kafka-clickhouse-connect-sink)
- [ClickHouse Kafka Connect exactly-once design](https://github.com/ClickHouse/clickhouse-kafka-connect/blob/main/docs/DESIGN.md)
- [ClickHouse guidance on deduplication and ReplacingMergeTree visibility](https://clickhouse.com/blog/common-getting-started-issues-with-clickhouse)

## Backup and recovery

- [ClickHouse backup and restore overview](https://clickhouse.com/docs/operations/backup/overview)
- [ClickHouse backup and restore through an S3 endpoint](https://clickhouse.com/docs/operations/backup/s3_endpoint)
- [ClickHouse named collections](https://clickhouse.com/docs/operations/named-collections)

## Production experience

- [PostHog architecture: Kafka as the central bus connecting ingestion to storage](https://posthog.com/docs/how-posthog-works)
- [Wix's Kafka journey, including local-disk-first producer durability](https://www.wix.engineering/post/wix-s-journey-into-data-streams)
- [Wix's production migration pipeline: Avro, schema registry, isolated consumer offsets, and validation](https://www.wix.engineering/posts/how-we-built-a-zero-downtime-database-migration-service-at-wix)
- [eBay's Kafka-to-ClickHouse deterministic retry protocol](https://innovation.ebayinc.com/stories/block-aggregator-real-time-data-ingestion-from-kafka-to-clickhouse-with-deterministic-retries/)
- [eBay's Kafka resiliency and disaster-recovery trade-offs](https://innovation.ebayinc.com/stories/resiliency-and-disaster-recovery-with-kafka/)

These systems are references for failure models and operational practice, not
claims that their architectures are identical to this chart.
