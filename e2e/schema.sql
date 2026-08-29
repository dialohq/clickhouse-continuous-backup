CREATE DATABASE IF NOT EXISTS durable_e2e;

CREATE TABLE IF NOT EXISTS durable_e2e.events
(
    id String,
    external_connection_id Nullable(String),
    occurred_at DateTime64(3, 'UTC'),
    source LowCardinality(String),
    metadata String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/durable_e2e/events', '01')
ORDER BY (occurred_at, id);
