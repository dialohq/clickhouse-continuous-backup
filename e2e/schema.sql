CREATE DATABASE IF NOT EXISTS durable_e2e;

CREATE TABLE IF NOT EXISTS durable_e2e.records
(
    record_key String,
    recorded_at DateTime64(3, 'UTC'),
    payload String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/durable_e2e/records', '01')
ORDER BY (recorded_at, record_key);
