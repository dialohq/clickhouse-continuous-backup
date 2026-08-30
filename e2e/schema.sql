CREATE DATABASE IF NOT EXISTS durable_e2e;

CREATE TABLE IF NOT EXISTS durable_e2e.records
(
    record_key String,
    recorded_at DateTime64(3, 'UTC'),
    payload String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/durable_e2e/records', '01')
ORDER BY (recorded_at, record_key);

CREATE TABLE IF NOT EXISTS durable_e2e.pitr_records AS durable_e2e.records
ENGINE = ReplicatedMergeTree('/clickhouse/tables/durable_e2e/pitr_records', '01')
ORDER BY (recorded_at, record_key);

CREATE TABLE IF NOT EXISTS durable_e2e.live_records AS durable_e2e.records
ENGINE = ReplicatedMergeTree('/clickhouse/tables/durable_e2e/live_records', '01')
ORDER BY (recorded_at, record_key);

CREATE TABLE IF NOT EXISTS durable_e2e.invalid_partial_records AS durable_e2e.records
ENGINE = ReplicatedMergeTree('/clickhouse/tables/durable_e2e/invalid_partial_records', '01')
ORDER BY (recorded_at, record_key);

CREATE TABLE IF NOT EXISTS durable_e2e.invalid_future_records AS durable_e2e.records
ENGINE = ReplicatedMergeTree('/clickhouse/tables/durable_e2e/invalid_future_records', '01')
ORDER BY (recorded_at, record_key);

CREATE TABLE IF NOT EXISTS durable_e2e.ambiguous_restore_records AS durable_e2e.records
ENGINE = ReplicatedMergeTree('/clickhouse/tables/durable_e2e/ambiguous_restore_records', '01')
ORDER BY (recorded_at, record_key);
