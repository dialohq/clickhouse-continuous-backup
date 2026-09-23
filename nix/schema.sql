CREATE TABLE IF NOT EXISTS durable_e2e.records
(
    record_key String,
    recorded_at DateTime64(3, 'UTC'),
    payload String
)
ENGINE = ReplicatedMergeTree{% if database_engine == 'Atomic' %}('/clickhouse/tables/{shard}/durable_e2e/records', '{replica}'){% endif %}
ORDER BY (recorded_at, record_key);

CREATE TABLE IF NOT EXISTS durable_e2e.pitr_records AS durable_e2e.records
ENGINE = ReplicatedMergeTree{% if database_engine == 'Atomic' %}('/clickhouse/tables/{shard}/durable_e2e/pitr_records', '{replica}'){% endif %}
ORDER BY (recorded_at, record_key);

CREATE TABLE IF NOT EXISTS durable_e2e.live_records AS durable_e2e.records
ENGINE = ReplicatedMergeTree{% if database_engine == 'Atomic' %}('/clickhouse/tables/{shard}/durable_e2e/live_records', '{replica}'){% endif %}
ORDER BY (recorded_at, record_key);

CREATE TABLE IF NOT EXISTS durable_e2e.invalid_partial_records AS durable_e2e.records
ENGINE = ReplicatedMergeTree{% if database_engine == 'Atomic' %}('/clickhouse/tables/{shard}/durable_e2e/invalid_partial_records', '{replica}'){% endif %}
ORDER BY (recorded_at, record_key);

CREATE TABLE IF NOT EXISTS durable_e2e.invalid_future_records AS durable_e2e.records
ENGINE = ReplicatedMergeTree{% if database_engine == 'Atomic' %}('/clickhouse/tables/{shard}/durable_e2e/invalid_future_records', '{replica}'){% endif %}
ORDER BY (recorded_at, record_key);

CREATE TABLE IF NOT EXISTS durable_e2e.invalid_credentials_records AS durable_e2e.records
ENGINE = ReplicatedMergeTree{% if database_engine == 'Atomic' %}('/clickhouse/tables/{shard}/durable_e2e/invalid_credentials_records', '{replica}'){% endif %}
ORDER BY (recorded_at, record_key);

CREATE TABLE IF NOT EXISTS durable_e2e.ambiguous_restore_records AS durable_e2e.records
ENGINE = ReplicatedMergeTree{% if database_engine == 'Atomic' %}('/clickhouse/tables/{shard}/durable_e2e/ambiguous_restore_records', '{replica}'){% endif %}
ORDER BY (recorded_at, record_key);
