#!/usr/bin/env bash
set -euo pipefail

backup=$1
mock_curl=$2
mock_kafka_producer=$3
restore_offsets=$4
test_root=$(mktemp -d)
trap 'rm -rf -- "$test_root"' EXIT

run_backup() {
  local scenario=$1 connectors=${2-$'one\ntwo'}
  case_dir="$test_root/$scenario"
  mkdir -p "$case_dir/state"
  : >"$case_dir/requests"
  : >"$case_dir/kafka-args"
  : >"$case_dir/kafka-records"
  set +e
  env \
    CONNECT_URL=http://connect:8083 \
    CLICKHOUSE_URL=http://clickhouse:8123 \
    CONNECTOR_NAMES="$connectors" \
    BACKUP_OBJECTS='DATABASE events, DATABASE calls' \
    BACKUP_NAMED_COLLECTION=backups \
    BACKUP_PATH_PREFIX=durable \
    BACKUP_ARCHIVE_EXTENSION=tar.zst \
    BACKUP_RUN_ID=11111111-2222-3333-4444-555555555555 \
    PAUSE_TIMEOUT_SECONDS=2 \
    KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    KAFKA_RECOVERY_TOPIC=durable.recovery-points \
    KAFKA_PROPERTIES_FILE=/credentials/client.properties \
    CLICKHOUSE_CURL_CONFIG=/credentials/curl.config \
    CURL_BIN="$mock_curl" \
    KAFKA_PRODUCER_BIN="$mock_kafka_producer" \
    JQ_BIN=jq \
    SLEEP_BIN=true \
    SCENARIO="$scenario" \
    MOCK_LOG="$case_dir/requests" \
    MOCK_KAFKA_LOG="$case_dir/kafka-args" \
    MOCK_KAFKA_RECORDS="$case_dir/kafka-records" \
    MOCK_STATE="$case_dir/state" \
    "$backup" >"$case_dir/stdout" 2>"$case_dir/stderr"
  status=$?
  set -e
}

request_count() {
  local pattern=$1 count
  count=$(rg --count-matches "$pattern" "$case_dir/requests" 2>/dev/null || true)
  printf '%s\n' "${count:-0}"
}

expect_failure() {
  run_backup "$@"
  [[ $status -ne 0 ]]
}

expect_failure zero-tasks
[[ $(request_count '/pause|/resume|BACKUP ') == 0 ]]

expect_failure failed-task
[[ $(request_count '/pause|/resume|BACKUP ') == 0 ]]

expect_failure second-not-running
[[ $(request_count '/pause|/resume|BACKUP ') == 0 ]]

expect_failure pause-failure
[[ $(request_count '/resume') == 2 ]]
[[ $(request_count 'BACKUP ') == 0 ]]

expect_failure pause-timeout one
[[ $(request_count '/pause') == 1 ]]
[[ $(request_count '/resume') == 1 ]]
[[ $(request_count 'BACKUP ') == 0 ]]

expect_failure offsets-failure
[[ $(request_count '/resume') == 2 ]]
[[ $(request_count 'BACKUP ') == 0 ]]

expect_failure malformed-offsets
[[ $(request_count '/resume') == 2 ]]
[[ $(request_count 'BACKUP ') == 0 ]]

run_backup empty-offsets one
[[ $status == 0 ]]
jq --exit-status '.recovery_point.connectors == [{"name":"one","offsets":[]}]' "$case_dir/stdout" >/dev/null

expect_failure backup-failure
[[ $(request_count '/pause') == 2 ]]
[[ $(request_count '/resume') == 2 ]]
[[ $(request_count 'SELECT ') == 0 ]]

expect_failure malformed-response
[[ $(request_count '/resume') == 2 ]]
[[ $(request_count 'SELECT ') == 0 ]]

expect_failure missing-verification
[[ $(request_count '/resume') == 2 ]]
[[ $(request_count 'SELECT ') == 1 ]]

expect_failure wrong-verification
[[ $(request_count '/resume') == 2 ]]
[[ $(request_count 'SELECT ') == 1 ]]

expect_failure manifest-failure
[[ $(request_count '/resume') == 2 ]]
[[ $(request_count 'BACKUP ') == 1 ]]
[[ $(wc -l <"$case_dir/kafka-records") == 1 ]]

case_dir="$test_root/blocked-backup"
mkdir -p "$case_dir/state"
: >"$case_dir/requests"
env \
  CONNECT_URL=http://connect:8083 \
  CLICKHOUSE_URL=http://clickhouse:8123 \
  CONNECTOR_NAMES=$'one\ntwo' \
  BACKUP_OBJECTS='DATABASE events, DATABASE calls' \
  BACKUP_NAMED_COLLECTION=backups \
  BACKUP_PATH_PREFIX=durable \
  BACKUP_ARCHIVE_EXTENSION=tar.zst \
  BACKUP_RUN_ID=11111111-2222-3333-4444-555555555555 \
  PAUSE_TIMEOUT_SECONDS=2 \
  KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  KAFKA_RECOVERY_TOPIC=durable.recovery-points \
  CLICKHOUSE_CURL_CONFIG=/credentials/curl.config \
  CURL_BIN="$mock_curl" \
  KAFKA_PRODUCER_BIN="$mock_kafka_producer" \
  JQ_BIN=jq \
  SLEEP_BIN=true \
  SCENARIO=blocked-backup \
  MOCK_LOG="$case_dir/requests" \
  MOCK_KAFKA_LOG="$case_dir/kafka-args" \
  MOCK_KAFKA_RECORDS="$case_dir/kafka-records" \
  MOCK_STATE="$case_dir/state" \
  "$backup" >"$case_dir/stdout" 2>"$case_dir/stderr" &
backup_pid=$!
for _ in {1..100}; do
  [[ -e $case_dir/state/blocked ]] && break
  sleep 0.01
done
[[ -e $case_dir/state/blocked ]]
kill -TERM "$backup_pid"
set +e
wait "$backup_pid"
status=$?
set -e
[[ $status == 143 ]]
[[ $(request_count '/resume') == 2 ]]

expect_failure success ''
[[ $(request_count '^') == 0 ]]

run_backup success
[[ $status == 0 ]]
[[ $(request_count '/pause') == 2 ]]
[[ $(request_count '/resume') == 2 ]]
[[ $(request_count 'BACKUP DATABASE events, DATABASE calls TO S3\(backups, .durable/[0-9]{8}T[0-9]{6}Z-11111111-2222-3333-4444-555555555555\.tar\.zst.\)') == 1 ]]
[[ $(request_count "WHERE id = '123e4567-e89b-12d3-a456-426614174000'") == 1 ]]
[[ $(wc -l <"$case_dir/kafka-records") == 1 ]]
rg --fixed-strings -- '--command-property acks=all --command-property enable.idempotence=true --producer.config /credentials/client.properties' "$case_dir/kafka-args" >/dev/null
recovery_key=$(cut -f1 "$case_dir/kafka-records")
recovery_manifest=$(cut -f2- "$case_dir/kafka-records")
[[ $recovery_key == 123e4567-e89b-12d3-a456-426614174000 ]]
jq --exit-status '.backup.name | startswith("S3(backups, '\''durable/")' <<<"$recovery_manifest" >/dev/null
jq --exit-status '.connectors == [
  {"name":"one","offsets":[{"partition":{"kafka_topic":"events.canonical","kafka_partition":0},"offset":{"kafka_offset":42}}]},
  {"name":"two","offsets":[{"partition":{"kafka_topic":"calls.canonical","kafka_partition":1},"offset":{"kafka_offset":24}}]}
]' <<<"$recovery_manifest" >/dev/null
jq --exit-status '.status == "BACKUP_CREATED" and .recovery_point == $manifest' \
  --argjson manifest "$recovery_manifest" "$case_dir/stdout" >/dev/null

manifest='{"format":"durable-clickhouse-sink/recovery-point-v1","created_at":"2026-08-29T12:00:00Z","backup":{"id":"123e4567-e89b-12d3-a456-426614174000","name":"S3(backups, '\''durable/point.tar.zst'\'')"},"connectors":[{"name":"one","offsets":[{"partition":{"kafka_topic":"events.canonical","kafka_partition":0},"offset":{"kafka_offset":42}}]},{"name":"two","offsets":[{"partition":{"kafka_topic":"calls.canonical","kafka_partition":1},"offset":{"kafka_offset":24}}]}]}'
backup_name="S3(backups, 'durable/point.tar.zst')"

run_restore() {
  local scenario=$1 connectors=${2-$'one\ntwo'} input=${3-$manifest}
  case_dir="$test_root/restore-$scenario"
  mkdir -p "$case_dir/state"
  : >"$case_dir/requests"
  if [[ $scenario != restore-not-stopped ]]; then
    touch "$case_dir/state/one.stopped" "$case_dir/state/two.stopped"
  fi
  set +e
  printf '%s\n' "$input" | env \
    CONNECT_URL=http://connect:8083 \
    CONNECTOR_NAMES="$connectors" \
    EXPECTED_BACKUP_NAME="$backup_name" \
    RECOVERY_MANIFEST_FILE=- \
    STOP_TIMEOUT_SECONDS=2 \
    CURL_BIN="$mock_curl" \
    JQ_BIN=jq \
    SLEEP_BIN=true \
    SCENARIO="$scenario" \
    MOCK_LOG="$case_dir/requests" \
    MOCK_STATE="$case_dir/state" \
    "$restore_offsets" >"$case_dir/stdout" 2>"$case_dir/stderr"
  status=$?
  set -e
}

run_restore restore-success
[[ $status == 0 ]]
[[ $(request_count 'PATCH.*connectors/.*/offsets') == 2 ]]
jq --exit-status '.backup.name == $name' --arg name "$backup_name" "$case_dir/stdout" >/dev/null

empty_manifest=$(jq --compact-output '.connectors = [{"name":"one","offsets":[]}]' <<<"$manifest")
run_restore restore-empty-offsets one "$empty_manifest"
[[ $status == 0 ]]
[[ $(request_count 'PATCH.*connectors/.*/offsets') == 1 ]]

run_restore restore-not-stopped one
[[ $status != 0 ]]
[[ $(request_count 'PATCH') == 0 ]]

run_restore restore-stop-timeout one
[[ $status != 0 ]]
[[ $(request_count 'PATCH') == 0 ]]

run_restore restore-patch-failure
[[ $status != 0 ]]
[[ $(request_count 'PATCH') == 1 ]]

run_restore restore-wrong-verification
[[ $status != 0 ]]
[[ $(request_count 'PATCH') == 2 ]]

run_restore restore-wrong-backup one "${manifest/durable\/point.tar.zst/durable\/other.tar.zst}"
[[ $status != 0 ]]
[[ $(request_count '^') == 0 ]]

run_restore restore-wrong-connectors one
[[ $status != 0 ]]
[[ $(request_count '^') == 0 ]]

run_restore restore-malformed one '{"format":"wrong"}'
[[ $status != 0 ]]
[[ $(request_count '^') == 0 ]]

printf '%s\n' 'backup and recovery edge-case tests passed'
