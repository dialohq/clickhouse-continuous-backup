#!/usr/bin/env bash
set -euo pipefail

: "${CONNECT_URL:?CONNECT_URL is required}"
: "${CLICKHOUSE_URL:?CLICKHOUSE_URL is required}"
: "${CONNECTOR_NAMES:?CONNECTOR_NAMES is required}"
: "${BACKUP_OBJECTS:?BACKUP_OBJECTS is required}"
: "${BACKUP_NAMED_COLLECTION:?BACKUP_NAMED_COLLECTION is required}"
: "${BACKUP_PATH_PREFIX:?BACKUP_PATH_PREFIX is required}"
: "${BACKUP_ARCHIVE_EXTENSION:?BACKUP_ARCHIVE_EXTENSION is required}"
: "${BACKUP_RUN_ID:?BACKUP_RUN_ID is required}"
: "${PAUSE_TIMEOUT_SECONDS:?PAUSE_TIMEOUT_SECONDS is required}"
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"
: "${KAFKA_RECOVERY_TOPIC:?KAFKA_RECOVERY_TOPIC is required}"

curl_bin=${CURL_BIN:-curl}
jq_bin=${JQ_BIN:-jq}
sleep_bin=${SLEEP_BIN:-sleep}
date_bin=${DATE_BIN:-date}
kafka_producer_bin=${KAFKA_PRODUCER_BIN:-kafka-console-producer.sh}
curl_config=${CLICKHOUSE_CURL_CONFIG:-/etc/clickhouse/curl.config}
kafka_config=${KAFKA_PROPERTIES_FILE:-}

[[ $PAUSE_TIMEOUT_SECONDS =~ ^[1-9][0-9]*$ ]] || {
  echo "PAUSE_TIMEOUT_SECONDS must be a positive integer" >&2
  exit 1
}
[[ $BACKUP_RUN_ID =~ ^[A-Za-z0-9-]+$ ]] || {
  echo "BACKUP_RUN_ID contains unsupported characters" >&2
  exit 1
}

connectors=()
while IFS= read -r connector; do
  [[ -n $connector ]] && connectors+=("$connector")
done <<<"$CONNECTOR_NAMES"
((${#connectors[@]} > 0)) || {
  echo "at least one connector is required" >&2
  exit 1
}

connect_request() {
  "$curl_bin" --connect-timeout 5 --max-time 15 --fail --silent --show-error "$@"
}

resume() {
  local connector
  for connector in "${connectors[@]}"; do
    connect_request --request PUT "$CONNECT_URL/connectors/$connector/resume" || true
  done
}

resume_on_exit() {
  local status=$?
  trap - EXIT INT TERM
  resume
  exit "$status"
}

request_pid=
terminate() {
  local status=$1
  if [[ -n $request_pid ]]; then
    kill -TERM "$request_pid" 2>/dev/null || true
    wait "$request_pid" 2>/dev/null || true
    request_pid=
  fi
  exit "$status"
}

capture_clickhouse() {
  local variable=$1 query=$2 output_file status
  output_file=$(mktemp)
  "$curl_bin" --connect-timeout 10 --fail-with-body --silent --show-error \
    --config "$curl_config" --data-binary "$query" "$CLICKHOUSE_URL" >"$output_file" &
  request_pid=$!
  if wait "$request_pid"; then
    status=0
  else
    status=$?
  fi
  request_pid=
  if ((status != 0)); then
    unlink "$output_file"
    return "$status"
  fi
  printf -v "$variable" '%s' "$(<"$output_file")"
  unlink "$output_file"
}

for connector in "${connectors[@]}"; do
  state=$(connect_request "$CONNECT_URL/connectors/$connector/status")
  if ! "$jq_bin" --exit-status '(.tasks | length > 0) and ([.connector.state, (.tasks[].state)] | all(. == "RUNNING"))' <<<"$state" >/dev/null; then
    echo "connector must be fully running before backup: $connector" >&2
    exit 1
  fi
done

trap resume_on_exit EXIT
trap 'terminate 130' INT
trap 'terminate 143' TERM

for connector in "${connectors[@]}"; do
  connect_request --fail-with-body --request PUT "$CONNECT_URL/connectors/$connector/pause"
done

for connector in "${connectors[@]}"; do
  for ((attempt = 1; attempt <= PAUSE_TIMEOUT_SECONDS; attempt++)); do
    if connect_request "$CONNECT_URL/connectors/$connector/status" |
      "$jq_bin" --exit-status '[.connector.state, (.tasks[].state)] | all(. == "PAUSED")' >/dev/null; then
      break
    fi
    if ((attempt == PAUSE_TIMEOUT_SECONDS)); then
      echo "connector did not pause: $connector" >&2
      exit 1
    fi
    "$sleep_bin" 1
  done
done

offset_snapshots='[]'
for connector in "${connectors[@]}"; do
  snapshot=$(connect_request "$CONNECT_URL/connectors/$connector/offsets")
  if ! "$jq_bin" --exit-status '
    (.offsets | type == "array") and
    ([.offsets[] |
      ((.partition.kafka_topic | type) == "string") and
      ((.partition.kafka_partition | type) == "number") and
      (.partition.kafka_partition >= 0) and
      ((.partition.kafka_partition | floor) == .partition.kafka_partition) and
      ((.offset.kafka_offset | type) == "number") and
      (.offset.kafka_offset >= 0) and
      ((.offset.kafka_offset | floor) == .offset.kafka_offset)
    ] | all) and
    (([.offsets[].partition | [.kafka_topic, .kafka_partition]] | length) ==
     ([.offsets[].partition | [.kafka_topic, .kafka_partition]] | unique | length))
  ' <<<"$snapshot" >/dev/null; then
    echo "connector returned invalid offsets: $connector" >&2
    exit 1
  fi
  offset_snapshots=$(
    "$jq_bin" --compact-output --null-input \
      --argjson snapshots "$offset_snapshots" \
      --arg connector "$connector" \
      --argjson snapshot "$snapshot" \
      '$snapshots + [{name: $connector, offsets: $snapshot.offsets}]'
  )
done

stamp=$($date_bin -u +%Y%m%dT%H%M%SZ)
destination="S3($BACKUP_NAMED_COLLECTION, '$BACKUP_PATH_PREFIX/$stamp-$BACKUP_RUN_ID.$BACKUP_ARCHIVE_EXTENSION')"
result=
capture_clickhouse result "BACKUP $BACKUP_OBJECTS TO $destination"
IFS=$'\t' read -r backup_id backup_status extra <<<"$result"
uuid='^[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}$'
if [[ ! $backup_id =~ $uuid ]] || [[ $backup_status != BACKUP_CREATED ]] || [[ -n ${extra:-} ]]; then
  echo "unexpected ClickHouse BACKUP result: $result" >&2
  exit 1
fi

details=
capture_clickhouse details "SELECT name, status, num_files, uncompressed_size, compressed_size FROM system.backups WHERE id = '$backup_id' FORMAT JSONEachRow"
if ! "$jq_bin" --exit-status --arg destination "$destination" \
  '(.name == $destination) and (.status == "BACKUP_CREATED")' <<<"$details" >/dev/null; then
  echo "ClickHouse did not confirm the created backup: $backup_id" >&2
  exit 1
fi

created_at=$($date_bin -u +%Y-%m-%dT%H:%M:%SZ)
manifest=$(
  "$jq_bin" --compact-output --null-input \
    --arg format durable-clickhouse-sink/recovery-point-v1 \
    --arg created_at "$created_at" \
    --arg backup_id "$backup_id" \
    --arg backup_name "$destination" \
    --argjson connectors "$offset_snapshots" \
    '{format: $format, created_at: $created_at, backup: {id: $backup_id, name: $backup_name}, connectors: $connectors}'
)
producer_config=()
if [[ -n $kafka_config ]]; then
  producer_config=(--producer.config "$kafka_config")
fi
printf '%s\t%s\n' "$backup_id" "$manifest" |
  "$kafka_producer_bin" \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$KAFKA_RECOVERY_TOPIC" \
    --property parse.key=true \
    --property $'key.separator=\t' \
    --command-property acks=all \
    --command-property enable.idempotence=true \
    "${producer_config[@]}"

"$jq_bin" --compact-output --argjson recovery_point "$manifest" \
  '. + {recovery_point: $recovery_point}' <<<"$details"
