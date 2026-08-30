#!/usr/bin/env bash
set -euo pipefail

: "${CONNECT_URL:?CONNECT_URL is required}"
: "${CONNECTOR_NAMES:?CONNECTOR_NAMES is required}"
: "${EXPECTED_BACKUP_NAME:?EXPECTED_BACKUP_NAME is required}"
: "${RECOVERY_MANIFEST_FILE:?RECOVERY_MANIFEST_FILE is required}"

curl_bin=${CURL_BIN:-curl}
jq_bin=${JQ_BIN:-jq}
sleep_bin=${SLEEP_BIN:-sleep}
stop_timeout_seconds=${STOP_TIMEOUT_SECONDS:-120}

[[ $stop_timeout_seconds =~ ^[1-9][0-9]*$ ]] || {
  echo "STOP_TIMEOUT_SECONDS must be a positive integer" >&2
  exit 1
}

connect_request() {
  "$curl_bin" --connect-timeout 5 --max-time 15 --fail-with-body --silent --show-error "$@"
}

if [[ $RECOVERY_MANIFEST_FILE == - ]]; then
  manifest=$(cat)
else
  manifest=$(<"$RECOVERY_MANIFEST_FILE")
fi

if ! "$jq_bin" --exit-status --arg backup "$EXPECTED_BACKUP_NAME" '
  .format == "durable-clickhouse-sink/recovery-point-v1" and
  .backup.name == $backup and
  (.backup.id | type == "string") and
  (.connectors | type == "array" and length > 0) and
  (([.connectors[].name] | length) == ([.connectors[].name] | unique | length)) and
  ([.connectors[] |
    (.name | type == "string" and length > 0) and
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
  ] | all)
' <<<"$manifest" >/dev/null; then
  echo "invalid recovery-point manifest" >&2
  exit 1
fi

connectors=()
while IFS= read -r connector; do
  [[ -n $connector ]] && connectors+=("$connector")
done <<<"$CONNECTOR_NAMES"
((${#connectors[@]} > 0)) || {
  echo "at least one connector is required" >&2
  exit 1
}

expected_names=$(printf '%s\n' "${connectors[@]}" | "$jq_bin" --raw-input --slurp 'split("\n") | map(select(length > 0)) | sort')
manifest_names=$("$jq_bin" '[.connectors[].name] | sort' <<<"$manifest")
if [[ $expected_names != "$manifest_names" ]]; then
  echo "manifest connector set does not match this release" >&2
  exit 1
fi

for connector in "${connectors[@]}"; do
  state=$(connect_request "$CONNECT_URL/connectors/$connector/status")
  if ! "$jq_bin" --exit-status '.connector.state == "STOPPED"' <<<"$state" >/dev/null; then
    echo "connector must be stopped before restoring offsets: $connector" >&2
    exit 1
  fi
  for ((attempt = 1; attempt <= stop_timeout_seconds; attempt++)); do
    if "$jq_bin" --exit-status '.connector.state == "STOPPED" and ([.tasks[].state] | all(. == "STOPPED"))' <<<"$state" >/dev/null; then
      break
    fi
    if ((attempt == stop_timeout_seconds)); then
      echo "connector tasks did not stop: $connector" >&2
      exit 1
    fi
    "$sleep_bin" 1
    state=$(connect_request "$CONNECT_URL/connectors/$connector/status")
  done
done

for connector in "${connectors[@]}"; do
  offsets=$("$jq_bin" --compact-output --arg connector "$connector" \
    '{offsets: (.connectors[] | select(.name == $connector) | .offsets)}' <<<"$manifest")
  connect_request \
    --request PATCH \
    --header 'Content-Type: application/json' \
    --data-binary "$offsets" \
    "$CONNECT_URL/connectors/$connector/offsets" >/dev/null
done

for connector in "${connectors[@]}"; do
  expected=$("$jq_bin" --compact-output --sort-keys --arg connector "$connector" \
    '(.connectors[] | select(.name == $connector) | .offsets) | sort_by(.partition.kafka_topic, .partition.kafka_partition)' <<<"$manifest")
  actual=$(connect_request "$CONNECT_URL/connectors/$connector/offsets" |
    "$jq_bin" --compact-output --sort-keys '.offsets | sort_by(.partition.kafka_topic, .partition.kafka_partition)')
  if [[ $actual != "$expected" ]]; then
    echo "connector offset verification failed: $connector" >&2
    exit 1
  fi
done

printf '%s\n' "$manifest"
