#!/usr/bin/env bash
set -euo pipefail

backup=$1
mock_curl=$2
test_root=$(mktemp -d)
trap 'rm -rf -- "$test_root"' EXIT

run_backup() {
  local scenario=$1 connectors=${2-$'one\ntwo'}
  case_dir="$test_root/$scenario"
  mkdir -p "$case_dir/state"
  : >"$case_dir/requests"
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
    CLICKHOUSE_CURL_CONFIG=/credentials/curl.config \
    CURL_BIN="$mock_curl" \
    JQ_BIN=jq \
    SLEEP_BIN=true \
    SCENARIO="$scenario" \
    MOCK_LOG="$case_dir/requests" \
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
  CLICKHOUSE_CURL_CONFIG=/credentials/curl.config \
  CURL_BIN="$mock_curl" \
  JQ_BIN=jq \
  SLEEP_BIN=true \
  SCENARIO=blocked-backup \
  MOCK_LOG="$case_dir/requests" \
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
jq --exit-status '.name | startswith("S3(backups, '\''durable/")' "$case_dir/stdout" >/dev/null
jq --exit-status '.status == "BACKUP_CREATED"' "$case_dir/stdout" >/dev/null

printf '%s\n' 'backup edge-case tests passed'
