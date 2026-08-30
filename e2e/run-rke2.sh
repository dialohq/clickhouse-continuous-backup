#!/usr/bin/env bash
set -euo pipefail

if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
  echo "e2e/run-rke2.sh must run as root" >&2
  exit 1
fi

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

namespace=durable-sink-e2e
service_name=rke2-durable-clickhouse-sink-e2e.service
rke2_data=/var/lib/rancher/rke2-durable-clickhouse-sink-e2e
kubeconfig=/run/rke2-durable-clickhouse-sink-e2e.yaml
containerd_socket=/run/k3s/containerd/containerd.sock
work_dir=$(mktemp -d -t durable-clickhouse-sink-e2e.XXXXXXXX)
started_rke2=false

k() {
  kubectl --kubeconfig "$kubeconfig" "$@"
}

wait_for_ready_pod() {
  local selector=$1 timeout=${2:-300s}
  for _ in {1..120}; do
    [[ -n $(k -n "$namespace" get pod -l "$selector" -o name 2>/dev/null) ]] && break
    sleep 1
  done
  k -n "$namespace" wait pod -l "$selector" --for=condition=Ready --timeout="$timeout"
}

diagnostics() {
  k -n "$namespace" get pods,jobs,cronjobs 2>/dev/null || true
  k -n "$namespace" get events --sort-by=.lastTimestamp 2>/dev/null | tail -100 || true
  local pod
  while read -r pod; do
    [[ -n $pod ]] || continue
    k -n "$namespace" logs "$pod" --all-containers --tail=150 2>/dev/null || true
  done < <(k -n "$namespace" get pods -o name 2>/dev/null)
}

cleanup() {
  local status=$?
  trap - EXIT
  if ((status != 0)); then
    diagnostics
  fi
  if ((status != 0)) && [[ ${DURABLE_SINK_E2E_KEEP_ON_FAILURE:-false} == true ]]; then
    echo "Keeping the failed RKE2 cluster for inspection" >&2
    exit "$status"
  fi
  k delete namespace "$namespace" --ignore-not-found --wait=true --timeout=180s >/dev/null 2>&1 || true
  if [[ $started_rke2 == true ]]; then
    systemctl stop "$service_name" >/dev/null 2>&1 || true
    systemctl reset-failed "$service_name" >/dev/null 2>&1 || true
    mapfile -t leftover_pids < <(pgrep -f "$rke2_data" || true)
    if ((${#leftover_pids[@]} > 0)); then
      kill -TERM "${leftover_pids[@]}" 2>/dev/null || true
      for _ in {1..50}; do
        mapfile -t leftover_pids < <(pgrep -f "$rke2_data" || true)
        ((${#leftover_pids[@]} == 0)) && break
        sleep 0.1
      done
      ((${#leftover_pids[@]} == 0)) || kill -KILL "${leftover_pids[@]}" 2>/dev/null || true
    fi
    if [[ $rke2_data == /var/lib/rancher/rke2-durable-clickhouse-sink-e2e ]]; then
      rm -rf -- "$rke2_data"
    fi
    rm -f -- "$kubeconfig"
  fi
  rm -rf -- "$work_dir"
  exit "$status"
}
trap cleanup EXIT

for command in rke2 kubectl ctr nix helm jq; do
  command -v "$command" >/dev/null
done

if ! systemctl is-active --quiet "$service_name"; then
  if [[ $rke2_data == /var/lib/rancher/rke2-durable-clickhouse-sink-e2e ]]; then
    rm -rf -- "$rke2_data"
  fi
  rm -f -- "$kubeconfig"
  systemd-run \
    --unit "$service_name" \
    --property=Restart=no \
    rke2 server \
      --data-dir "$rke2_data" \
      --write-kubeconfig "$kubeconfig" \
      --write-kubeconfig-mode 600 \
      --ingress-controller none \
      --disable rke2-metrics-server \
      --disable rke2-snapshot-controller \
      --disable rke2-snapshot-controller-crd \
      --disable rke2-snapshot-validation-webhook \
      --etcd-disable-snapshots >/dev/null
  started_rke2=true
fi

for _ in {1..180}; do
  if [[ -s $kubeconfig ]] && [[ -n $(k get nodes -o name 2>/dev/null) ]]; then
    break
  fi
  sleep 2
done
k wait node --all --for=condition=Ready --timeout=300s
k delete namespace "$namespace" --ignore-not-found --wait=true --timeout=180s

load_image() {
  local package=$1 image=$2 archive="$work_dir/$1.tar"
  nix run ".#$package.copyTo" -- "docker-archive:$archive:$image" >/dev/null
  ctr --address "$containerd_socket" --namespace k8s.io images import "$archive" >/dev/null
}
load_image deduplicatorImage ghcr.io/dialohq/durable-clickhouse-deduplicator:e2e
load_image connectImage ghcr.io/dialohq/durable-clickhouse-connect:e2e

manifests=$(nix build .#e2eManifests --no-link --print-out-paths)
k apply -f "$manifests/e2e/Namespace-durable-sink-e2e.yaml"
while read -r resource; do
  [[ $resource == */Namespace-durable-sink-e2e.yaml ]] && continue
  k apply -f "$resource"
done < <(find -L "$manifests/e2e" -maxdepth 1 -type f -name '*.yaml' | sort)

wait_for_ready_pod app=redpanda
wait_for_ready_pod app=clickhouse
wait_for_ready_pod app=clickhouse-restore
wait_for_ready_pod app=minio
k -n "$namespace" wait job/minio-bucket --for=condition=Complete --timeout=300s
k -n "$namespace" wait job/schema --for=condition=Complete --timeout=300s

chart=$(nix build .#chartSource --no-link --print-out-paths)
values=$(nix build .#e2eValues --no-link --print-out-paths)
helm --kubeconfig "$kubeconfig" upgrade --install sink "$chart" \
  --namespace "$namespace" --values "$values" --wait --timeout 10m

wait_for_ready_pod app.kubernetes.io/component=deduplicator
wait_for_ready_pod app.kubernetes.io/component=connect

clickhouse() {
  k -n "$namespace" exec clickhouse-0 -- clickhouse-client --query "$1"
}

restore_clickhouse() {
  k -n "$namespace" exec clickhouse-restore-0 -- clickhouse-client --query "$1"
}

connector_request() {
  local method=$1 path=$2
  k -n "$namespace" exec deployment/sink-durable-clickhouse-sink-connect -- \
    curl --fail --silent --show-error --request "$method" \
      "http://localhost:8083/connectors/sink-durable-clickhouse-sink-events$path"
}

wait_connector_state() {
  local expected=$1 actual
  for _ in {1..120}; do
    actual=$(connector_request GET /status 2>/dev/null | jq -r '.connector.state' || true)
    [[ $actual == "$expected" ]] && return
    sleep 1
  done
  echo "expected connector state $expected, got $actual" >&2
  return 1
}

wait_count() {
  local expected=$1 query=$2 actual
  for _ in {1..240}; do
    actual=$(clickhouse "$query" 2>/dev/null || true)
    [[ $actual == "$expected" ]] && return
    sleep 1
  done
  echo "expected $expected, got $actual for: $query" >&2
  return 1
}

start_producer() {
  local name=$1 first=$2 last=$3 repeats=$4
  # The single-quoted program expands inside the producer pod.
  # shellcheck disable=SC2016
  k -n "$namespace" run "$name" \
    --image=ghcr.io/dialohq/durable-clickhouse-connect:e2e \
    --image-pull-policy=Never \
    --restart=Never \
    --env="FIRST=$first" --env="LAST=$last" --env="REPEATS=$repeats" \
    --command -- /bin/bash -euc '
      for i in $(seq "$FIRST" "$LAST"); do
        value=$(printf "{\"id\":\"event-%s\",\"external_connection_id\":\"connection-%s\",\"occurred_at\":\"2026-08-29 12:00:00.000\",\"source\":\"e2e\",\"metadata\":\"sequence-%s\"}" "$i" "$i" "$i")
        for _ in $(seq 1 "$REPEATS"); do printf "event-%s\t%s\n" "$i" "$value"; done
      done | /bin/kafka-console-producer.sh --bootstrap-server redpanda:9092 --topic events.raw --property parse.key=true --property key.separator=$'"'"'\t'"'"'
    '
}

start_producer initial-events 1 100 2
k -n "$namespace" wait pod/initial-events --for=jsonpath='{.status.phase}'=Succeeded --timeout=300s

conflicting='{"id":"event-1","external_connection_id":"different","occurred_at":"2026-08-29 12:00:00.000","source":"e2e","metadata":"{}"}'
printf 'event-1\t%s\n' "$conflicting" | k -n "$namespace" exec -i deployment/sink-durable-clickhouse-sink-connect -- \
  /bin/kafka-console-producer.sh --bootstrap-server redpanda:9092 --topic events.raw --property parse.key=true --property key.separator=$'\t'

wait_count 100 'SELECT count() FROM durable_e2e.events'
wait_count 100 'SELECT uniqExact(id) FROM durable_e2e.events'

start_producer crash-events 101 1100 2
for _ in {1..3}; do
  sleep 2
  k -n "$namespace" delete pod -l app.kubernetes.io/component=deduplicator --grace-period=0 --force --wait=false
  k -n "$namespace" delete pod -l app.kubernetes.io/component=connect --grace-period=0 --force --wait=false
  wait_for_ready_pod app.kubernetes.io/component=deduplicator
  wait_for_ready_pod app.kubernetes.io/component=connect
done
k -n "$namespace" wait pod/crash-events --for=jsonpath='{.status.phase}'=Succeeded --timeout=600s
wait_count 1100 'SELECT count() FROM durable_e2e.events'
wait_count 1100 'SELECT uniqExact(id) FROM durable_e2e.events'

conflicts=$(k -n "$namespace" exec redpanda-0 -- rpk topic consume events.conflicts -n 1 --format '%k' -X brokers=redpanda:9092)
[[ $conflicts == event-1 ]]

connector_request PUT /pause
wait_connector_state PAUSED
k -n "$namespace" create job e2e-backup-refuses-paused --from=cronjob/sink-durable-clickhouse-sink-backup
k -n "$namespace" wait job/e2e-backup-refuses-paused --for=condition=Failed --timeout=300s
wait_connector_state PAUSED
connector_request PUT /resume
wait_connector_state RUNNING

backups_before=$(clickhouse "SELECT count() FROM system.backups WHERE status = 'BACKUP_CREATED'")
k -n "$namespace" create job e2e-backup-bad-credentials \
  --from=cronjob/sink-durable-clickhouse-sink-backup --dry-run=client -o json |
  jq '(.spec.template.spec.containers[0].env[] | select(.name == "CLICKHOUSE_USERNAME" or .name == "CLICKHOUSE_PASSWORD").valueFrom.secretKeyRef.name) = "clickhouse-bad-backup"' |
  k apply -f -
k -n "$namespace" wait job/e2e-backup-bad-credentials --for=condition=Failed --timeout=300s
wait_connector_state RUNNING
[[ $(clickhouse "SELECT count() FROM system.backups WHERE status = 'BACKUP_CREATED'") == "$backups_before" ]]

run_backup() {
  local job=$1
  k -n "$namespace" create job "$job" --from=cronjob/sink-durable-clickhouse-sink-backup >&2
  for _ in {1..900}; do
    state=$(k -n "$namespace" get "job/$job" -o jsonpath='{range .status.conditions[*]}{.type}:{.status}{"\n"}{end}')
    [[ $state == *'Complete:True'* ]] && break
    if [[ $state == *'Failed:True'* ]]; then
      k -n "$namespace" logs "job/$job" --all-containers --prefix >&2
      return 1
    fi
    sleep 1
  done
  [[ $state == *'Complete:True'* ]]
  k -n "$namespace" logs "job/$job" |
    jq --raw-input --compact-output 'fromjson? | select(.status == "BACKUP_CREATED")'
}

base_output=$(run_backup e2e-backup-base)
base=$(jq -r '.name' <<<"$base_output")
base_id=$(jq -r '.recovery_point.backup.id' <<<"$base_output")
base_manifest=$(jq --compact-output '.recovery_point' <<<"$base_output")
jq --exit-status '.backup.kind == "full" and .backup.position == 0 and (.backup | has("base") | not)' <<<"$base_manifest" >/dev/null

start_producer incremental-one-events 1101 1150 2
k -n "$namespace" wait pod/incremental-one-events --for=jsonpath='{.status.phase}'=Succeeded --timeout=300s
wait_count 1150 'SELECT count() FROM durable_e2e.events'
incremental_one_output=$(run_backup e2e-backup-incremental-one)
incremental_one=$(jq -r '.name' <<<"$incremental_one_output")
incremental_one_id=$(jq -r '.recovery_point.backup.id' <<<"$incremental_one_output")
jq --exit-status --arg id "$base_id" --arg name "$base" '
  .recovery_point.backup.kind == "incremental" and
  .recovery_point.backup.position == 1 and
  .recovery_point.backup.base == {id: $id, name: $name}
' <<<"$incremental_one_output" >/dev/null

start_producer incremental-two-events 1151 1200 2
k -n "$namespace" wait pod/incremental-two-events --for=jsonpath='{.status.phase}'=Succeeded --timeout=300s
wait_count 1200 'SELECT count() FROM durable_e2e.events'
incremental_two_output=$(run_backup e2e-backup-incremental-two)
backup=$(jq -r '.name' <<<"$incremental_two_output")
checkpoint_backup=$(jq -r '.recovery_point.checkpoint_backup.name' <<<"$incremental_two_output")
backup_id=$(jq -r '.recovery_point.backup.id' <<<"$incremental_two_output")
manifest=$(jq --compact-output '.recovery_point' <<<"$incremental_two_output")
jq --exit-status --arg id "$incremental_one_id" --arg name "$incremental_one" '
  .backup.kind == "incremental" and
  .backup.position == 2 and
  .backup.base == {id: $id, name: $name} and
  ([.connectors[].keeper.rows[].state] | all(. == "AFTER_PROCESSING")) and
  ([.connectors[] | .offsets as $exact | .observed_connect_offsets[] |
    . as $observed | ($exact[] | select(.partition == $observed.partition) | .offset.kafka_offset) >= $observed.offset.kafka_offset] | all)
' <<<"$manifest" >/dev/null
[[ $backup == S3\(* ]]
[[ $backup_id =~ ^[[:xdigit:]-]{36}$ ]]

kafka_manifest=$(k -n "$namespace" exec deployment/sink-durable-clickhouse-sink-connect -- \
  /bin/kafka-console-consumer.sh \
    --bootstrap-server redpanda:9092 \
    --topic sink-durable-clickhouse-sink.recovery-points \
    --partition 0 \
    --from-beginning \
    --timeout-ms 10000 2>/dev/null |
  jq --raw-input --compact-output --arg id "$backup_id" 'fromjson? | select(.backup.id == $id)' | tail -1)
if ! jq --exit-status --argjson expected "$manifest" '. == $expected' <<<"$kafka_manifest" >/dev/null; then
  echo "recovery-point topic did not return the Job manifest" >&2
  printf 'expected: %s\nactual: %s\n' "$manifest" "$kafka_manifest" >&2
  exit 1
fi

start_producer pre-restore-tail 1201 1250 2
k -n "$namespace" wait pod/pre-restore-tail --for=jsonpath='{.status.phase}'=Succeeded --timeout=300s
wait_count 1250 'SELECT count() FROM durable_e2e.events'
rollover_output=$(run_backup e2e-backup-rollover)
jq --exit-status '.recovery_point.backup.kind == "full" and .recovery_point.backup.position == 0' <<<"$rollover_output" >/dev/null

restore_clickhouse 'CREATE DATABASE durable_e2e'
restore_clickhouse "RESTORE TABLE durable_e2e.events FROM $backup"
[[ $(restore_clickhouse 'SELECT count() FROM durable_e2e.events') == 1200 ]]
[[ $(restore_clickhouse 'SELECT uniqExact(id) FROM durable_e2e.events') == 1200 ]]

connector_request PUT /stop
wait_connector_state STOPPED
apply_recovery() {
  local recovery_manifest=$1 expected_backup=$2
  printf '%s\n' "$recovery_manifest" | k -n "$namespace" exec -i deployment/sink-durable-clickhouse-sink-connect -- \
    env \
      CONNECT_URL=http://localhost:8083 \
      CLICKHOUSE_URL=http://clickhouse-restore:8123/ \
      CLICKHOUSE_USERNAME=default \
      CLICKHOUSE_PASSWORD= \
      CONNECTOR_NAMES=sink-durable-clickhouse-sink-events \
      EXPECTED_BACKUP_NAME="$expected_backup" \
      RECOVERY_MANIFEST_FILE=- \
      STOP_TIMEOUT_SECONDS=120 \
      /bin/durable-clickhouse-recovery restore-offsets
}

assert_recovery_rejected() {
  local pathology=$1 recovery_manifest=$2 expected_backup=$3 offsets_before offsets_after
  offsets_before=$(connector_request GET /offsets | jq --sort-keys --compact-output .)
  if apply_recovery "$recovery_manifest" "$expected_backup" >/dev/null 2>&1; then
    echo "recovery accepted pathology: $pathology" >&2
    exit 1
  fi
  offsets_after=$(connector_request GET /offsets | jq --sort-keys --compact-output .)
  if [[ $offsets_after != "$offsets_before" ]]; then
    echo "rejected recovery changed Connect offsets: $pathology" >&2
    exit 1
  fi
}

assert_recovery_rejected missing-keeper-checkpoint "$manifest" "$backup"

restore_clickhouse "RESTORE TABLE durable_e2e.durable_sink_e2e_events_state FROM $checkpoint_backup"

offset_plus_one=$(jq --compact-output '(.connectors[0].offsets[0].offset.kafka_offset) += 1' <<<"$manifest")
offset_minus_one=$(jq --compact-output '(.connectors[0].offsets[0].offset.kafka_offset) -= 1' <<<"$manifest")
duplicate_offset=$(jq --compact-output '.connectors[0].offsets += [.connectors[0].offsets[0]]' <<<"$manifest")
keeper_ahead=$(jq --compact-output '
  (.connectors[0].keeper.rows[0].key | split("-") | last | tonumber) as $partition |
  (.connectors[0].keeper.rows[0].maxOffset) += 1 |
  (.connectors[0].offsets[] | select(.partition.kafka_partition == $partition).offset.kafka_offset) += 1
' <<<"$manifest")
assert_recovery_rejected exact-offset-plus-one "$offset_plus_one" "$backup"
assert_recovery_rejected exact-offset-minus-one "$offset_minus_one" "$backup"
assert_recovery_rejected duplicate-topic-partition "$duplicate_offset" "$backup"
assert_recovery_rejected restored-keeper-mismatch "$keeper_ahead" "$backup"
assert_recovery_rejected wrong-event-backup "$manifest" "$base"

connector_request PUT /resume
wait_connector_state RUNNING
assert_recovery_rejected connector-running "$manifest" "$backup"
connector_request PUT /stop
wait_connector_state STOPPED

apply_recovery "$manifest" "$backup" >/dev/null

helm --kubeconfig "$kubeconfig" upgrade sink "$chart" \
  --namespace "$namespace" --values "$values" \
  --set-string clickhouse.host=clickhouse-restore.durable-sink-e2e.svc.cluster.local \
  --set-string clickhouse.database=durable_e2e \
  --set backup.enabled=false \
  --wait --timeout 10m

connector_request PUT /resume
wait_connector_state RUNNING

start_producer post-restore-events 1251 1300 2
k -n "$namespace" wait pod/post-restore-events --for=jsonpath='{.status.phase}'=Succeeded --timeout=300s
for _ in {1..240}; do
  restored=$(restore_clickhouse 'SELECT count() FROM durable_e2e.events' 2>/dev/null || true)
  [[ $restored == 1300 ]] && break
  sleep 1
done
[[ $restored == 1300 ]]
[[ $(restore_clickhouse 'SELECT uniqExact(id) FROM durable_e2e.events') == 1300 ]]
[[ $(clickhouse 'SELECT count() FROM durable_e2e.events') == 1250 ]]

echo "RKE2 E2E passed: crash recovery, bounded deduplication, conflict quarantine, exact KeeperMap offsets, full/incremental rollover, adversarial recovery rejection, tail replay, and restore cutover"
