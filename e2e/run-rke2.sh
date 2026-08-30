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
load_image connectImage ghcr.io/dialohq/durable-clickhouse-connect:e2e

manifests=$(nix build .#e2eManifests --no-link --print-out-paths)
k apply -f "$manifests/e2e/Namespace-durable-sink-e2e.yaml"
while read -r resource; do
  [[ $resource == */Namespace-durable-sink-e2e.yaml ]] && continue
  k apply -f "$resource"
done < <(find -L "$manifests/e2e" -maxdepth 1 -type f -name '*.yaml' | sort)

wait_for_ready_pod app=redpanda
wait_for_ready_pod app=clickhouse
wait_for_ready_pod app=minio
k -n "$namespace" wait job/minio-bucket --for=condition=Complete --timeout=300s
k -n "$namespace" wait job/schema --for=condition=Complete --timeout=300s

chart=$(nix build .#chartSource --no-link --print-out-paths)
values=$(nix build .#e2eValues --no-link --print-out-paths)
helm --kubeconfig "$kubeconfig" upgrade --install sink "$chart" \
  --namespace "$namespace" --values "$values" --wait --timeout 10m

wait_for_ready_pod app.kubernetes.io/component=connect
wait_for_ready_pod app.kubernetes.io/component=recovery-controller

clickhouse() {
  k -n "$namespace" exec clickhouse-0 -- clickhouse-client --query "$1"
}

connector_request() {
  local method=$1 path=$2
  k -n "$namespace" exec deployment/sink-durable-clickhouse-sink-connect -- \
    curl --fail --silent --show-error --request "$method" \
      "http://localhost:8083/connectors/sink-durable-clickhouse-sink-records$path"
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
  local name=$1 first=$2 last=$3 payload_bytes=${4:-0}
  # shellcheck disable=SC2016
  k -n "$namespace" run "$name" \
    --image=ghcr.io/dialohq/durable-clickhouse-connect:e2e \
    --image-pull-policy=Never \
    --restart=Never \
    --env="FIRST=$first" --env="LAST=$last" --env="PAYLOAD_BYTES=$payload_bytes" \
    --command -- /bin/bash -euc '
      for i in $(seq "$FIRST" "$LAST"); do
        if [[ $PAYLOAD_BYTES == 0 ]]; then
          payload="value-$i"
        else
          payload=$(head -c "$PAYLOAD_BYTES" /dev/urandom | base64 -w0)
        fi
        value=$(printf "{\"record_key\":\"record-%s\",\"recorded_at\":\"2026-08-29 12:00:00.000\",\"payload\":\"%s\"}" "$i" "$payload")
        printf "record-%s\t%s\n" "$i" "$value"
      done | /bin/kafka-console-producer.sh --bootstrap-server redpanda:9092 --topic records.input --property parse.key=true --property key.separator=$'"'"'\t'"'"'
    '
}

start_producer initial-records 1 100
k -n "$namespace" wait pod/initial-records --for=jsonpath='{.status.phase}'=Succeeded --timeout=300s

wait_count 100 'SELECT count() FROM durable_e2e.records'
wait_count 100 'SELECT uniqExact(record_key) FROM durable_e2e.records'

start_producer crash-records 101 1100 2048
for _ in {1..3}; do
  sleep 2
  k -n "$namespace" delete pod -l app.kubernetes.io/component=connect --grace-period=0 --force --wait=false
  wait_for_ready_pod app.kubernetes.io/component=connect
done
k -n "$namespace" wait pod/crash-records --for=jsonpath='{.status.phase}'=Succeeded --timeout=600s
wait_count 1100 'SELECT count() FROM durable_e2e.records'
wait_count 1100 'SELECT uniqExact(record_key) FROM durable_e2e.records'

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

wait_backup() {
  local job=$1
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

run_backup() {
  local job=$1
  k -n "$namespace" create job "$job" --from=cronjob/sink-durable-clickhouse-sink-backup >&2
  wait_backup "$job"
}

k -n "$namespace" create job e2e-backup-base --from=cronjob/sink-durable-clickhouse-sink-backup
backup_active=false
for _ in {1..300}; do
  backup_status=$(clickhouse "SELECT status FROM system.backups ORDER BY start_time DESC LIMIT 1" 2>/dev/null || true)
  connector_status=$(connector_request GET /status 2>/dev/null | jq -r '.connector.state' || true)
  if [[ $backup_status == CREATING_BACKUP && $connector_status == RUNNING ]]; then
    backup_active=true
    break
  fi
  sleep 0.1
done
[[ $backup_active == true ]]

start_producer during-base-upload 1101 1150
k -n "$namespace" wait pod/during-base-upload --for=jsonpath='{.status.phase}'=Succeeded --timeout=300s
wait_count 1150 'SELECT count() FROM durable_e2e.records'
base_output=$(wait_backup e2e-backup-base)
base=$(jq -r '.name' <<<"$base_output")
base_id=$(jq -r '.manifest.backup.id' <<<"$base_output")
base_manifest=$(jq --compact-output '.manifest' <<<"$base_output")
jq --exit-status '.backup.kind == "full" and .backup.position == 0 and (.backup | has("base") | not)' <<<"$base_manifest" >/dev/null

clickhouse "RESTORE TABLE durable_e2e.records AS durable_e2e.base_snapshot_probe FROM $base"
[[ $(clickhouse 'SELECT count() FROM durable_e2e.base_snapshot_probe') == 1100 ]]
clickhouse 'DROP TABLE durable_e2e.base_snapshot_probe SYNC'

incremental_one_output=$(run_backup e2e-backup-incremental-one)
incremental_one=$(jq -r '.name' <<<"$incremental_one_output")
incremental_one_id=$(jq -r '.manifest.backup.id' <<<"$incremental_one_output")
jq --exit-status --arg id "$base_id" --arg name "$base" '
  .manifest.backup.kind == "incremental" and
  .manifest.backup.position == 1 and
  .manifest.backup.base == {id: $id, name: $name}
' <<<"$incremental_one_output" >/dev/null

start_producer incremental-two-records 1151 1200
k -n "$namespace" wait pod/incremental-two-records --for=jsonpath='{.status.phase}'=Succeeded --timeout=300s
wait_count 1200 'SELECT count() FROM durable_e2e.records'
incremental_two_output=$(run_backup e2e-backup-incremental-two)
backup=$(jq -r '.name' <<<"$incremental_two_output")
backup_id=$(jq -r '.manifest.backup.id' <<<"$incremental_two_output")
manifest=$(jq --compact-output '.manifest' <<<"$incremental_two_output")
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
    --topic sink-durable-clickhouse-sink.backup-catalog \
    --partition 0 \
    --from-beginning \
    --timeout-ms 10000 2>/dev/null |
  jq --raw-input --compact-output --arg id "$backup_id" 'fromjson? | select(.backup.id == $id)' | tail -1)
if ! jq --exit-status --argjson expected "$manifest" '. == $expected' <<<"$kafka_manifest" >/dev/null; then
  echo "backup catalog did not return the Job manifest" >&2
  printf 'expected: %s\nactual: %s\n' "$manifest" "$kafka_manifest" >&2
  exit 1
fi

start_producer final-records 1201 1250
k -n "$namespace" wait pod/final-records --for=jsonpath='{.status.phase}'=Succeeded --timeout=300s
wait_count 1250 'SELECT count() FROM durable_e2e.records'

target_offsets=$(k -n "$namespace" exec deployment/sink-durable-clickhouse-sink-connect -- \
  /bin/kafka-get-offsets.sh --bootstrap-server redpanda:9092 --topic records.input --time -1 |
  jq --raw-input --slurp 'split("\n") | map(select(length > 0) | split(":") | {topic: .[0], partition: (.[1] | tonumber), offset: (.[2] | tonumber)})')

apply_table_recovery() {
  local name=$1 table=$2 targets=${3:-}
  if [[ -n $targets ]]; then
    jq -n --arg name "$name" --arg table "$table" --arg id "$backup_id" --argjson targets "$targets" '{
      apiVersion: "chbackup.dialo.ai/v1alpha1",
      kind: "TableRecovery",
      metadata: {name: $name},
      spec: {
        source: {database: "durable_e2e", table: "records", recoveryPointID: $id},
        destination: {database: "durable_e2e", table: $table},
        targetOffsets: $targets
      }
    }' | k -n "$namespace" apply -f -
  else
    jq -n --arg name "$name" --arg table "$table" --arg id "$backup_id" '{
      apiVersion: "chbackup.dialo.ai/v1alpha1",
      kind: "TableRecovery",
      metadata: {name: $name},
      spec: {
        source: {database: "durable_e2e", table: "records", recoveryPointID: $id},
        destination: {database: "durable_e2e", table: $table}
      }
    }' | k -n "$namespace" apply -f -
  fi
}

wait_recovery() {
  local name=$1 phase=$2
  k -n "$namespace" wait "tablerecovery/$name" \
    --for="jsonpath={.status.phase}=$phase" --timeout=900s
}

partial_offsets=$(jq '.[0:-1]' <<<"$target_offsets")
apply_table_recovery invalid-partial invalid_partial_records "$partial_offsets"
k -n "$namespace" wait tablerecovery/invalid-partial \
  --for=jsonpath='{.status.conditions[?(@.reason=="ReconcileFailed")].reason}'=ReconcileFailed --timeout=300s
[[ $(clickhouse 'SELECT count() FROM durable_e2e.invalid_partial_records') == 0 ]]

future_offsets=$(jq '.[0].offset += 1' <<<"$target_offsets")
apply_table_recovery invalid-future invalid_future_records "$future_offsets"
k -n "$namespace" wait tablerecovery/invalid-future \
  --for=jsonpath='{.status.conditions[?(@.reason=="ReconcileFailed")].reason}'=ReconcileFailed --timeout=300s
[[ $(clickhouse 'SELECT count() FROM durable_e2e.invalid_future_records') == 0 ]]

apply_table_recovery bounded-pitr pitr_records "$target_offsets"
for _ in {1..120}; do
  recovery_phase=$(k -n "$namespace" get tablerecovery/bounded-pitr -o jsonpath='{.status.phase}' 2>/dev/null || true)
  [[ $recovery_phase == Replaying ]] && break
  [[ $recovery_phase == Complete ]] && break
  sleep 1
done
if [[ $recovery_phase == Replaying ]]; then
  k -n "$namespace" delete pod -l app.kubernetes.io/component=recovery-controller --grace-period=0 --force --wait=false
  wait_for_ready_pod app.kubernetes.io/component=recovery-controller
fi
wait_recovery bounded-pitr Complete
wait_count 1250 'SELECT count() FROM durable_e2e.pitr_records'
wait_count 1250 'SELECT uniqExact(record_key) FROM durable_e2e.pitr_records'
bounded_follow=$(k -n "$namespace" get tablerecovery/bounded-pitr -o jsonpath='{.status.followConnectors[0]}')
bounded_state=$(k -n "$namespace" exec deployment/sink-durable-clickhouse-sink-connect -- \
  curl --fail --silent "http://localhost:8083/connectors/$bounded_follow/status" | jq -r '.connector.state')
[[ $bounded_state == STOPPED ]]

k -n "$namespace" scale deployment/sink-durable-clickhouse-sink-recovery --replicas=0
for _ in {1..120}; do
  [[ -z $(k -n "$namespace" get pods -l app.kubernetes.io/component=recovery-controller -o name) ]] && break
  sleep 1
done
[[ -z $(k -n "$namespace" get pods -l app.kubernetes.io/component=recovery-controller -o name) ]]
apply_table_recovery ambiguous-restore ambiguous_restore_records "$target_offsets"
k -n "$namespace" patch tablerecovery/ambiguous-restore --subresource=status --type=merge \
  --patch '{"status":{"phase":"Restoring"}}'
clickhouse "INSERT INTO durable_e2e.ambiguous_restore_records VALUES ('ambiguous', now64(3), 'partial')"
apply_table_recovery live-follow live_records
live_uid=$(k -n "$namespace" get tablerecovery/live-follow -o jsonpath='{.metadata.uid}')
live_token=${live_uid//-/}
live_connector="dcs-$live_token-follow-0"
live_state_table="dcs_recovery_${live_token}_follow_0_state"
live_keeper_path="/durable-clickhouse-sink/recovery/$live_token/follow/0"
clickhouse "CREATE TABLE durable_e2e.$live_state_table (key String, minOffset Int64, maxOffset Int64, state String) ENGINE = KeeperMap('$live_keeper_path') PRIMARY KEY key"
live_config=$(connector_request GET /config | jq \
  --arg table "$live_state_table" --arg path "$live_keeper_path" '
    del(.name, ."topics.regex", ."consumer.override.group.id") |
    .topics = "records.input" |
    .topic2TableMap = "records.input=live_records" |
    .hostname = "clickhouse.durable-sink-e2e.svc.cluster.local" |
    .port = "8123" |
    .ssl = "false" |
    .database = "durable_e2e" |
    .zkPath = $path |
    .zkDatabase = $table |
    ."consumer.override.isolation.level" = "read_committed" |
    ."consumer.override.auto.offset.reset" = "none"
  ')
jq -n --arg name "$live_connector" --argjson config "$live_config" \
  '{name: $name, config: $config, initial_state: "STOPPED"}' |
  k -n "$namespace" exec -i deployment/sink-durable-clickhouse-sink-connect -- \
    curl --fail --silent --show-error --header 'Content-Type: application/json' \
      --data-binary @- http://localhost:8083/connectors >/dev/null
k -n "$namespace" scale deployment/sink-durable-clickhouse-sink-recovery --replicas=1
wait_for_ready_pod app.kubernetes.io/component=recovery-controller
k -n "$namespace" wait tablerecovery/ambiguous-restore \
  --for=jsonpath='{.status.conditions[?(@.reason=="ReconcileFailed")].reason}'=ReconcileFailed --timeout=300s
[[ $(clickhouse 'SELECT count() FROM durable_e2e.ambiguous_restore_records') == 1 ]]
wait_recovery live-follow Streaming
wait_count 1250 'SELECT count() FROM durable_e2e.live_records'
start_producer live-follow-records 1251 1260
k -n "$namespace" wait pod/live-follow-records --for=jsonpath='{.status.phase}'=Succeeded --timeout=300s
wait_count 1260 'SELECT count() FROM durable_e2e.records'
wait_count 1260 'SELECT count() FROM durable_e2e.live_records'
wait_count 1260 'SELECT uniqExact(record_key) FROM durable_e2e.live_records'

rollover_output=$(run_backup e2e-backup-rollover)
jq --exit-status '.manifest.backup.kind == "full" and .manifest.backup.position == 0' <<<"$rollover_output" >/dev/null

echo "RKE2 E2E passed: crash retries, snapshot concurrency, incremental rollover, adversarial offset rejection, bounded PITR restart, live follow, KeeperMap rehydration, tail replay, and restore cutover"
