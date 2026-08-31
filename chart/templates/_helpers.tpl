{{- define "durable-clickhouse-sink.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "durable-clickhouse-sink.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name (include "durable-clickhouse-sink.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "durable-clickhouse-sink.labels" -}}
app.kubernetes.io/name: {{ include "durable-clickhouse-sink.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
{{- end }}

{{- define "durable-clickhouse-sink.pipelineName" -}}
{{- $root := index . 0 -}}
{{- $pipeline := index . 1 -}}
{{- printf "%s-%s" (include "durable-clickhouse-sink.fullname" $root) $pipeline.name | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "durable-clickhouse-sink.kafkaSecretVolume" -}}
{{- if .Values.kafka.existingSecret }}
- name: kafka-client
  secret:
    secretName: {{ .Values.kafka.existingSecret }}
    items:
      - key: {{ .Values.kafka.propertiesKey }}
        path: client.properties
{{- end }}
{{- end }}

{{- define "durable-clickhouse-sink.stateTable" -}}
{{- $root := index . 0 -}}
{{- $pipeline := index . 1 -}}
{{- printf "durable_sink_%s_%s_state" $root.Values.stateNamespace $pipeline.name | replace "-" "_" | trunc 127 -}}
{{- end }}

{{- define "durable-clickhouse-sink.backupPipelines" -}}
{{- $pipelines := list -}}
{{- range $pipeline := .Values.pipelines -}}
{{- $pipelines = append $pipelines (dict
  "connector" (include "durable-clickhouse-sink.pipelineName" (list $ $pipeline))
  "database" (default $.Values.clickhouse.database $pipeline.database)
  "state_table" (include "durable-clickhouse-sink.stateTable" (list $ $pipeline))
  "keeper_path" (printf "/durable-clickhouse-sink/%s/%s" $.Values.stateNamespace $pipeline.name)
  "table" $pipeline.table
  "topic" $pipeline.topic
  "partitions" (default $.Values.topics.partitions $pipeline.partitions)) -}}
{{- end -}}
{{- toJson $pipelines -}}
{{- end }}

{{- define "durable-clickhouse-sink.catalogTopic" -}}
{{- default (printf "%s.backup-catalog" (include "durable-clickhouse-sink.fullname" .)) .Values.backup.catalogTopic -}}
{{- end }}

{{- define "durable-clickhouse-sink.runtimeTimeouts" -}}
{{- toJson (dict
  "clickhouseConnectSeconds" .Values.timeouts.clickhouseConnectSeconds
  "connectConnectSeconds" .Values.timeouts.connectConnectSeconds
  "connectRequestSeconds" .Values.timeouts.connectRequestSeconds
  "connectPollSeconds" .Values.timeouts.connectPollSeconds
  "kafkaMetadataSeconds" .Values.timeouts.kafkaMetadataSeconds
  "kafkaCatalogAcquireSeconds" .Values.timeouts.kafkaCatalogAcquireSeconds
  "kafkaCatalogReadSeconds" .Values.timeouts.kafkaCatalogReadSeconds
  "kafkaTransactionSeconds" .Values.timeouts.kafkaTransactionSeconds
  "kafkaMaxPollSeconds" .Values.timeouts.kafkaMaxPollSeconds) -}}
{{- end }}
