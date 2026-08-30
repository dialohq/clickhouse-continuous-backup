{backup, lib, runCommand, symlinkJoin, writeTextDir}:
let
  recoveryCrd = runCommand "durable-clickhouse-sink-recovery-crd" {} ''
    mkdir -p $out/crds
    ${backup}/bin/durable-clickhouse-backup print-recovery-crd > $out/crds/table-recovery.yaml
  '';
  files = {
    "Chart.yaml" = ''
      apiVersion: v2
      name: durable-clickhouse-sink
      description: Durable Kafka-compatible ingestion into ClickHouse with incremental backups
      type: application
      version: 0.1.0
      appVersion: 0.1.0
      home: https://github.com/dialohq/durable-clickhouse-sink
      sources:
        - https://github.com/dialohq/durable-clickhouse-sink
      annotations:
        artifacthub.io/license: Apache-2.0
    '';

    "values.yaml" = ''
      nameOverride: ""
      fullnameOverride: ""
      imagePullSecrets: []
      stateNamespace: default
      serviceAccount:
        annotations: {}

      kafka:
        bootstrapServers: ""
        existingSecret: ""
        propertiesKey: client.properties
        replicationFactor: 3
        internalTopicReplicationFactor: 3

      clickhouse:
        host: ""
        port: 8443
        secure: true
        database: default
        credentialsSecret:
          name: ""
          propertiesKey: clickhouse.properties

      connect:
        image:
          repository: ghcr.io/dialohq/durable-clickhouse-connect
          tag: 0.1.0
          pullPolicy: IfNotPresent
        podAnnotations: {}
        replicas: 2
        tasksMax: 1
        deleteConnectorsOnUninstall: true
        readinessProbe:
          periodSeconds: 5
          failureThreshold: 30
        livenessProbe:
          initialDelaySeconds: 30
          periodSeconds: 15
        resources:
          requests:
            cpu: 250m
            memory: 512Mi
          limits:
            memory: 2Gi

      timeouts:
        clickhouseConnectSeconds: 10
        connectConnectSeconds: 5
        connectRequestSeconds: 15
        connectPollSeconds: 1
        kafkaMetadataSeconds: 15
        kafkaCatalogAcquireSeconds: 30
        kafkaCatalogReadSeconds: 15
        kafkaTransactionSeconds: 30
        kafkaMaxPollSeconds: 86400
        kafkaReplayPollSeconds: 15
        recoveryCatchupSeconds: 3600
        controllerRetrySeconds: 15
        hookJobSeconds: 600

      topics:
        manage: true
        partitions: 6

      backup:
        enabled: false
        schedule: "17 2 * * *"
        suspend: false
        namedCollection: durable_clickhouse_backups
        pathPrefix: durable-clickhouse-sink
        archiveExtension: tar.zst
        maxIncrementalsPerFull: 0
        maxBandwidthBytesPerSecond: 0
        pauseTimeoutSeconds: 120
        activeDeadlineSeconds: 21600
        terminationGracePeriodSeconds: 300
        catalogTopic: ""
        kafkaPropertiesKey: librdkafka.properties
        credentialsSecret:
          name: ""
          usernameKey: username
          passwordKey: password
        successfulJobsHistoryLimit: 3
        failedJobsHistoryLimit: 3

      recovery:
        credentialsSecret:
          name: ""
          propertiesKey: clickhouse.properties
        replayTopicReplicationFactor: 3
        replayTopicRetentionMs: 604800000
        replayBatchRecords: 500
        resources:
          requests:
            cpu: 100m
            memory: 128Mi
          limits:
            memory: 512Mi

      pipelines: []
      # - name: records
      #   topic: records.input
      #   table: records
      #   database: default
      #   retentionMs: 7776000000
      #   partitions: 6
      #   valueConverter: org.apache.kafka.connect.json.JsonConverter
      #   valueConverterSchemasEnable: false
      #   connectorConfig: {}
    '';

    "values.schema.json" = builtins.toJSON {
      "$schema" = "https://json-schema.org/draft/2020-12/schema";
      type = "object";
      required = ["kafka" "clickhouse" "timeouts" "recovery" "pipelines"];
      properties = {
        stateNamespace = {type = "string"; pattern = "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"; maxLength = 30;};
        kafka = {
          type = "object";
          required = ["bootstrapServers"];
          properties = {
            bootstrapServers = {type = "string"; minLength = 1;};
            existingSecret = {type = "string";};
            propertiesKey = {type = "string"; minLength = 1;};
            replicationFactor = {type = "integer"; minimum = 1;};
            internalTopicReplicationFactor = {type = "integer"; minimum = 1;};
          };
        };
        clickhouse = {
          type = "object";
          required = ["host" "credentialsSecret"];
          properties = {
            host = {type = "string"; minLength = 1;};
            port = {type = "integer"; minimum = 1; maximum = 65535;};
            secure = {type = "boolean";};
            database = {type = "string"; pattern = "^[A-Za-z_][A-Za-z0-9_]*$";};
            credentialsSecret = {
              type = "object";
              required = ["name" "propertiesKey"];
              properties = {
                name = {type = "string"; minLength = 1;};
                propertiesKey = {type = "string"; minLength = 1;};
              };
            };
          };
        };
        pipelines = {
          type = "array";
          minItems = 1;
          items = {
            type = "object";
            required = ["name" "topic" "table"];
            properties = {
              name = {type = "string"; pattern = "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"; maxLength = 40;};
              topic = {type = "string"; pattern = "^[A-Za-z0-9._-]+$"; maxLength = 249;};
              table = {type = "string"; pattern = "^[A-Za-z_][A-Za-z0-9_]*$";};
              database = {type = "string"; pattern = "^[A-Za-z_][A-Za-z0-9_]*$";};
              retentionMs = {type = "integer"; minimum = 60000; default = 7776000000;};
              partitions = {type = "integer"; minimum = 1;};
              valueConverter = {type = "string"; minLength = 1;};
              valueConverterSchemasEnable = {type = "boolean";};
              connectorConfig = {type = "object"; additionalProperties = true;};
            };
          };
        };
        timeouts = {
          type = "object";
          required = [
            "clickhouseConnectSeconds"
            "connectConnectSeconds"
            "connectRequestSeconds"
            "connectPollSeconds"
            "kafkaMetadataSeconds"
            "kafkaCatalogAcquireSeconds"
            "kafkaCatalogReadSeconds"
            "kafkaTransactionSeconds"
            "kafkaMaxPollSeconds"
            "kafkaReplayPollSeconds"
            "recoveryCatchupSeconds"
            "controllerRetrySeconds"
            "hookJobSeconds"
          ];
          properties = {
            clickhouseConnectSeconds = {type = "integer"; minimum = 1; maximum = 3600;};
            connectConnectSeconds = {type = "integer"; minimum = 1; maximum = 3600;};
            connectRequestSeconds = {type = "integer"; minimum = 1; maximum = 3600;};
            connectPollSeconds = {type = "integer"; minimum = 1; maximum = 300;};
            kafkaMetadataSeconds = {type = "integer"; minimum = 1; maximum = 3600;};
            kafkaCatalogAcquireSeconds = {type = "integer"; minimum = 1; maximum = 3600;};
            kafkaCatalogReadSeconds = {type = "integer"; minimum = 1; maximum = 3600;};
            kafkaTransactionSeconds = {type = "integer"; minimum = 1; maximum = 3600;};
            kafkaMaxPollSeconds = {type = "integer"; minimum = 1; maximum = 604800;};
            kafkaReplayPollSeconds = {type = "integer"; minimum = 1; maximum = 3600;};
            recoveryCatchupSeconds = {type = "integer"; minimum = 1; maximum = 604800;};
            controllerRetrySeconds = {type = "integer"; minimum = 1; maximum = 3600;};
            hookJobSeconds = {type = "integer"; minimum = 1; maximum = 86400;};
          };
        };
        backup = {
          type = "object";
          properties = {
            enabled = {type = "boolean";};
            schedule = {type = "string"; minLength = 1;};
            suspend = {type = "boolean";};
            namedCollection = {type = "string"; pattern = "^[A-Za-z_][A-Za-z0-9_]*$";};
            pathPrefix = {type = "string"; pattern = "^[A-Za-z0-9_./-]+$";};
            archiveExtension = {type = "string"; enum = ["tar.zst" "tar.gz" "tar.xz" "tar.bz2" "tgz" "tzst"];};
            maxIncrementalsPerFull = {type = "integer"; minimum = 0; maximum = 9999;};
            maxBandwidthBytesPerSecond = {type = "integer"; minimum = 0;};
            pauseTimeoutSeconds = {type = "integer"; minimum = 1; maximum = 3600;};
            activeDeadlineSeconds = {type = "integer"; minimum = 1; maximum = 604800;};
            terminationGracePeriodSeconds = {type = "integer"; minimum = 1; maximum = 3600;};
            catalogTopic = {type = "string"; pattern = "^$|^[A-Za-z0-9._-]+$"; maxLength = 249;};
            kafkaPropertiesKey = {type = "string"; minLength = 1;};
            credentialsSecret = {
              type = "object";
              properties = {
                name = {type = "string";};
                usernameKey = {type = "string"; minLength = 1;};
                passwordKey = {type = "string"; minLength = 1;};
              };
            };
            successfulJobsHistoryLimit = {type = "integer"; minimum = 0;};
            failedJobsHistoryLimit = {type = "integer"; minimum = 0;};
          };
        };
        recovery = {
          type = "object";
          required = ["credentialsSecret" "replayTopicReplicationFactor" "replayTopicRetentionMs" "replayBatchRecords"];
          properties = {
            credentialsSecret = {
              type = "object";
              required = ["name" "propertiesKey"];
              properties = {
                name = {type = "string"; minLength = 1;};
                propertiesKey = {type = "string"; minLength = 1;};
              };
            };
            replayTopicReplicationFactor = {type = "integer"; minimum = 1;};
            replayTopicRetentionMs = {type = "integer"; minimum = 60000;};
            replayBatchRecords = {type = "integer"; minimum = 1; maximum = 100000;};
            resources = {type = "object";};
          };
        };
      };
    };

    "templates/_helpers.tpl" = ''
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
        "kafkaMaxPollSeconds" .Values.timeouts.kafkaMaxPollSeconds
        "kafkaReplayPollSeconds" .Values.timeouts.kafkaReplayPollSeconds
        "recoveryCatchupSeconds" .Values.timeouts.recoveryCatchupSeconds
        "controllerRetrySeconds" .Values.timeouts.controllerRetrySeconds) -}}
      {{- end }}
    '';

    "templates/validate.yaml" = ''
      {{- $names := dict -}}
      {{- $topics := dict -}}
      {{- $reserved := list "connector.class" "tasks.max" "topics" "topics.regex" "topic2TableMap" "hostname" "port" "ssl" "database" "username" "password" "exactlyOnce" "errors.tolerance" "bufferCount" "consumer.override.group.id" "consumer.override.isolation.level" "zkPath" "zkDatabase" -}}
      {{- range $pipeline := .Values.pipelines }}
      {{- if hasKey $names $pipeline.name }}
      {{- fail (printf "pipeline names must be unique: %s" $pipeline.name) }}
      {{- end }}
      {{- $_ := set $names $pipeline.name true }}
      {{- if hasKey $topics $pipeline.topic }}
      {{- fail (printf "topic names must be unique across pipelines: %s" $pipeline.topic) }}
      {{- end }}
      {{- $_ := set $topics $pipeline.topic true }}
      {{- range $key, $_ := default dict $pipeline.connectorConfig }}
      {{- if has $key $reserved }}
      {{- fail (printf "pipeline %s cannot override safety-critical connector setting %s" $pipeline.name $key) }}
      {{- end }}
      {{- end }}
      {{- end }}
      {{- if and .Values.backup.enabled (not .Values.backup.credentialsSecret.name) }}
      {{- fail "backup.credentialsSecret.name is required when backups are enabled" }}
      {{- end }}
      {{- if not .Values.recovery.credentialsSecret.name }}
      {{- fail "recovery.credentialsSecret.name is required" }}
      {{- end }}
      {{- $minimumResumeGrace := mul (len .Values.pipelines) (int .Values.timeouts.connectRequestSeconds) }}
      {{- if le (int .Values.backup.terminationGracePeriodSeconds) (int $minimumResumeGrace) }}
      {{- fail "backup.terminationGracePeriodSeconds must exceed connectRequestSeconds multiplied by the pipeline count" }}
      {{- end }}
      {{- range $segment := splitList "/" (trimSuffix "/" .Values.backup.pathPrefix) }}
      {{- if or (eq $segment "") (eq $segment ".") (eq $segment "..") }}
      {{- fail "backup.pathPrefix must be a relative object path without empty, . or .. segments" }}
      {{- end }}
      {{- end }}
    '';

    "templates/serviceaccount.yaml" = ''
      apiVersion: v1
      kind: ServiceAccount
      metadata:
        name: {{ include "durable-clickhouse-sink.fullname" . }}
        labels:
          {{- include "durable-clickhouse-sink.labels" . | nindent 4 }}
        {{- with .Values.serviceAccount.annotations }}
        annotations:
          {{- toYaml . | nindent 4 }}
        {{- end }}
      automountServiceAccountToken: false
    '';

    "templates/connect-configmap.yaml" = ''
      apiVersion: v1
      kind: ConfigMap
      metadata:
        name: {{ include "durable-clickhouse-sink.fullname" . }}-connect
        labels:
          {{- include "durable-clickhouse-sink.labels" . | nindent 4 }}
      data:
        connect-distributed.properties: |
          bootstrap.servers={{ .Values.kafka.bootstrapServers }}
          group.id={{ include "durable-clickhouse-sink.fullname" . }}-connect
          config.storage.topic={{ include "durable-clickhouse-sink.fullname" . }}.connect-configs
          offset.storage.topic={{ include "durable-clickhouse-sink.fullname" . }}.connect-offsets
          status.storage.topic={{ include "durable-clickhouse-sink.fullname" . }}.connect-status
          config.storage.replication.factor={{ .Values.kafka.internalTopicReplicationFactor }}
          offset.storage.replication.factor={{ .Values.kafka.internalTopicReplicationFactor }}
          status.storage.replication.factor={{ .Values.kafka.internalTopicReplicationFactor }}
          key.converter=org.apache.kafka.connect.storage.StringConverter
          value.converter=org.apache.kafka.connect.json.JsonConverter
          value.converter.schemas.enable=false
          plugin.path=/plugins
          rest.port=8083
          config.providers=file
          config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
          connector.client.config.override.policy=All
          offset.flush.interval.ms=1000
    '';

    "templates/runtime-config.yaml" = ''
      apiVersion: v1
      kind: ConfigMap
      metadata:
        name: {{ include "durable-clickhouse-sink.fullname" . }}-runtime
        labels:
          {{- include "durable-clickhouse-sink.labels" . | nindent 4 }}
      data:
        target.json: |
          {
            "clickhouseUrl": {{ printf "%s://%s:%v/" (ternary "https" "http" .Values.clickhouse.secure) .Values.clickhouse.host .Values.clickhouse.port | quote }},
            "clickhousePropertiesFile": "/etc/clickhouse/clickhouse.properties",
            "pipelines": {{ include "durable-clickhouse-sink.backupPipelines" . }},
            "timeouts": {{ include "durable-clickhouse-sink.runtimeTimeouts" . }}
          }
        backup.json: |
          {
            "connectUrl": "http://{{ include "durable-clickhouse-sink.fullname" . }}-connect:8083",
            "clickhouseUrl": {{ printf "%s://%s:%v/" (ternary "https" "http" .Values.clickhouse.secure) .Values.clickhouse.host .Values.clickhouse.port | quote }},
            "namedCollection": {{ .Values.backup.namedCollection | quote }},
            "pathPrefix": {{ trimSuffix "/" .Values.backup.pathPrefix | quote }},
            "archiveExtension": {{ .Values.backup.archiveExtension | quote }},
            "pauseTimeoutSeconds": {{ .Values.backup.pauseTimeoutSeconds }},
            "kafkaBootstrapServers": {{ .Values.kafka.bootstrapServers | quote }},
            "kafkaPropertiesFile": {{ ternary (quote "/etc/kafka-backup/client.properties") "null" (not (empty .Values.kafka.existingSecret)) }},
            "catalogTopic": {{ include "durable-clickhouse-sink.catalogTopic" . | quote }},
            "maxIncrementalsPerFull": {{ .Values.backup.maxIncrementalsPerFull }},
            "maxBackupBandwidth": {{ .Values.backup.maxBandwidthBytesPerSecond }},
            "pipelines": {{ include "durable-clickhouse-sink.backupPipelines" . }},
            "timeouts": {{ include "durable-clickhouse-sink.runtimeTimeouts" . }}
          }
        recovery.json: |
          {
            "connectUrl": "http://{{ include "durable-clickhouse-sink.fullname" . }}-connect:8083",
            "clickhouseUrl": {{ printf "%s://%s:%v/" (ternary "https" "http" .Values.clickhouse.secure) .Values.clickhouse.host .Values.clickhouse.port | quote }},
            "clickhousePropertiesFile": "/etc/clickhouse-recovery/clickhouse.properties",
            "clickhouseConnectorHost": {{ .Values.clickhouse.host | quote }},
            "clickhouseConnectorPort": {{ .Values.clickhouse.port }},
            "clickhouseConnectorSecure": {{ .Values.clickhouse.secure }},
            "kafkaBootstrapServers": {{ .Values.kafka.bootstrapServers | quote }},
            "kafkaPropertiesFile": {{ ternary (quote "/etc/kafka-recovery/client.properties") "null" (not (empty .Values.kafka.existingSecret)) }},
            "catalogTopic": {{ include "durable-clickhouse-sink.catalogTopic" . | quote }},
            "replayTopicReplicationFactor": {{ .Values.recovery.replayTopicReplicationFactor }},
            "replayTopicRetentionMs": {{ int64 .Values.recovery.replayTopicRetentionMs }},
            "replayBatchRecords": {{ .Values.recovery.replayBatchRecords }},
            "timeouts": {{ include "durable-clickhouse-sink.runtimeTimeouts" . }}
          }
    '';

    "templates/connect.yaml" = ''
      apiVersion: v1
      kind: Service
      metadata:
        name: {{ include "durable-clickhouse-sink.fullname" . }}-connect
        labels:
          {{- include "durable-clickhouse-sink.labels" . | nindent 4 }}
          app.kubernetes.io/component: connect
      spec:
        selector:
          app.kubernetes.io/name: {{ include "durable-clickhouse-sink.name" . }}
          app.kubernetes.io/instance: {{ .Release.Name }}
          app.kubernetes.io/component: connect
        ports:
          - name: http
            port: 8083
            targetPort: http
      ---
      apiVersion: apps/v1
      kind: Deployment
      metadata:
        name: {{ include "durable-clickhouse-sink.fullname" . }}-connect
        labels:
          {{- include "durable-clickhouse-sink.labels" . | nindent 4 }}
          app.kubernetes.io/component: connect
      spec:
        replicas: {{ .Values.connect.replicas }}
        selector:
          matchLabels:
            app.kubernetes.io/name: {{ include "durable-clickhouse-sink.name" . }}
            app.kubernetes.io/instance: {{ .Release.Name }}
            app.kubernetes.io/component: connect
        template:
          metadata:
            labels:
              {{- include "durable-clickhouse-sink.labels" . | nindent 8 }}
              app.kubernetes.io/component: connect
            annotations:
              checksum/config: {{ include (print $.Template.BasePath "/connect-configmap.yaml") . | sha256sum }}
              {{- with .Values.connect.podAnnotations }}
              {{- toYaml . | nindent 8 }}
              {{- end }}
          spec:
            serviceAccountName: {{ include "durable-clickhouse-sink.fullname" . }}
            automountServiceAccountToken: false
            securityContext:
              runAsNonRoot: true
              runAsUser: 65532
              runAsGroup: 65532
              fsGroup: 65532
            {{- with .Values.imagePullSecrets }}
            imagePullSecrets:
              {{- toYaml . | nindent 8 }}
            {{- end }}
            initContainers:
              - name: configure
                image: "{{ .Values.connect.image.repository }}:{{ .Values.connect.image.tag }}"
                imagePullPolicy: {{ .Values.connect.image.pullPolicy }}
                command: ["/bin/bash", "-euc"]
                args:
                  - |
                    cp /config/connect-distributed.properties /work/connect-distributed.properties
                    {{- if .Values.kafka.existingSecret }}
                    printf '\n' >> /work/connect-distributed.properties
                    cat /kafka/client.properties >> /work/connect-distributed.properties
                    {{- end }}
                volumeMounts:
                  - name: connect-config
                    mountPath: /config
                    readOnly: true
                  - name: connect-work
                    mountPath: /work
                  {{- if .Values.kafka.existingSecret }}
                  - name: kafka-client
                    mountPath: /kafka
                    readOnly: true
                  {{- end }}
            containers:
              - name: connect
                image: "{{ .Values.connect.image.repository }}:{{ .Values.connect.image.tag }}"
                imagePullPolicy: {{ .Values.connect.image.pullPolicy }}
                command: ["/bin/connect-distributed.sh", "/etc/durable-clickhouse/connect-distributed.properties"]
                ports:
                  - name: http
                    containerPort: 8083
                readinessProbe:
                  httpGet: {path: /, port: http}
                  periodSeconds: {{ .Values.connect.readinessProbe.periodSeconds }}
                  failureThreshold: {{ .Values.connect.readinessProbe.failureThreshold }}
                livenessProbe:
                  httpGet: {path: /, port: http}
                  initialDelaySeconds: {{ .Values.connect.livenessProbe.initialDelaySeconds }}
                  periodSeconds: {{ .Values.connect.livenessProbe.periodSeconds }}
                resources:
                  {{- toYaml .Values.connect.resources | nindent 18 }}
                volumeMounts:
                  - name: connect-work
                    mountPath: /etc/durable-clickhouse
                  - name: clickhouse-credentials
                    mountPath: /etc/clickhouse
                    readOnly: true
                  - {name: tmp, mountPath: /tmp}
            volumes:
              - name: connect-config
                configMap:
                  name: {{ include "durable-clickhouse-sink.fullname" . }}-connect
              - name: connect-work
                emptyDir: {}
              - name: clickhouse-credentials
                secret:
                  secretName: {{ .Values.clickhouse.credentialsSecret.name }}
                  items:
                    - key: {{ .Values.clickhouse.credentialsSecret.propertiesKey }}
                      path: clickhouse.properties
              - {name: tmp, emptyDir: {}}
              {{- include "durable-clickhouse-sink.kafkaSecretVolume" . | nindent 6 }}
    '';

    "templates/topics.yaml" = ''
      {{- if .Values.topics.manage }}
      {{- range $pipeline := .Values.pipelines }}
      {{- $name := include "durable-clickhouse-sink.pipelineName" (list $ $pipeline) }}
      apiVersion: batch/v1
      kind: Job
      metadata:
        name: {{ $name }}-topics
        labels:
          {{- include "durable-clickhouse-sink.labels" $ | nindent 4 }}
          app.kubernetes.io/component: topic-manager
        annotations:
          helm.sh/hook: pre-install,pre-upgrade
          helm.sh/hook-weight: "-5"
          helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded
      spec:
        backoffLimit: 6
        activeDeadlineSeconds: {{ $.Values.timeouts.hookJobSeconds }}
        template:
          metadata:
            labels:
              {{- include "durable-clickhouse-sink.labels" $ | nindent 8 }}
              app.kubernetes.io/component: topic-manager
          spec:
            restartPolicy: OnFailure
            automountServiceAccountToken: false
            securityContext: {runAsNonRoot: true, runAsUser: 65532, runAsGroup: 65532}
            containers:
              - name: topics
                image: "{{ $.Values.connect.image.repository }}:{{ $.Values.connect.image.tag }}"
                imagePullPolicy: {{ $.Values.connect.image.pullPolicy }}
                command: ["/bin/bash", "-euc"]
                args:
                  - |
                    config=()
                    if [[ -f /etc/kafka/client.properties ]]; then config=(--command-config /etc/kafka/client.properties); fi
                    create() {
                      /bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVERS" "''${config[@]}" --create --if-not-exists --topic "$1" --partitions "$PARTITIONS" --replication-factor "$REPLICATION_FACTOR"
                    }
                    verify_partitions() {
                      description=$(/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVERS" "''${config[@]}" --describe --topic "$1")
                      [[ "$description" =~ PartitionCount:[[:space:]]+$PARTITIONS([[:space:]]|$) ]] || { echo "$1 does not have $PARTITIONS partitions: $description" >&2; exit 1; }
                    }
                    configure() {
                      /bin/kafka-configs.sh --bootstrap-server "$BOOTSTRAP_SERVERS" "''${config[@]}" --entity-type topics --entity-name "$1" --alter --add-config "$2"
                    }
                    create "$TOPIC"
                    verify_partitions "$TOPIC"
                    configure "$TOPIC" "cleanup.policy=delete,retention.ms=$RETENTION,retention.bytes=-1"
                env:
                  - {name: BOOTSTRAP_SERVERS, value: {{ $.Values.kafka.bootstrapServers | quote }}}
                  - {name: TOPIC, value: {{ $pipeline.topic | quote }}}
                  - {name: RETENTION, value: {{ int64 (default 7776000000 $pipeline.retentionMs) | quote }}}
                  - {name: PARTITIONS, value: {{ default $.Values.topics.partitions $pipeline.partitions | quote }}}
                  - {name: REPLICATION_FACTOR, value: {{ $.Values.kafka.replicationFactor | quote }}}
                {{- if $.Values.kafka.existingSecret }}
                volumeMounts:
                  - {name: kafka-client, mountPath: /etc/kafka, readOnly: true}
                  - {name: tmp, mountPath: /tmp}
                {{- else }}
                volumeMounts:
                  - {name: tmp, mountPath: /tmp}
                {{- end }}
            volumes:
              - {name: tmp, emptyDir: {}}
              {{- if $.Values.kafka.existingSecret }}
              {{- include "durable-clickhouse-sink.kafkaSecretVolume" $ | nindent 6 }}
              {{- end }}
      ---
      {{- end }}
      {{- end }}
      {{- if and .Values.backup.enabled .Values.topics.manage }}
      apiVersion: batch/v1
      kind: Job
      metadata:
        name: {{ include "durable-clickhouse-sink.fullname" . }}-backup-catalog-topic
        labels:
          {{- include "durable-clickhouse-sink.labels" . | nindent 4 }}
          app.kubernetes.io/component: topic-manager
        annotations:
          helm.sh/hook: pre-install,pre-upgrade
          helm.sh/hook-weight: "-5"
          helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded
      spec:
        backoffLimit: 6
        activeDeadlineSeconds: {{ .Values.timeouts.hookJobSeconds }}
        template:
          metadata:
            labels:
              {{- include "durable-clickhouse-sink.labels" . | nindent 8 }}
              app.kubernetes.io/component: topic-manager
          spec:
            restartPolicy: OnFailure
            automountServiceAccountToken: false
            securityContext: {runAsNonRoot: true, runAsUser: 65532, runAsGroup: 65532}
            containers:
              - name: topic
                image: "{{ .Values.connect.image.repository }}:{{ .Values.connect.image.tag }}"
                imagePullPolicy: {{ .Values.connect.image.pullPolicy }}
                command: ["/bin/bash", "-euc"]
                args:
                  - |
                    config=()
                    if [[ -f /etc/kafka/client.properties ]]; then config=(--command-config /etc/kafka/client.properties); fi
                    /bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVERS" "''${config[@]}" --create --if-not-exists --topic "$BACKUP_CATALOG_TOPIC" --partitions 1 --replication-factor "$REPLICATION_FACTOR"
                    description=$(/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVERS" "''${config[@]}" --describe --topic "$BACKUP_CATALOG_TOPIC")
                    [[ "$description" =~ PartitionCount:[[:space:]]+1([[:space:]]|$) ]] || { echo "$BACKUP_CATALOG_TOPIC does not have 1 partition: $description" >&2; exit 1; }
                    /bin/kafka-configs.sh --bootstrap-server "$BOOTSTRAP_SERVERS" "''${config[@]}" --entity-type topics --entity-name "$BACKUP_CATALOG_TOPIC" --alter --add-config cleanup.policy=compact,retention.ms=-1,retention.bytes=-1
                env:
                  - {name: BOOTSTRAP_SERVERS, value: {{ .Values.kafka.bootstrapServers | quote }}}
                  - {name: BACKUP_CATALOG_TOPIC, value: {{ include "durable-clickhouse-sink.catalogTopic" . | quote }}}
                  - {name: REPLICATION_FACTOR, value: {{ .Values.kafka.replicationFactor | quote }}}
                volumeMounts:
                  - {name: tmp, mountPath: /tmp}
                  {{- if .Values.kafka.existingSecret }}
                  - {name: kafka-client, mountPath: /etc/kafka, readOnly: true}
                  {{- end }}
            volumes:
              - {name: tmp, emptyDir: {}}
              {{- include "durable-clickhouse-sink.kafkaSecretVolume" . | nindent 6 }}
      ---
      {{- end }}
    '';

    "templates/connectors.yaml" = ''
      {{- range $pipeline := .Values.pipelines }}
      {{- $name := include "durable-clickhouse-sink.pipelineName" (list $ $pipeline) }}
      apiVersion: v1
      kind: ConfigMap
      metadata:
        name: {{ $name }}-connector
        labels:
          {{- include "durable-clickhouse-sink.labels" $ | nindent 4 }}
          app.kubernetes.io/component: connector
      data:
        config.json: |
          {
            "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
            "tasks.max": {{ $.Values.connect.tasksMax | quote }},
            "topics": {{ $pipeline.topic | quote }},
            "topic2TableMap": {{ printf "%s=%s" $pipeline.topic $pipeline.table | quote }},
            "hostname": {{ $.Values.clickhouse.host | quote }},
            "port": {{ $.Values.clickhouse.port | quote }},
            "ssl": {{ ternary "true" "false" $.Values.clickhouse.secure | quote }},
            "database": {{ default $.Values.clickhouse.database $pipeline.database | quote }},
            "username": "''${file:/etc/clickhouse/clickhouse.properties:username}",
            "password": "''${file:/etc/clickhouse/clickhouse.properties:password}",
            "exactlyOnce": "true",
            "consumer.override.isolation.level": "read_committed",
            "errors.tolerance": "none",
            "bufferCount": "0",
            "zkPath": {{ printf "/durable-clickhouse-sink/%s/%s" $.Values.stateNamespace $pipeline.name | quote }},
            "zkDatabase": {{ include "durable-clickhouse-sink.stateTable" (list $ $pipeline) | quote }},
            "value.converter": {{ default "org.apache.kafka.connect.json.JsonConverter" $pipeline.valueConverter | quote }},
            "value.converter.schemas.enable": {{ ternary "true" "false" (default false $pipeline.valueConverterSchemasEnable) | quote }},
            "clickhouseSettings": "async_insert=0,insert_deduplicate=1"{{- range $key, $value := default dict $pipeline.connectorConfig }},
            {{ $key | quote }}: {{ $value | toString | quote }}{{- end }}
          }
      ---
      apiVersion: batch/v1
      kind: Job
      metadata:
        name: {{ $name }}-register
        labels:
          {{- include "durable-clickhouse-sink.labels" $ | nindent 4 }}
          app.kubernetes.io/component: connector-manager
        annotations:
          helm.sh/hook: post-install,post-upgrade
          helm.sh/hook-weight: "5"
          helm.sh/hook-delete-policy: before-hook-creation
      spec:
        backoffLimit: 6
        activeDeadlineSeconds: {{ $.Values.timeouts.hookJobSeconds }}
        template:
          metadata:
            labels:
              {{- include "durable-clickhouse-sink.labels" $ | nindent 8 }}
              app.kubernetes.io/component: connector-manager
          spec:
            restartPolicy: OnFailure
            automountServiceAccountToken: false
            securityContext: {runAsNonRoot: true, runAsUser: 65532, runAsGroup: 65532}
            initContainers:
              - name: validate-clickhouse-targets
                image: "{{ $.Values.connect.image.repository }}:{{ $.Values.connect.image.tag }}"
                imagePullPolicy: {{ $.Values.connect.image.pullPolicy }}
                command: ["/bin/durable-clickhouse-backup", "validate-targets", "/etc/durable-clickhouse/target.json"]
                volumeMounts:
                  - {name: clickhouse-credentials, mountPath: /etc/clickhouse, readOnly: true}
                  - {name: runtime-config, mountPath: /etc/durable-clickhouse, readOnly: true}
            containers:
              - name: register
                image: "{{ $.Values.connect.image.repository }}:{{ $.Values.connect.image.tag }}"
                imagePullPolicy: {{ $.Values.connect.image.pullPolicy }}
                command: ["/bin/bash", "-euc"]
                args:
                  - |
                    endpoint="http://{{ include "durable-clickhouse-sink.fullname" $ }}-connect:8083"
                    curl_options=(--connect-timeout "$CONNECT_TIMEOUT_SECONDS" --max-time "$REQUEST_TIMEOUT_SECONDS")
                    until curl "''${curl_options[@]}" --silent --fail "$endpoint/connector-plugins" >/dev/null; do sleep "$POLL_SECONDS"; done
                    curl "''${curl_options[@]}" --fail-with-body --request PUT --header 'Content-Type: application/json' --data-binary @/connector/config.json "$endpoint/connectors/{{ $name }}/config"
                env:
                  - {name: CONNECT_TIMEOUT_SECONDS, value: {{ $.Values.timeouts.connectConnectSeconds | quote }}}
                  - {name: REQUEST_TIMEOUT_SECONDS, value: {{ $.Values.timeouts.connectRequestSeconds | quote }}}
                  - {name: POLL_SECONDS, value: {{ $.Values.timeouts.connectPollSeconds | quote }}}
                volumeMounts:
                  - {name: connector, mountPath: /connector, readOnly: true}
                  - {name: tmp, mountPath: /tmp}
            volumes:
              - name: connector
                configMap:
                  name: {{ $name }}-connector
              - name: clickhouse-credentials
                secret:
                  secretName: {{ $.Values.clickhouse.credentialsSecret.name }}
                  items:
                    - key: {{ $.Values.clickhouse.credentialsSecret.propertiesKey }}
                      path: clickhouse.properties
              - name: runtime-config
                configMap:
                  name: {{ include "durable-clickhouse-sink.fullname" $ }}-runtime
              - {name: tmp, emptyDir: {}}
      ---
      {{- if $.Values.connect.deleteConnectorsOnUninstall }}
      apiVersion: batch/v1
      kind: Job
      metadata:
        name: {{ $name }}-unregister
        labels:
          {{- include "durable-clickhouse-sink.labels" $ | nindent 4 }}
          app.kubernetes.io/component: connector-manager
        annotations:
          helm.sh/hook: pre-delete
          helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded
      spec:
        activeDeadlineSeconds: {{ $.Values.timeouts.hookJobSeconds }}
        template:
          metadata:
            labels:
              {{- include "durable-clickhouse-sink.labels" $ | nindent 8 }}
              app.kubernetes.io/component: connector-manager
          spec:
            restartPolicy: OnFailure
            automountServiceAccountToken: false
            securityContext: {runAsNonRoot: true, runAsUser: 65532, runAsGroup: 65532}
            containers:
              - name: unregister
                image: "{{ $.Values.connect.image.repository }}:{{ $.Values.connect.image.tag }}"
                imagePullPolicy: {{ $.Values.connect.image.pullPolicy }}
                command: ["/bin/bash", "-euc"]
                args:
                  - curl --connect-timeout "$CONNECT_TIMEOUT_SECONDS" --max-time "$REQUEST_TIMEOUT_SECONDS" --silent --show-error --request DELETE "http://{{ include "durable-clickhouse-sink.fullname" $ }}-connect:8083/connectors/{{ $name }}" || true
                env:
                  - {name: CONNECT_TIMEOUT_SECONDS, value: {{ $.Values.timeouts.connectConnectSeconds | quote }}}
                  - {name: REQUEST_TIMEOUT_SECONDS, value: {{ $.Values.timeouts.connectRequestSeconds | quote }}}
                volumeMounts:
                  - {name: tmp, mountPath: /tmp}
            volumes:
              - {name: tmp, emptyDir: {}}
      ---
      {{- end }}
      {{- end }}
    '';

    "templates/backup.yaml" = ''
      {{- if .Values.backup.enabled }}
      apiVersion: batch/v1
      kind: CronJob
      metadata:
        name: {{ include "durable-clickhouse-sink.fullname" . }}-backup
        labels:
          {{- include "durable-clickhouse-sink.labels" . | nindent 4 }}
          app.kubernetes.io/component: backup
      spec:
        schedule: {{ .Values.backup.schedule | quote }}
        suspend: {{ .Values.backup.suspend }}
        concurrencyPolicy: Forbid
        successfulJobsHistoryLimit: {{ .Values.backup.successfulJobsHistoryLimit }}
        failedJobsHistoryLimit: {{ .Values.backup.failedJobsHistoryLimit }}
        jobTemplate:
          spec:
            backoffLimit: 1
            activeDeadlineSeconds: {{ .Values.backup.activeDeadlineSeconds }}
            template:
              metadata:
                labels:
                  {{- include "durable-clickhouse-sink.labels" . | nindent 12 }}
                  app.kubernetes.io/component: backup
              spec:
                restartPolicy: Never
                automountServiceAccountToken: false
                terminationGracePeriodSeconds: {{ .Values.backup.terminationGracePeriodSeconds }}
                securityContext: {runAsNonRoot: true, runAsUser: 65532, runAsGroup: 65532}
                containers:
                  - name: backup
                    image: "{{ .Values.connect.image.repository }}:{{ .Values.connect.image.tag }}"
                    imagePullPolicy: {{ .Values.connect.image.pullPolicy }}
                    command: ["/bin/durable-clickhouse-backup", "backup", "/etc/durable-clickhouse/backup.json"]
                    env:
                      - name: BACKUP_RUN_ID
                        valueFrom:
                          fieldRef:
                            fieldPath: metadata.uid
                      - name: CLICKHOUSE_USERNAME
                        valueFrom:
                          secretKeyRef:
                            name: {{ .Values.backup.credentialsSecret.name }}
                            key: {{ .Values.backup.credentialsSecret.usernameKey }}
                      - name: CLICKHOUSE_PASSWORD
                        valueFrom:
                          secretKeyRef:
                            name: {{ .Values.backup.credentialsSecret.name }}
                            key: {{ .Values.backup.credentialsSecret.passwordKey }}
                    volumeMounts:
                      - {name: tmp, mountPath: /tmp}
                      - {name: runtime-config, mountPath: /etc/durable-clickhouse, readOnly: true}
                      {{- if .Values.kafka.existingSecret }}
                      - {name: kafka-backup-client, mountPath: /etc/kafka-backup, readOnly: true}
                      {{- end }}
                volumes:
                  - {name: tmp, emptyDir: {}}
                  - name: runtime-config
                    configMap:
                      name: {{ include "durable-clickhouse-sink.fullname" . }}-runtime
                  {{- if .Values.kafka.existingSecret }}
                  - name: kafka-backup-client
                    secret:
                      secretName: {{ .Values.kafka.existingSecret }}
                      items:
                        - key: {{ .Values.backup.kafkaPropertiesKey }}
                          path: client.properties
                  {{- end }}
      {{- end }}
    '';

    "templates/recovery-controller.yaml" = ''
      apiVersion: rbac.authorization.k8s.io/v1
      kind: Role
      metadata:
        name: {{ include "durable-clickhouse-sink.fullname" . }}-recovery
        labels:
          {{- include "durable-clickhouse-sink.labels" . | nindent 4 }}
      rules:
        - apiGroups: ["chbackup.dialo.ai"]
          resources: ["tablerecoveries"]
          verbs: ["get", "list", "watch"]
        - apiGroups: ["chbackup.dialo.ai"]
          resources: ["tablerecoveries/status"]
          verbs: ["get", "patch", "update"]
      ---
      apiVersion: rbac.authorization.k8s.io/v1
      kind: RoleBinding
      metadata:
        name: {{ include "durable-clickhouse-sink.fullname" . }}-recovery
        labels:
          {{- include "durable-clickhouse-sink.labels" . | nindent 4 }}
      roleRef:
        apiGroup: rbac.authorization.k8s.io
        kind: Role
        name: {{ include "durable-clickhouse-sink.fullname" . }}-recovery
      subjects:
        - kind: ServiceAccount
          name: {{ include "durable-clickhouse-sink.fullname" . }}
          namespace: {{ .Release.Namespace }}
      ---
      apiVersion: apps/v1
      kind: Deployment
      metadata:
        name: {{ include "durable-clickhouse-sink.fullname" . }}-recovery
        labels:
          {{- include "durable-clickhouse-sink.labels" . | nindent 4 }}
          app.kubernetes.io/component: recovery-controller
      spec:
        replicas: 1
        strategy: {type: Recreate}
        selector:
          matchLabels:
            app.kubernetes.io/name: {{ include "durable-clickhouse-sink.name" . }}
            app.kubernetes.io/instance: {{ .Release.Name }}
            app.kubernetes.io/component: recovery-controller
        template:
          metadata:
            labels:
              {{- include "durable-clickhouse-sink.labels" . | nindent 8 }}
              app.kubernetes.io/component: recovery-controller
          spec:
            serviceAccountName: {{ include "durable-clickhouse-sink.fullname" . }}
            automountServiceAccountToken: true
            securityContext: {runAsNonRoot: true, runAsUser: 65532, runAsGroup: 65532}
            {{- with .Values.imagePullSecrets }}
            imagePullSecrets:
              {{- toYaml . | nindent 14 }}
            {{- end }}
            containers:
              - name: controller
                image: "{{ .Values.connect.image.repository }}:{{ .Values.connect.image.tag }}"
                imagePullPolicy: {{ .Values.connect.image.pullPolicy }}
                command: ["/bin/durable-clickhouse-backup", "recovery-controller", "/etc/durable-clickhouse/recovery.json"]
                resources:
                  {{- toYaml .Values.recovery.resources | nindent 18 }}
                env:
                  - name: POD_NAMESPACE
                    valueFrom:
                      fieldRef: {fieldPath: metadata.namespace}
                volumeMounts:
                  - {name: tmp, mountPath: /tmp}
                  - {name: runtime-config, mountPath: /etc/durable-clickhouse, readOnly: true}
                  - {name: clickhouse-recovery-credentials, mountPath: /etc/clickhouse-recovery, readOnly: true}
                  {{- if .Values.kafka.existingSecret }}
                  - {name: kafka-recovery-client, mountPath: /etc/kafka-recovery, readOnly: true}
                  {{- end }}
            volumes:
              - {name: tmp, emptyDir: {}}
              - name: runtime-config
                configMap:
                  name: {{ include "durable-clickhouse-sink.fullname" . }}-runtime
              - name: clickhouse-recovery-credentials
                secret:
                  secretName: {{ .Values.recovery.credentialsSecret.name }}
                  items:
                    - key: {{ .Values.recovery.credentialsSecret.propertiesKey }}
                      path: clickhouse.properties
              {{- if .Values.kafka.existingSecret }}
              - name: kafka-recovery-client
                secret:
                  secretName: {{ .Values.kafka.existingSecret }}
                  items:
                    - key: {{ .Values.backup.kafkaPropertiesKey }}
                      path: client.properties
              {{- end }}
    '';

    "templates/NOTES.txt" = ''
      Durable ClickHouse Sink is configured for {{ len .Values.pipelines }} pipeline(s).

      The chart expects:
      - Kafka at {{ .Values.kafka.bootstrapServers }}
      - ClickHouse at {{ .Values.clickhouse.host }}:{{ .Values.clickhouse.port }}
      - ClickHouse credentials in Secret {{ .Values.clickhouse.credentialsSecret.name }}

      Target ClickHouse tables must exist before connector registration completes.
    '';
  };
in
symlinkJoin {
  name = "durable-clickhouse-sink-chart-source";
  paths = (lib.mapAttrsToList writeTextDir files) ++ [recoveryCrd];
}
