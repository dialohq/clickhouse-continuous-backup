{lib, symlinkJoin, writeTextDir}:
let
  files = {
    "Chart.yaml" = ''
      apiVersion: v2
      name: durable-clickhouse-sink
      description: Durable Kafka-compatible ingestion into ClickHouse with backup and recovery
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
        pauseTimeoutSeconds: 120
        activeDeadlineSeconds: 21600
        terminationGracePeriodSeconds: 300
        recoveryTopic: ""
        kafkaPropertiesKey: librdkafka.properties
        credentialsSecret:
          name: ""
          usernameKey: username
          passwordKey: password
        successfulJobsHistoryLimit: 3
        failedJobsHistoryLimit: 3

      pipelines: []
      # - name: events
      #   topic: events.canonical
      #   table: events
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
      required = ["kafka" "clickhouse" "timeouts" "pipelines"];
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
            pauseTimeoutSeconds = {type = "integer"; minimum = 1; maximum = 3600;};
            activeDeadlineSeconds = {type = "integer"; minimum = 1; maximum = 604800;};
            terminationGracePeriodSeconds = {type = "integer"; minimum = 1; maximum = 3600;};
            recoveryTopic = {type = "string"; pattern = "^$|^[A-Za-z0-9._-]+$"; maxLength = 249;};
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

      {{- define "durable-clickhouse-sink.backupObjects" -}}
      {{- $tables := dict -}}
      {{- range $pipeline := .Values.pipelines -}}
      {{- $database := default $.Values.clickhouse.database $pipeline.database -}}
      {{- $_ := set $tables (printf "%s.%s" $database $pipeline.table) (dict "database" $database "table" $pipeline.table) -}}
      {{- end -}}
      {{- $first := true -}}
      {{- range $key, $target := $tables -}}
      {{- if not $first }}, {{ end -}}
      TABLE {{ $target.database }}.{{ $target.table }}
      {{- $first = false -}}
      {{- end -}}
      {{- end }}

      {{- define "durable-clickhouse-sink.backupStateObjects" -}}
      {{- $first := true -}}
      {{- range $pipeline := .Values.pipelines -}}
      {{- if not $first }}, {{ end -}}
      TABLE {{ default $.Values.clickhouse.database $pipeline.database }}.{{ include "durable-clickhouse-sink.stateTable" (list $ $pipeline) }}
      {{- $first = false -}}
      {{- end -}}
      {{- end }}

      {{- define "durable-clickhouse-sink.backupPipelines" -}}
      {{- $pipelines := list -}}
      {{- range $pipeline := .Values.pipelines -}}
      {{- $pipelines = append $pipelines (dict
        "connector" (include "durable-clickhouse-sink.pipelineName" (list $ $pipeline))
        "database" (default $.Values.clickhouse.database $pipeline.database)
        "state_table" (include "durable-clickhouse-sink.stateTable" (list $ $pipeline))
        "table" $pipeline.table
        "topic" $pipeline.topic
        "partitions" (default $.Values.topics.partitions $pipeline.partitions)) -}}
      {{- end -}}
      {{- toJson $pipelines -}}
      {{- end }}

      {{- define "durable-clickhouse-sink.recoveryTopic" -}}
      {{- default (printf "%s.recovery-points" (include "durable-clickhouse-sink.fullname" .)) .Values.backup.recoveryTopic -}}
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
    '';

    "templates/validate.yaml" = ''
      {{- $names := dict -}}
      {{- $topics := dict -}}
      {{- $reserved := list "connector.class" "tasks.max" "topics" "topic2TableMap" "hostname" "port" "ssl" "database" "username" "password" "exactlyOnce" "errors.tolerance" "bufferCount" "consumer.override.isolation.level" "zkPath" "zkDatabase" -}}
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
        name: {{ include "durable-clickhouse-sink.fullname" . }}-recovery-topic
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
                    /bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVERS" "''${config[@]}" --create --if-not-exists --topic "$RECOVERY_TOPIC" --partitions 1 --replication-factor "$REPLICATION_FACTOR"
                    description=$(/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVERS" "''${config[@]}" --describe --topic "$RECOVERY_TOPIC")
                    [[ "$description" =~ PartitionCount:[[:space:]]+1([[:space:]]|$) ]] || { echo "$RECOVERY_TOPIC does not have 1 partition: $description" >&2; exit 1; }
                    /bin/kafka-configs.sh --bootstrap-server "$BOOTSTRAP_SERVERS" "''${config[@]}" --entity-type topics --entity-name "$RECOVERY_TOPIC" --alter --add-config cleanup.policy=compact,retention.ms=-1,retention.bytes=-1
                env:
                  - {name: BOOTSTRAP_SERVERS, value: {{ .Values.kafka.bootstrapServers | quote }}}
                  - {name: RECOVERY_TOPIC, value: {{ include "durable-clickhouse-sink.recoveryTopic" . | quote }}}
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
                command: ["/bin/durable-clickhouse-recovery", "validate-targets"]
                env:
                  - name: CLICKHOUSE_URL
                    value: {{ printf "%s://%s:%v/" (ternary "https" "http" $.Values.clickhouse.secure) $.Values.clickhouse.host $.Values.clickhouse.port | quote }}
                  - name: CLICKHOUSE_PROPERTIES_FILE
                    value: /etc/clickhouse/clickhouse.properties
                  - name: BACKUP_PIPELINES
                    value: {{ include "durable-clickhouse-sink.backupPipelines" $ | quote }}
                  - name: RUNTIME_TIMEOUTS
                    value: {{ include "durable-clickhouse-sink.runtimeTimeouts" $ | quote }}
                volumeMounts:
                  - {name: clickhouse-credentials, mountPath: /etc/clickhouse, readOnly: true}
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
                    command: ["/bin/durable-clickhouse-recovery", "backup"]
                    env:
                      - name: CONNECT_URL
                        value: http://{{ include "durable-clickhouse-sink.fullname" . }}-connect:8083
                      - name: CLICKHOUSE_URL
                        value: {{ printf "%s://%s:%v/" (ternary "https" "http" .Values.clickhouse.secure) .Values.clickhouse.host .Values.clickhouse.port | quote }}
                      - name: BACKUP_OBJECTS
                        value: {{ include "durable-clickhouse-sink.backupObjects" . | trim | quote }}
                      - name: BACKUP_STATE_OBJECTS
                        value: {{ include "durable-clickhouse-sink.backupStateObjects" . | trim | quote }}
                      - name: BACKUP_PIPELINES
                        value: {{ include "durable-clickhouse-sink.backupPipelines" . | quote }}
                      - {name: BACKUP_NAMED_COLLECTION, value: {{ .Values.backup.namedCollection | quote }}}
                      - {name: BACKUP_PATH_PREFIX, value: {{ trimSuffix "/" .Values.backup.pathPrefix | quote }}}
                      - {name: BACKUP_ARCHIVE_EXTENSION, value: {{ .Values.backup.archiveExtension | quote }}}
                      - {name: MAX_INCREMENTALS_PER_FULL, value: {{ .Values.backup.maxIncrementalsPerFull | quote }}}
                      - {name: PAUSE_TIMEOUT_SECONDS, value: {{ .Values.backup.pauseTimeoutSeconds | quote }}}
                      - {name: KAFKA_BOOTSTRAP_SERVERS, value: {{ .Values.kafka.bootstrapServers | quote }}}
                      - {name: KAFKA_RECOVERY_TOPIC, value: {{ include "durable-clickhouse-sink.recoveryTopic" . | quote }}}
                      - {name: RUNTIME_TIMEOUTS, value: {{ include "durable-clickhouse-sink.runtimeTimeouts" . | quote }}}
                      {{- if .Values.kafka.existingSecret }}
                      - {name: KAFKA_PROPERTIES_FILE, value: /etc/kafka-recovery/client.properties}
                      {{- end }}
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
                      {{- if .Values.kafka.existingSecret }}
                      - {name: kafka-recovery-client, mountPath: /etc/kafka-recovery, readOnly: true}
                      {{- end }}
                volumes:
                  - {name: tmp, emptyDir: {}}
                  {{- if .Values.kafka.existingSecret }}
                  - name: kafka-recovery-client
                    secret:
                      secretName: {{ .Values.kafka.existingSecret }}
                      items:
                        - key: {{ .Values.backup.kafkaPropertiesKey }}
                          path: client.properties
                  {{- end }}
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
  paths = lib.mapAttrsToList writeTextDir files;
}
