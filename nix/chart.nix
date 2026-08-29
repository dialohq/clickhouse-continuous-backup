{lib, symlinkJoin, writeTextDir}:
let
  files = {
    "Chart.yaml" = ''
      apiVersion: v2
      name: durable-clickhouse-sink
      description: Durable, deduplicated Kafka-compatible ingestion into ClickHouse
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

      deduplicator:
        enabled: true
        image:
          repository: ghcr.io/dialohq/durable-clickhouse-deduplicator
          tag: 0.1.0
          pullPolicy: IfNotPresent
        podAnnotations: {}
        replicas: 2
        standbyReplicas: 1
        commitIntervalMs: 100
        persistence:
          enabled: true
          size: 10Gi
          storageClass: ""
        resources:
          requests:
            cpu: 100m
            memory: 256Mi
          limits:
            memory: 1Gi

      connect:
        image:
          repository: ghcr.io/dialohq/durable-clickhouse-connect
          tag: 0.1.0
          pullPolicy: IfNotPresent
        podAnnotations: {}
        replicas: 2
        tasksMax: 1
        deleteConnectorsOnUninstall: true
        resources:
          requests:
            cpu: 250m
            memory: 512Mi
          limits:
            memory: 2Gi

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
        credentialsSecret:
          name: ""
          httpConfigKey: clickhouse-curl.config
        successfulJobsHistoryLimit: 3
        failedJobsHistoryLimit: 3

      pipelines: []
      # - name: events
      #   rawTopic: events.raw
      #   canonicalTopic: events.canonical
      #   conflictTopic: events.conflicts
      #   table: events
      #   database: default
      #   rawRetentionMs: 1209600000
      #   deduplicationRetentionMs: 2592000000
      #   canonicalRetentionMs: 7776000000
      #   conflictRetentionMs: 7776000000
      #   partitions: 6
      #   valueConverter: org.apache.kafka.connect.json.JsonConverter
      #   valueConverterSchemasEnable: false
      #   connectorConfig: {}
    '';

    "values.schema.json" = builtins.toJSON {
      "$schema" = "https://json-schema.org/draft/2020-12/schema";
      type = "object";
      required = ["kafka" "clickhouse" "pipelines"];
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
            required = ["name" "rawTopic" "canonicalTopic" "conflictTopic" "table"];
            properties = {
              name = {type = "string"; pattern = "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"; maxLength = 40;};
              rawTopic = {type = "string"; minLength = 1;};
              canonicalTopic = {type = "string"; minLength = 1;};
              conflictTopic = {type = "string"; minLength = 1;};
              table = {type = "string"; pattern = "^[A-Za-z_][A-Za-z0-9_]*$";};
              database = {type = "string"; pattern = "^[A-Za-z_][A-Za-z0-9_]*$";};
              rawRetentionMs = {type = "integer"; minimum = 60000; default = 1209600000;};
              deduplicationRetentionMs = {type = "integer"; minimum = 60000; default = 2592000000;};
              canonicalRetentionMs = {type = "integer"; minimum = 60000; default = 7776000000;};
              conflictRetentionMs = {type = "integer"; minimum = 60000; default = 7776000000;};
              partitions = {type = "integer"; minimum = 1;};
              valueConverter = {type = "string"; minLength = 1;};
              valueConverterSchemasEnable = {type = "boolean";};
              connectorConfig = {type = "object"; additionalProperties = true;};
            };
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
            credentialsSecret = {
              type = "object";
              properties = {
                name = {type = "string";};
                httpConfigKey = {type = "string"; minLength = 1;};
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
      {{- $databases := dict .Values.clickhouse.database true -}}
      {{- range $pipeline := .Values.pipelines -}}
      {{- $_ := set $databases (default $.Values.clickhouse.database $pipeline.database) true -}}
      {{- end -}}
      {{- $first := true -}}
      {{- range $database, $_ := $databases -}}
      {{- if not $first }}, {{ end -}}
      DATABASE {{ $database }}
      {{- $first = false -}}
      {{- end -}}
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
      {{- range $topic := list $pipeline.rawTopic $pipeline.canonicalTopic $pipeline.conflictTopic }}
      {{- if hasKey $topics $topic }}
      {{- fail (printf "topic names must be unique across pipelines: %s" $topic) }}
      {{- end }}
      {{- $_ := set $topics $topic true }}
      {{- end }}
      {{- range $key, $_ := default dict $pipeline.connectorConfig }}
      {{- if has $key $reserved }}
      {{- fail (printf "pipeline %s cannot override safety-critical connector setting %s" $pipeline.name $key) }}
      {{- end }}
      {{- end }}
      {{- $rawRetention := int64 (default 1209600000 $pipeline.rawRetentionMs) }}
      {{- $dedupeRetention := int64 (default 2592000000 $pipeline.deduplicationRetentionMs) }}
      {{- if ge $rawRetention $dedupeRetention }}
      {{- fail (printf "pipeline %s requires rawRetentionMs < deduplicationRetentionMs" $pipeline.name) }}
      {{- end }}
      {{- end }}
      {{- if and .Values.backup.enabled (not .Values.backup.credentialsSecret.name) }}
      {{- fail "backup.credentialsSecret.name is required when backups are enabled" }}
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
                  periodSeconds: 5
                  failureThreshold: 30
                livenessProbe:
                  httpGet: {path: /, port: http}
                  initialDelaySeconds: 30
                  periodSeconds: 15
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

    "templates/deduplicators.yaml" = ''
      {{- if .Values.deduplicator.enabled }}
      {{- range $pipeline := .Values.pipelines }}
      {{- $name := include "durable-clickhouse-sink.pipelineName" (list $ $pipeline) }}
      apiVersion: v1
      kind: Service
      metadata:
        name: {{ $name }}
        labels:
          {{- include "durable-clickhouse-sink.labels" $ | nindent 4 }}
          app.kubernetes.io/component: deduplicator
          durable-clickhouse.dialo.ai/pipeline: {{ $pipeline.name }}
      spec:
        clusterIP: None
        selector:
          app.kubernetes.io/instance: {{ $.Release.Name }}
          app.kubernetes.io/component: deduplicator
          durable-clickhouse.dialo.ai/pipeline: {{ $pipeline.name }}
        ports:
          - name: health
            port: 8080
      ---
      apiVersion: apps/v1
      kind: StatefulSet
      metadata:
        name: {{ $name }}
        labels:
          {{- include "durable-clickhouse-sink.labels" $ | nindent 4 }}
          app.kubernetes.io/component: deduplicator
          durable-clickhouse.dialo.ai/pipeline: {{ $pipeline.name }}
      spec:
        serviceName: {{ $name }}
        replicas: {{ $.Values.deduplicator.replicas }}
        podManagementPolicy: Parallel
        selector:
          matchLabels:
            app.kubernetes.io/instance: {{ $.Release.Name }}
            app.kubernetes.io/component: deduplicator
            durable-clickhouse.dialo.ai/pipeline: {{ $pipeline.name }}
        template:
          metadata:
            labels:
              {{- include "durable-clickhouse-sink.labels" $ | nindent 8 }}
              app.kubernetes.io/component: deduplicator
              durable-clickhouse.dialo.ai/pipeline: {{ $pipeline.name }}
            {{- with $.Values.deduplicator.podAnnotations }}
            annotations:
              {{- toYaml . | nindent 8 }}
            {{- end }}
          spec:
            serviceAccountName: {{ include "durable-clickhouse-sink.fullname" $ }}
            automountServiceAccountToken: false
            terminationGracePeriodSeconds: 60
            securityContext:
              runAsNonRoot: true
              runAsUser: 65532
              runAsGroup: 65532
              fsGroup: 65532
            {{- with $.Values.imagePullSecrets }}
            imagePullSecrets:
              {{- toYaml . | nindent 8 }}
            {{- end }}
            containers:
              - name: deduplicator
                image: "{{ $.Values.deduplicator.image.repository }}:{{ $.Values.deduplicator.image.tag }}"
                imagePullPolicy: {{ $.Values.deduplicator.image.pullPolicy }}
                env:
                  - {name: APPLICATION_ID, value: {{ printf "%s-%s-v1" (include "durable-clickhouse-sink.fullname" $) $pipeline.name | quote }}}
                  - {name: KAFKA_BOOTSTRAP_SERVERS, value: {{ $.Values.kafka.bootstrapServers | quote }}}
                  - {name: INPUT_TOPIC, value: {{ $pipeline.rawTopic | quote }}}
                  - {name: OUTPUT_TOPIC, value: {{ $pipeline.canonicalTopic | quote }}}
                  - {name: CONFLICT_TOPIC, value: {{ $pipeline.conflictTopic | quote }}}
                  - {name: DEDUPLICATION_RETENTION_MS, value: {{ int64 (default 2592000000 $pipeline.deduplicationRetentionMs) | quote }}}
                  - {name: REPLICATION_FACTOR, value: {{ $.Values.kafka.replicationFactor | quote }}}
                  - {name: STANDBY_REPLICAS, value: {{ $.Values.deduplicator.standbyReplicas | quote }}}
                  - {name: COMMIT_INTERVAL_MS, value: {{ $.Values.deduplicator.commitIntervalMs | quote }}}
                  - {name: STATE_DIR, value: /var/lib/deduplicator/state}
                  {{- if $.Values.kafka.existingSecret }}
                  - {name: KAFKA_PROPERTIES_FILE, value: /etc/kafka/client.properties}
                  {{- end }}
                ports:
                  - {name: health, containerPort: 8080}
                readinessProbe:
                  httpGet: {path: /ready, port: health}
                  periodSeconds: 5
                  failureThreshold: 60
                livenessProbe:
                  httpGet: {path: /health, port: health}
                  initialDelaySeconds: 30
                  periodSeconds: 15
                resources:
                  {{- toYaml $.Values.deduplicator.resources | nindent 18 }}
                volumeMounts:
                  - {name: state, mountPath: /var/lib/deduplicator}
                  {{- if $.Values.kafka.existingSecret }}
                  - {name: kafka-client, mountPath: /etc/kafka, readOnly: true}
                  {{- end }}
            {{- if not $.Values.deduplicator.persistence.enabled }}
            volumes:
              - {name: state, emptyDir: {}}
              {{- include "durable-clickhouse-sink.kafkaSecretVolume" $ | nindent 6 }}
            {{- else if $.Values.kafka.existingSecret }}
            volumes:
              {{- include "durable-clickhouse-sink.kafkaSecretVolume" $ | nindent 6 }}
            {{- end }}
        {{- if $.Values.deduplicator.persistence.enabled }}
        volumeClaimTemplates:
          - metadata:
              name: state
            spec:
              accessModes: [ReadWriteOnce]
              {{- with $.Values.deduplicator.persistence.storageClass }}
              storageClassName: {{ . }}
              {{- end }}
              resources:
                requests:
                  storage: {{ $.Values.deduplicator.persistence.size }}
        {{- end }}
      ---
      {{- end }}
      {{- end }}
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
                    configure() {
                      /bin/kafka-configs.sh --bootstrap-server "$BOOTSTRAP_SERVERS" "''${config[@]}" --entity-type topics --entity-name "$1" --alter --add-config "$2"
                    }
                    create "$RAW_TOPIC"
                    create "$CANONICAL_TOPIC"
                    create "$CONFLICT_TOPIC"
                    configure "$RAW_TOPIC" "cleanup.policy=delete,retention.ms=$RAW_RETENTION,message.timestamp.type=LogAppendTime"
                    configure "$CANONICAL_TOPIC" "cleanup.policy=delete,retention.ms=$CANONICAL_RETENTION"
                    configure "$CONFLICT_TOPIC" "cleanup.policy=delete,retention.ms=$CONFLICT_RETENTION"
                env:
                  - {name: BOOTSTRAP_SERVERS, value: {{ $.Values.kafka.bootstrapServers | quote }}}
                  - {name: RAW_TOPIC, value: {{ $pipeline.rawTopic | quote }}}
                  - {name: CANONICAL_TOPIC, value: {{ $pipeline.canonicalTopic | quote }}}
                  - {name: CONFLICT_TOPIC, value: {{ $pipeline.conflictTopic | quote }}}
                  - {name: RAW_RETENTION, value: {{ int64 (default 1209600000 $pipeline.rawRetentionMs) | quote }}}
                  - {name: CANONICAL_RETENTION, value: {{ int64 (default 7776000000 $pipeline.canonicalRetentionMs) | quote }}}
                  - {name: CONFLICT_RETENTION, value: {{ int64 (default 7776000000 $pipeline.conflictRetentionMs) | quote }}}
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
            "topics": {{ $pipeline.canonicalTopic | quote }},
            "topic2TableMap": {{ printf "%s=%s" $pipeline.canonicalTopic $pipeline.table | quote }},
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
            "clickhouseSettings": "async_insert=1,wait_for_async_insert=1"{{- range $key, $value := default dict $pipeline.connectorConfig }},
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
              - name: register
                image: "{{ $.Values.connect.image.repository }}:{{ $.Values.connect.image.tag }}"
                imagePullPolicy: {{ $.Values.connect.image.pullPolicy }}
                command: ["/bin/bash", "-euc"]
                args:
                  - |
                    endpoint="http://{{ include "durable-clickhouse-sink.fullname" $ }}-connect:8083"
                    until curl --silent --fail "$endpoint/connector-plugins" >/dev/null; do sleep 2; done
                    curl --fail-with-body --request PUT --header 'Content-Type: application/json' --data-binary @/connector/config.json "$endpoint/connectors/{{ $name }}/config"
                volumeMounts:
                  - {name: connector, mountPath: /connector, readOnly: true}
                  - {name: tmp, mountPath: /tmp}
            volumes:
              - name: connector
                configMap:
                  name: {{ $name }}-connector
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
                  - curl --silent --show-error --request DELETE "http://{{ include "durable-clickhouse-sink.fullname" $ }}-connect:8083/connectors/{{ $name }}" || true
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
            template:
              metadata:
                labels:
                  {{- include "durable-clickhouse-sink.labels" . | nindent 12 }}
                  app.kubernetes.io/component: backup
              spec:
                restartPolicy: Never
                automountServiceAccountToken: false
                securityContext: {runAsNonRoot: true, runAsUser: 65532, runAsGroup: 65532}
                containers:
                  - name: backup
                    image: "{{ .Values.connect.image.repository }}:{{ .Values.connect.image.tag }}"
                    imagePullPolicy: {{ .Values.connect.image.pullPolicy }}
                    command: ["/bin/bash", "-euc"]
                    args:
                      - |
                        connectors=(
                          {{- range $pipeline := .Values.pipelines }}
                          {{ include "durable-clickhouse-sink.pipelineName" (list $ $pipeline) | quote }}
                          {{- end }}
                        )
                        resume() {
                          for connector in "''${connectors[@]}"; do
                            curl --fail --silent --show-error --request PUT "$CONNECT_URL/connectors/$connector/resume" || true
                          done
                        }
                        for connector in "''${connectors[@]}"; do
                          state=$(curl --fail --silent "$CONNECT_URL/connectors/$connector/status")
                          if ! jq --exit-status '(.tasks | length > 0) and ([.connector.state, (.tasks[].state)] | all(. == "RUNNING"))' <<<"$state" >/dev/null; then
                            echo "connector must be fully running before backup: $connector" >&2
                            exit 1
                          fi
                        done
                        trap resume EXIT
                        for connector in "''${connectors[@]}"; do
                          curl --fail-with-body --request PUT "$CONNECT_URL/connectors/$connector/pause"
                        done
                        for connector in "''${connectors[@]}"; do
                          for attempt in $(seq 1 120); do
                            if curl --fail --silent "$CONNECT_URL/connectors/$connector/status" | jq --exit-status '[.connector.state, (.tasks[].state)] | all(. == "PAUSED")' >/dev/null; then
                              break
                            fi
                            if [[ "$attempt" == 120 ]]; then
                              echo "connector did not pause: $connector" >&2
                              exit 1
                            fi
                            sleep 1
                          done
                        done
                        stamp=$(date -u +%Y%m%dT%H%M%SZ)
                        destination="S3({{ .Values.backup.namedCollection }}, '{{ trimSuffix "/" .Values.backup.pathPrefix }}/$stamp.{{ .Values.backup.archiveExtension }}')"
                        query="BACKUP {{ include "durable-clickhouse-sink.backupObjects" . }} TO $destination"
                        result=$(curl --silent --show-error --config /etc/clickhouse/curl.config --fail-with-body --data-binary "$query" "$CLICKHOUSE_URL")
                        backup_id="''${result%%$'\t'*}"
                        if [[ ! $backup_id =~ ^[0-9a-fA-F-]{36}$ ]] || [[ $result != *$'\tBACKUP_CREATED' ]]; then
                          echo "unexpected ClickHouse BACKUP result: $result" >&2
                          exit 1
                        fi
                        curl --silent --show-error --config /etc/clickhouse/curl.config --fail-with-body --data-binary "SELECT name, status, num_files, uncompressed_size, compressed_size FROM system.backups WHERE id = '$backup_id' FORMAT JSONEachRow" "$CLICKHOUSE_URL"
                    env:
                      - name: CONNECT_URL
                        value: http://{{ include "durable-clickhouse-sink.fullname" . }}-connect:8083
                      - name: CLICKHOUSE_URL
                        value: {{ printf "%s://%s:%v/" (ternary "https" "http" .Values.clickhouse.secure) .Values.clickhouse.host .Values.clickhouse.port | quote }}
                    volumeMounts:
                      - {name: clickhouse-credentials, mountPath: /etc/clickhouse, readOnly: true}
                      - {name: tmp, mountPath: /tmp}
                volumes:
                  - name: clickhouse-credentials
                    secret:
                      secretName: {{ .Values.backup.credentialsSecret.name }}
                      items:
                        - key: {{ .Values.backup.credentialsSecret.httpConfigKey }}
                          path: curl.config
                  - {name: tmp, emptyDir: {}}
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
