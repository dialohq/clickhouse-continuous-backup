{specialArgs, ...}: let
  namespace = "durable-sink-e2e";
  labels = app: {inherit app;};
in {
  nixidy.target = {
    repository = "local";
    branch = "local";
    rootPath = "e2e";
  };

  applications.e2e = {
    inherit namespace;
    resources = {
      namespaces.${namespace} = {};

      configMaps = {
        clickhouse.data."e2e.xml" = builtins.readFile ../e2e/clickhouse.xml;
        schema.data."schema.sql" = builtins.readFile ../e2e/schema.sql;
      };

      secrets.clickhouse-credentials.stringData = {
        "clickhouse.properties" = ''
          username=default
          password=
        '';
        "clickhouse-curl.config" = ''
          user = "default:"
        '';
      };

      services = {
        redpanda.spec = {
          selector = labels "redpanda";
          ports = [{name = "kafka"; port = 9092; targetPort = 9092;}];
        };
        clickhouse.spec = {
          selector = labels "clickhouse";
          ports = [
            {name = "http"; port = 8123; targetPort = 8123;}
            {name = "tcp"; port = 9000; targetPort = 9000;}
          ];
        };
        clickhouse-restore.spec = {
          selector = labels "clickhouse-restore";
          ports = [
            {name = "http"; port = 8123; targetPort = 8123;}
            {name = "tcp"; port = 9000; targetPort = 9000;}
          ];
        };
        minio.spec = {
          selector = labels "minio";
          ports = [{name = "s3"; port = 9000; targetPort = 9000;}];
        };
      };

      statefulSets = {
        redpanda.spec = {
          serviceName = "redpanda";
          replicas = 1;
          selector.matchLabels = labels "redpanda";
          template = {
            metadata.labels = labels "redpanda";
            spec = {
              containers = [{
                name = "redpanda";
                image = "docker.redpanda.com/redpandadata/redpanda:v26.2.2";
                args = [
                  "redpanda" "start"
                  "--kafka-addr" "internal://0.0.0.0:9092"
                  "--advertise-kafka-addr" "internal://redpanda.${namespace}.svc.cluster.local:9092"
                  "--rpc-addr" "0.0.0.0:33145"
                  "--advertise-rpc-addr" "redpanda.${namespace}.svc.cluster.local:33145"
                  "--smp" "1" "--memory" "1G" "--reserve-memory" "0M"
                  "--overprovisioned" "--lock-memory=false" "--unsafe-bypass-fsync=true"
                ];
                ports = [{name = "kafka"; containerPort = 9092;} {name = "admin"; containerPort = 9644;}];
                startupProbe = {httpGet = {path = "/v1/status/ready"; port = "admin";}; periodSeconds = 2; failureThreshold = 120;};
                readinessProbe = {httpGet = {path = "/v1/status/ready"; port = "admin";}; periodSeconds = 3;};
                resources.requests = {cpu = "250m"; memory = "1Gi";};
                volumeMounts = [{name = "data"; mountPath = "/var/lib/redpanda/data";}];
              }];
              volumes = [{name = "data"; emptyDir = {};}];
            };
          };
        };

        clickhouse.spec = {
          serviceName = "clickhouse";
          replicas = 1;
          selector.matchLabels = labels "clickhouse";
          template = {
            metadata.labels = labels "clickhouse";
            spec = {
              containers = [{
                name = "clickhouse";
                image = "clickhouse/clickhouse-server:26.7";
                env = [{name = "CLICKHOUSE_SKIP_USER_SETUP"; value = "1";}];
                ports = [{name = "http"; containerPort = 8123;} {name = "tcp"; containerPort = 9000;}];
                startupProbe = {httpGet = {path = "/ping"; port = "http";}; periodSeconds = 2; failureThreshold = 120;};
                readinessProbe = {httpGet = {path = "/ping"; port = "http";}; periodSeconds = 3;};
                resources.requests = {cpu = "500m"; memory = "1Gi";};
                volumeMounts = [
                  {name = "config"; mountPath = "/etc/clickhouse-server/config.d/e2e.xml"; subPath = "e2e.xml";}
                  {name = "data"; mountPath = "/var/lib/clickhouse";}
                ];
              }];
              volumes = [
                {name = "config"; configMap.name = "clickhouse";}
                {name = "data"; emptyDir = {};}
              ];
            };
          };
        };
        clickhouse-restore.spec = {
          serviceName = "clickhouse-restore";
          replicas = 1;
          selector.matchLabels = labels "clickhouse-restore";
          template = {
            metadata.labels = labels "clickhouse-restore";
            spec = {
              containers = [{
                name = "clickhouse";
                image = "clickhouse/clickhouse-server:26.7";
                env = [{name = "CLICKHOUSE_SKIP_USER_SETUP"; value = "1";}];
                ports = [{name = "http"; containerPort = 8123;} {name = "tcp"; containerPort = 9000;}];
                startupProbe = {httpGet = {path = "/ping"; port = "http";}; periodSeconds = 2; failureThreshold = 120;};
                readinessProbe = {httpGet = {path = "/ping"; port = "http";}; periodSeconds = 3;};
                resources.requests = {cpu = "500m"; memory = "1Gi";};
                volumeMounts = [
                  {name = "config"; mountPath = "/etc/clickhouse-server/config.d/e2e.xml"; subPath = "e2e.xml";}
                  {name = "data"; mountPath = "/var/lib/clickhouse";}
                ];
              }];
              volumes = [
                {name = "config"; configMap.name = "clickhouse";}
                {name = "data"; emptyDir = {};}
              ];
            };
          };
        };
      };

      deployments.minio.spec = {
        replicas = 1;
        selector.matchLabels = labels "minio";
        template = {
          metadata.labels = labels "minio";
          spec = {
            containers = [{
              name = "minio";
              image = "quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z";
              args = ["server" "/data"];
              env = [
                {name = "MINIO_ROOT_USER"; value = "durable-e2e";}
                {name = "MINIO_ROOT_PASSWORD"; value = "durable-e2e-secret";}
              ];
              ports = [{name = "s3"; containerPort = 9000;}];
              readinessProbe = {httpGet = {path = "/minio/health/ready"; port = "s3";}; periodSeconds = 3;};
              volumeMounts = [{name = "data"; mountPath = "/data";}];
            }];
            volumes = [{name = "data"; emptyDir = {};}];
          };
        };
      };

      jobs = {
        minio-bucket.spec = {
          backoffLimit = 6;
          template.spec = {
            restartPolicy = "OnFailure";
            containers = [{
              name = "create";
              image = "quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z";
              command = ["/bin/sh" "-ec"];
              args = ["until mc alias set e2e http://minio:9000 durable-e2e durable-e2e-secret; do sleep 2; done; mc mb --ignore-existing e2e/backups"];
            }];
          };
        };
        schema.spec = {
          backoffLimit = 6;
          template.spec = {
            restartPolicy = "OnFailure";
            containers = [{
              name = "schema";
              image = "clickhouse/clickhouse-server:26.7";
              command = ["/bin/bash" "-ec"];
              args = ["until clickhouse-client --host clickhouse --query 'SELECT 1'; do sleep 2; done; clickhouse-client --host clickhouse --multiquery < /schema/schema.sql"];
              volumeMounts = [{name = "schema"; mountPath = "/schema"; readOnly = true;}];
            }];
            volumes = [{name = "schema"; configMap.name = "schema";}];
          };
        };
      };
    };
  };
}
