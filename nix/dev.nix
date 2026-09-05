{
  dnvrState,
  pkgs,
  ...
}: let
  supervise = import ./supervise.nix {lib = pkgs.lib;};
  connector = pkgs.callPackage ./clickhouse-kafka-connect.nix {};
  redpanda = pkgs.stdenvNoCC.mkDerivation {
    pname = "redpanda";
    version = "26.2.2";
    src = pkgs.fetchurl {
      url = "https://vectorized-public.s3.us-west-2.amazonaws.com/releases/redpanda/26.2.2/redpanda-26.2.2-amd64.tar.gz";
      hash = "sha256-V1/vu8LLkpY04rgxrLh+ywsjQDsu+LNWo028CgCTSqU=";
    };
    sourceRoot = ".";
    nativeBuildInputs = [pkgs.patchelf];
    installPhase = ''
      runHook preInstall
      mkdir -p "$out"
      cp -a bin lib libexec "$out/"
      substituteInPlace "$out/bin/redpanda" \
        --replace-fail /opt/redpanda "$out"
      patchelf \
        --set-interpreter "$out/lib/ld.so" \
        --set-rpath "$out/lib" \
        "$out/libexec/redpanda"
      runHook postInstall
    '';
  };
in {
  dnvr.shells.default = {
    description = "Durable ClickHouse sink development services";

    packages = [
      pkgs.apacheKafka
      pkgs.clickhouse
      pkgs.curl
      pkgs.jq
      pkgs.kubeconform
      pkgs.kubectl
      pkgs.kubernetes-helm
      pkgs.containerd
      pkgs.rke2
      pkgs.minio
      pkgs.minio-client
      redpanda
      pkgs.cargo
      pkgs.rustc
      pkgs.rust-analyzer
      pkgs.clippy
      pkgs.rustfmt
    ];

    processes.minio = {
      packages = [pkgs.minio pkgs.curl];
      command = pkgs.writeShellApplication {
        name = "durable-sink-minio";
        runtimeInputs = [pkgs.minio pkgs.curl dnvrState];
        text = ''
          set -euo pipefail
          port=$(dnvr-state pick-port)
          console_port=$(dnvr-state pick-port)
          data="$DNVR_ROOT/.dnvr/data/minio"
          mkdir -p "$data"

          ${supervise {
            name = "MinIO";
            start = ''
              minio server "$data" \
                --address "127.0.0.1:$port" \
                --console-address "127.0.0.1:$console_port"
            '';
            readyWhen = ''curl --fail --silent "http://127.0.0.1:$port/minio/health/ready" >/dev/null'';
            onReady = ''
              dnvr-state set host 127.0.0.1
              dnvr-state set port "$port"
              dnvr-state set url "http://127.0.0.1:$port"
              echo "MinIO ready at http://127.0.0.1:$port"
            '';
          }}
        '';
      };
    };

    processes.minio-bucket = {
      packages = [pkgs.minio-client];
      env = {
        MINIO_URL = "dnvr://minio/url";
        MINIO_BUCKET = "backups";
      };
      command = pkgs.writeShellApplication {
        name = "durable-sink-minio-bucket";
        runtimeInputs = [pkgs.minio-client];
        text = ''
          config_dir="$DNVR_RUNTIME_DIR/minio-bucket-mc"
          mkdir -p "$config_dir"
          mc --config-dir "$config_dir" alias set local \
            "$MINIO_URL" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
          mc --config-dir "$config_dir" mb --ignore-existing "local/$MINIO_BUCKET"
          echo "Created MinIO bucket $MINIO_BUCKET"
        '';
      };
    };

    processes.redpanda = {
      packages = [redpanda pkgs.curl];
      command = pkgs.writeShellApplication {
        name = "durable-sink-redpanda";
        runtimeInputs = [redpanda pkgs.curl pkgs.minijinja dnvrState];
        text = ''
          set -euo pipefail
          kafka_port=$(dnvr-state pick-port)
          admin_port=$(dnvr-state pick-port)
          rpc_port=$(dnvr-state pick-port)
          data="$DNVR_ROOT/.dnvr/data/redpanda"
          config="$DNVR_RUNTIME_DIR/redpanda.yaml"
          mkdir -p "$data"

          minijinja-cli --strict --autoescape none \
            --define listen_host=127.0.0.1 \
            --define kafka_port="$kafka_port" \
            --define admin_port="$admin_port" \
            --define rpc_port="$rpc_port" \
            --define data_dir="$data" \
            ${./redpanda.yaml.j2} --output "$config"

          ${supervise {
            name = "Redpanda";
            start = ''
              redpanda --redpanda-cfg "$config" \
                --smp 1 --memory 1G --reserve-memory 0M \
                --overprovisioned --lock-memory=false \
                --unsafe-bypass-fsync=true
            '';
            readyWhen = ''curl --fail --silent "http://127.0.0.1:$admin_port/v1/status/ready" >/dev/null'';
            onReady = ''
              dnvr-state set host 127.0.0.1
              dnvr-state set port "$kafka_port"
              dnvr-state set adminPort "$admin_port"
              dnvr-state set bootstrapServers "127.0.0.1:$kafka_port"
              echo "Redpanda ready at 127.0.0.1:$kafka_port"
            '';
          }}
        '';
      };
    };

    processes.clickhouse = {
      packages = [pkgs.clickhouse pkgs.curl];
      env = {
        CLICKHOUSE_CONFIG = toString ./clickhouse.xml.j2;
        MINIO_URL = "dnvr://minio/url";
      };
      command = pkgs.writeShellApplication {
        name = "durable-sink-clickhouse";
        runtimeInputs = [pkgs.clickhouse pkgs.curl pkgs.minijinja dnvrState];
        text = ''
          set -euo pipefail
          http_port=$(dnvr-state pick-port)
          tcp_port=$(dnvr-state pick-port)
          interserver_http_port=$(dnvr-state pick-port)
          keeper_port=$(dnvr-state pick-port)
          keeper_raft_port=$(dnvr-state pick-port)
          data="$DNVR_ROOT/.dnvr/data/clickhouse"
          config="$DNVR_RUNTIME_DIR/config.xml"
          mkdir -p "$data"

          minijinja-cli --strict --autoescape html \
            --define listen_host=127.0.0.1 \
            --define http_port="$http_port" \
            --define tcp_port="$tcp_port" \
            --define interserver_http_port="$interserver_http_port" \
            --define data_dir="$data" \
            --define keeper_port="$keeper_port" \
            --define keeper_raft_port="$keeper_raft_port" \
            --define minio_url="$MINIO_URL" \
            --define minio_access_key_id="$MINIO_ROOT_USER" \
            --define minio_secret_access_key="$MINIO_ROOT_PASSWORD" \
            "$CLICKHOUSE_CONFIG" --output "$config"

          ${supervise {
            name = "ClickHouse";
            start = ''clickhouse-server --config-file="$config"'';
            readyWhen = ''curl --fail --silent "http://127.0.0.1:$http_port/ping" >/dev/null'';
            onReady = ''
              dnvr-state set host 127.0.0.1
              dnvr-state set httpPort "$http_port"
              dnvr-state set tcpPort "$tcp_port"
              dnvr-state set httpUrl "http://127.0.0.1:$http_port"
              echo "ClickHouse ready at http://127.0.0.1:$http_port"
            '';
          }}
        '';
      };
    };

    processes.connect = {
      packages = [pkgs.apacheKafka pkgs.curl];
      env.KAFKA_BOOTSTRAP_SERVERS = "dnvr://redpanda/bootstrapServers";
      command = pkgs.writeShellApplication {
        name = "durable-sink-connect";
        runtimeInputs = [pkgs.apacheKafka pkgs.curl pkgs.minijinja dnvrState];
        text = ''
          rest_port=$(dnvr-state pick-port)
          config="$DNVR_RUNTIME_DIR/connect-distributed.properties"

          minijinja-cli --strict --autoescape none \
            --define bootstrap_servers="$KAFKA_BOOTSTRAP_SERVERS" \
            --define plugin_path=${connector}/plugins \
            --define listen_host=127.0.0.1 \
            --define rest_port="$rest_port" \
            ${./connect-distributed.properties.j2} --output "$config"

          ${supervise {
            name = "Kafka Connect";
            start = ''connect-distributed.sh "$config"'';
            readyWhen = ''curl --fail --silent "http://127.0.0.1:$rest_port/connector-plugins" >/dev/null'';
            onReady = ''
              dnvr-state set host 127.0.0.1
              dnvr-state set port "$rest_port"
              dnvr-state set url "http://127.0.0.1:$rest_port"
              echo "Kafka Connect ready at http://127.0.0.1:$rest_port"
            '';
          }}
        '';
      };
    };

    processes.records-topic = {
      packages = [pkgs.apacheKafka];
      env.KAFKA_BOOTSTRAP_SERVERS = "dnvr://redpanda/bootstrapServers";
      command = pkgs.writeShellApplication {
        name = "durable-sink-records-topic";
        runtimeInputs = [pkgs.apacheKafka];
        text = ''
          kafka-topics.sh \
            --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
            --create \
            --if-not-exists \
            --topic records.input \
            --partitions 3 \
            --replication-factor 1
        '';
      };
    };

    processes.records-connector = {
      packages = [pkgs.apacheKafka pkgs.clickhouse pkgs.curl pkgs.minijinja];
      env = {
        CONNECT_URL = "dnvr://connect/url";
        KAFKA_BOOTSTRAP_SERVERS = "dnvr://redpanda/bootstrapServers";
        CLICKHOUSE_HOST = "dnvr://clickhouse/host";
        CLICKHOUSE_HTTP_PORT = "dnvr://clickhouse/httpPort";
        CLICKHOUSE_TCP_PORT = "dnvr://clickhouse/tcpPort";
      };
      command = pkgs.writeShellApplication {
        name = "durable-sink-records-connector";
        runtimeInputs = [pkgs.apacheKafka pkgs.clickhouse pkgs.curl pkgs.gnugrep pkgs.minijinja];
        text = ''
          config="$DNVR_RUNTIME_DIR/records-connector.json"

          until kafka-topics.sh \
            --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
            --describe --topic records.input 2>/dev/null \
            | grep -q 'PartitionCount: 3'; do
            sleep 0.2
          done
          until [[ $(clickhouse-client \
            --host "$CLICKHOUSE_HOST" \
            --port "$CLICKHOUSE_TCP_PORT" \
            --query "EXISTS durable_e2e.records") == 1 ]]; do
            sleep 0.2
          done

          minijinja-cli --strict --autoescape json \
            --define clickhouse_host="$CLICKHOUSE_HOST" \
            --define clickhouse_port="$CLICKHOUSE_HTTP_PORT" \
            ${./records-connector.json.j2} --output "$config"
          curl --fail-with-body --silent --show-error \
            --request PUT \
            --header 'Content-Type: application/json' \
            --data-binary @"$config" \
            "$CONNECT_URL/connectors/durable-clickhouse-sink-records/config"
          echo
        '';
      };
    };

    processes.schema = {
      packages = [pkgs.clickhouse];
      env = {
        CLICKHOUSE_HOST = "dnvr://clickhouse/host";
        CLICKHOUSE_TCP_PORT = "dnvr://clickhouse/tcpPort";
        CLICKHOUSE_DATABASE = "durable_e2e";
      };
      command = pkgs.writeShellApplication {
        name = "durable-sink-schema";
        runtimeInputs = [pkgs.clickhouse];
        text = ''
          clickhouse-client \
            --host "$CLICKHOUSE_HOST" \
            --port "$CLICKHOUSE_TCP_PORT" \
            --multiquery \
            < "$DNVR_ROOT/e2e/schema.sql"
          echo "Applied schema for $CLICKHOUSE_DATABASE"
        '';
      };
    };

    env = {
      MINIO_ROOT_USER = "durable-e2e";
      MINIO_ROOT_PASSWORD = "durable-e2e-secret";
    };
  };
}
