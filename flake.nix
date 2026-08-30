{
  description = "Durable Kafka-compatible ingestion into ClickHouse with incremental backups";

  nixConfig = {
    extra-substituters = ["https://nix-community.cachix.org"];
    extra-trusted-public-keys = ["nix-community.cachix.org-1:mB9FSh9qf2dCimDSUo8Zy7bkq5CX+/rkCWyvRCYg3Fs="];
  };

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    nixidy = {
      url = "github:arnarg/nixidy/main";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    nix2container = {
      url = "github:dialohq/nix2container/compressed-layers";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = inputs @ {self, nixpkgs, nixidy, nix2container, ...}: let
    systems = ["x86_64-linux" "aarch64-linux"];
    forAllSystems = nixpkgs.lib.genAttrs systems;
  in {
    packages = forAllSystems (system: let
      pkgs = import nixpkgs {inherit system;};
      backup = pkgs.callPackage ./nix/backup.nix {};
      images = import ./nix/images.nix {inherit pkgs system backup nix2container;};
      chartSource = pkgs.callPackage ./nix/chart.nix {};
      chart = pkgs.runCommand "durable-clickhouse-sink-chart-0.1.0" {nativeBuildInputs = [pkgs.kubernetes-helm];} ''
        mkdir -p $out
        helm package ${chartSource} --destination $out
      '';
      nixidyEnv = nixidy.lib.mkEnv {
        inherit pkgs;
        modules = [./nix/nixidy-validation.nix];
        extraSpecialArgs = {inherit chartSource;};
      };
      e2eEnv = nixidy.lib.mkEnv {
        inherit pkgs;
        modules = [./nix/e2e.nix];
      };
      e2eValues = (pkgs.formats.json {}).generate "durable-clickhouse-sink-e2e-values.json" {
        stateNamespace = "e2e";
        kafka = {
          bootstrapServers = "redpanda.durable-sink-e2e.svc.cluster.local:9092";
          replicationFactor = 1;
          internalTopicReplicationFactor = 1;
        };
        clickhouse = {
          host = "clickhouse.durable-sink-e2e.svc.cluster.local";
          port = 8123;
          secure = false;
          database = "durable_e2e";
          credentialsSecret.name = "clickhouse-credentials";
        };
        connect = {
          replicas = 1;
          tasksMax = 1;
          image = {
            repository = "ghcr.io/dialohq/durable-clickhouse-connect";
            tag = "e2e";
            pullPolicy = "Never";
          };
        };
        topics.partitions = 3;
        backup = {
          enabled = true;
          suspend = true;
          namedCollection = "durable_backups";
          pathPrefix = "durable-e2e";
          maxIncrementalsPerFull = 2;
          maxBandwidthBytesPerSecond = 262144;
          credentialsSecret.name = "clickhouse-credentials";
        };
        pipelines = [{
          name = "records";
          topic = "records.input";
          table = "records";
          retentionMs = 3600000;
          partitions = 3;
        }];
      };
    in {
      inherit backup chart chartSource e2eValues;
      connectImage = images.connect;
      manifests = nixidyEnv.environmentPackage;
      e2eManifests = e2eEnv.environmentPackage;
      default = chart;
    });

    checks = nixpkgs.lib.genAttrs ["x86_64-linux"] (system: let
      packages = self.packages.${system};
      pkgs = import nixpkgs {inherit system;};
    in {
      inherit (packages) backup manifests e2eManifests;
      e2eScript = pkgs.runCommand "check-e2e-script" {
        nativeBuildInputs = [pkgs.bash pkgs.shellcheck];
      } ''
        bash -n ${./e2e/run-rke2.sh}
        shellcheck ${./e2e/run-rke2.sh}
        touch $out
      '';
      chart = pkgs.runCommand "check-chart" {
        nativeBuildInputs = [pkgs.kubernetes-helm];
      } ''
        chart_args=(
          --set-string kafka.bootstrapServers=kafka.example:9092
          --set-string clickhouse.host=clickhouse.example
          --set-string clickhouse.credentialsSecret.name=clickhouse-credentials
          --set-string 'pipelines[0].name=records'
          --set-string 'pipelines[0].topic=records.input'
          --set-string 'pipelines[0].table=records'
          --set backup.enabled=true
          --set-string backup.credentialsSecret.name=clickhouse-backup-credentials
        )
        helm lint --strict ${packages.chartSource} "''${chart_args[@]}"
        helm template test ${packages.chartSource} "''${chart_args[@]}" > rendered.yaml
        helm template test ${packages.chartSource} "''${chart_args[@]}" \
          --set-string 'pipelines[1].name=other' \
          --set-string 'pipelines[1].topic=other.input' \
          --set-string 'pipelines[1].table=records' > rendered-shared-table.yaml

        expect_rejected() {
          if helm template test ${packages.chartSource} "''${chart_args[@]}" "$@" >/dev/null 2>&1; then
            echo "unsafe values were accepted: $*" >&2
            exit 1
          fi
        }
        expect_rejected --set-string 'pipelines[0].connectorConfig.exactlyOnce=false'
        expect_rejected --set backup.pauseTimeoutSeconds=0
        expect_rejected --set backup.activeDeadlineSeconds=0
        expect_rejected --set backup.terminationGracePeriodSeconds=15
        expect_rejected --set timeouts.kafkaTransactionSeconds=0
        expect_rejected --set-string "backup.pathPrefix=invalid')"
        expect_rejected --set-string 'backup.pathPrefix=valid/../escape'
        expect_rejected --set-string 'pipelines[1].name=records' \
          --set-string 'pipelines[1].topic=other.input' \
          --set-string 'pipelines[1].table=other_records'

        if grep -F "Disk('" rendered.yaml; then
          echo "backup unexpectedly uses server-local Disk metadata" >&2
          exit 1
        fi
        grep -F '/bin/durable-clickhouse-backup' rendered.yaml >/dev/null
        grep -F 'validate-targets' rendered.yaml >/dev/null
        grep -F 'async_insert=0,insert_deduplicate=1' rendered.yaml >/dev/null
        grep -F 'name: BACKUP_RUN_ID' rendered.yaml >/dev/null
        grep -F 'BACKUP_NAMED_COLLECTION' rendered.yaml >/dev/null
        grep -F 'BACKUP_PIPELINES' rendered.yaml >/dev/null
        grep -F 'MAX_INCREMENTALS_PER_FULL' rendered.yaml >/dev/null
        grep -F 'MAX_BACKUP_BANDWIDTH' rendered.yaml >/dev/null
        grep -F 'KAFKA_BACKUP_CATALOG_TOPIC' rendered.yaml >/dev/null
        grep -F 'RUNTIME_TIMEOUTS' rendered.yaml >/dev/null
        grep -F 'activeDeadlineSeconds: 21600' rendered.yaml >/dev/null
        grep -F 'activeDeadlineSeconds: 600' rendered.yaml >/dev/null
        grep -F 'cleanup.policy=compact,retention.ms=-1,retention.bytes=-1' rendered.yaml >/dev/null
        grep -F 'cleanup.policy=delete,retention.ms=$RETENTION,retention.bytes=-1' rendered.yaml >/dev/null
        grep -F '.backup-catalog' rendered.yaml >/dev/null
        grep -F 'durable_clickhouse_backups' rendered.yaml >/dev/null
        touch $out
      '';
    });

    devShells = forAllSystems (system: let pkgs = import nixpkgs {inherit system;}; in {
      default = pkgs.mkShell {
        packages = [
          pkgs.apacheKafka
          pkgs.jq
          pkgs.kubeconform
          pkgs.kubectl
          pkgs.kubernetes-helm
          pkgs.containerd
          pkgs.rke2
        ];
      };
    });
  };
}
