{
  description = "Durable, deduplicated Kafka-compatible ingestion into ClickHouse";

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
      deduplicator = pkgs.callPackage ./nix/deduplicator.nix {};
      backup = pkgs.callPackage ./nix/backup.nix {};
      backupTests = pkgs.callPackage ./nix/backup-tests.nix {inherit backup;};
      images = import ./nix/images.nix {inherit pkgs system backup deduplicator nix2container;};
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
        deduplicator = {
          replicas = 1;
          standbyReplicas = 0;
          persistence.enabled = false;
          image = {
            repository = "ghcr.io/dialohq/durable-clickhouse-deduplicator";
            tag = "e2e";
            pullPolicy = "Never";
          };
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
          credentialsSecret.name = "clickhouse-credentials";
        };
        pipelines = [{
          name = "events";
          rawTopic = "events.raw";
          canonicalTopic = "events.canonical";
          conflictTopic = "events.conflicts";
          table = "events";
          rawRetentionMs = 600000;
          deduplicationRetentionMs = 1200000;
          canonicalRetentionMs = 3600000;
          conflictRetentionMs = 3600000;
          partitions = 3;
        }];
      };
    in {
      inherit backup backupTests deduplicator chart chartSource e2eValues;
      deduplicatorImage = images.deduplicator;
      connectImage = images.connect;
      manifests = nixidyEnv.environmentPackage;
      e2eManifests = e2eEnv.environmentPackage;
      default = chart;
    });

    checks = nixpkgs.lib.genAttrs ["x86_64-linux"] (system: let
      packages = self.packages.${system};
      pkgs = import nixpkgs {inherit system;};
    in {
      inherit (packages) backupTests deduplicator manifests e2eManifests;
      chart = pkgs.runCommand "check-chart" {
        nativeBuildInputs = [pkgs.kubernetes-helm];
      } ''
        chart_args=(
          --set-string kafka.bootstrapServers=kafka.example:9092
          --set-string clickhouse.host=clickhouse.example
          --set-string clickhouse.credentialsSecret.name=clickhouse-credentials
          --set-string 'pipelines[0].name=events'
          --set-string 'pipelines[0].rawTopic=events.raw'
          --set-string 'pipelines[0].canonicalTopic=events.canonical'
          --set-string 'pipelines[0].conflictTopic=events.conflicts'
          --set-string 'pipelines[0].table=events'
          --set backup.enabled=true
          --set-string backup.credentialsSecret.name=clickhouse-backup-credentials
        )
        helm lint --strict ${packages.chartSource} "''${chart_args[@]}"
        helm template test ${packages.chartSource} "''${chart_args[@]}" > rendered.yaml

        expect_rejected() {
          if helm template test ${packages.chartSource} "''${chart_args[@]}" "$@" >/dev/null 2>&1; then
            echo "unsafe values were accepted: $*" >&2
            exit 1
          fi
        }
        expect_rejected --set-string 'pipelines[0].connectorConfig.exactlyOnce=false'
        expect_rejected --set-string 'pipelines[0].rawRetentionMs=2592000000'
        expect_rejected --set backup.pauseTimeoutSeconds=0
        expect_rejected --set-string "backup.pathPrefix=invalid')"
        expect_rejected --set-string 'pipelines[1].name=events' \
          --set-string 'pipelines[1].rawTopic=other.raw' \
          --set-string 'pipelines[1].canonicalTopic=other.canonical' \
          --set-string 'pipelines[1].conflictTopic=other.conflicts' \
          --set-string 'pipelines[1].table=other_events'

        if grep -F "Disk('" rendered.yaml; then
          echo "backup unexpectedly uses server-local Disk metadata" >&2
          exit 1
        fi
        grep -F '/bin/durable-clickhouse-backup' rendered.yaml >/dev/null
        grep -F 'name: BACKUP_RUN_ID' rendered.yaml >/dev/null
        grep -F 'BACKUP_NAMED_COLLECTION' rendered.yaml >/dev/null
        grep -F 'KAFKA_RECOVERY_TOPIC' rendered.yaml >/dev/null
        grep -F 'cleanup.policy=compact,retention.ms=-1,retention.bytes=-1' rendered.yaml >/dev/null
        grep -F '.recovery-points' rendered.yaml >/dev/null
        grep -F 'durable_clickhouse_backups' rendered.yaml >/dev/null
        touch $out
      '';
    });

    devShells = forAllSystems (system: let pkgs = import nixpkgs {inherit system;}; in {
      default = pkgs.mkShell {
        packages = [
          pkgs.apacheKafka
          pkgs.jdk
          pkgs.jq
          pkgs.kotlin
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
