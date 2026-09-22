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
    dnvr = {
      url = "github:plan9better/dnvr/runtime-config";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    nixpkgs,
    nixidy,
    nix2container,
    dnvr,
    ...
  }: let
    systems = ["x86_64-linux" "aarch64-linux"];
    forAllSystems = nixpkgs.lib.genAttrs systems;
  in {
    packages = forAllSystems (system: let
      pkgs = import nixpkgs {inherit system;};
      backup = pkgs.callPackage ./nix/backup.nix {};
      images = import ./nix/images.nix {inherit pkgs system backup nix2container;};
      chartSource = ./chart;
      chart = pkgs.runCommand "durable-clickhouse-sink-chart-0.1.0" {nativeBuildInputs = [pkgs.kubernetes-helm];} ''
        mkdir -p $out
        helm package ${chartSource} --destination $out
      '';
      nixidyEnv = nixidy.lib.mkEnv {
        inherit pkgs;
        modules = [./nix/nixidy-validation.nix];
        extraSpecialArgs = {inherit chartSource;};
      };
    in {
      inherit backup chart chartSource;
      connectImage = images.connect;
      manifests = nixidyEnv.environmentPackage;
      default = chart;
    });

    checks = nixpkgs.lib.genAttrs ["x86_64-linux"] (system: let
      packages = self.packages.${system};
      pkgs = import nixpkgs {inherit system;};
    in {
      inherit (packages) backup manifests;
      chart =
        pkgs.runCommand "check-chart" {
          nativeBuildInputs = [pkgs.diffutils pkgs.kubernetes-helm];
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
            --set-string recovery.credentialsSecret.name=clickhouse-recovery-credentials
            --set backup.maxIncrementalsPerFull=2
            --set backup.maxBandwidthBytesPerSecond=1048576
          )
          helm lint --strict ${packages.chartSource} "''${chart_args[@]}"
          helm template test ${packages.chartSource} "''${chart_args[@]}" > rendered.yaml
          helm template test ${packages.chartSource} "''${chart_args[@]}" \
            --set-string kafka.existingSecret=kafka-credentials > /dev/null
          helm template test ${packages.chartSource} "''${chart_args[@]}" \
            --set backup.enabled=false > rendered-without-backups.yaml
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
          expect_rejected --set-string 'pipelines[0].connectorConfig.consumer\.override\.group\.id=shared'
          expect_rejected --set-string 'pipelines[0].connectorConfig.topics\.regex=.*'
          expect_rejected --set backup.pauseTimeoutSeconds=0
          expect_rejected --set backup.activeDeadlineSeconds=0
          expect_rejected --set backup.terminationGracePeriodSeconds=15
          expect_rejected --set timeouts.kafkaTransactionSeconds=0
          expect_rejected --set recovery.replayTopicReplicationFactor=0
          expect_rejected --set recovery.replayTopicRetentionMs=0
          expect_rejected --set recovery.replayBatchRecords=0
          for component in clickhouse backup recovery; do
            expect_rejected --set-string "$component.credentialsSecret.name="
            expect_rejected --set-string "$component.credentialsFile=/vault/secrets/clickhouse.properties"
            expect_rejected --set-string "$component.credentialsSecret.name=" \
              --set-string "$component.credentialsFile=relative/path"
            expect_rejected --set-string "$component.credentialsSecret.name=" \
              --set-string "$component.credentialsFile=/invalid:path"
          done
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
          grep -F '/etc/durable-clickhouse/backup.json' rendered.yaml >/dev/null
          grep -F '/etc/durable-clickhouse/target.json' rendered.yaml >/dev/null
          grep -F '"maxIncrementalsPerFull": 2' rendered.yaml >/dev/null
          grep -F '"maxBackupBandwidth": 1048576' rendered.yaml >/dev/null
          grep -F '"catalogTopic": "test-durable-clickhouse-sink.backup-catalog"' rendered.yaml >/dev/null
          grep -F '"kafkaTransactionSeconds":30' rendered.yaml >/dev/null
          grep -F 'activeDeadlineSeconds: 21600' rendered.yaml >/dev/null
          grep -F 'activeDeadlineSeconds: 600' rendered.yaml >/dev/null
          grep -F 'cleanup.policy=compact,retention.ms=-1,retention.bytes=-1' rendered.yaml >/dev/null
          grep -F 'cleanup.policy=delete,retention.ms=$RETENTION,retention.bytes=-1' rendered.yaml >/dev/null
          grep -F '.backup-catalog' rendered.yaml >/dev/null
          grep -F 'durable_clickhouse_backups' rendered.yaml >/dev/null
          grep -F 'component: recovery-controller' rendered.yaml >/dev/null
          grep -F 'component: recovery-controller' rendered-without-backups.yaml >/dev/null
          grep -F 'resources: ["tablerecoveries/status"]' rendered.yaml >/dev/null
          grep -F '/etc/durable-clickhouse/recovery.json' rendered.yaml >/dev/null
          grep -F '"replayTopicRetentionMs": 604800000' rendered.yaml >/dev/null
          grep -F 'secretName: clickhouse-recovery-credentials' rendered.yaml >/dev/null
          grep -F 'backupID:' ${packages.chartSource}/crds/table-recovery.yaml >/dev/null
          grep -F 'rule: self == oldSelf' ${packages.chartSource}/crds/table-recovery.yaml >/dev/null
          ${packages.backup}/bin/durable-clickhouse-backup print-recovery-crd > generated-recovery-crd.yaml
          diff -u ${packages.chartSource}/crds/table-recovery.yaml generated-recovery-crd.yaml
          touch $out
        '';
    });

    devShells = nixpkgs.lib.genAttrs ["x86_64-linux"] (system: let
      pkgs = import nixpkgs {
        inherit system;
        config = {
          allowUnfreePredicate = pkg: nixpkgs.lib.getName pkg == "redpanda-rpk";
          permittedInsecurePackages = [
            "minio-2025-10-15T17-29-55Z"
          ];
        };
      };
      dnvrFramework = import dnvr {inherit pkgs;};
    in
      (dnvrFramework.mkShells [./nix/dev.nix]).devShells);
  };
}
