{pkgs, system, backup, deduplicator, nix2container}: let
  n2c = nix2container.packages.${system}.nix2container;
  connector = pkgs.runCommand "clickhouse-kafka-connect-1.5.0" {
    src = pkgs.fetchurl {
      url = "https://github.com/ClickHouse/clickhouse-kafka-connect/releases/download/v1.5.0/clickhouse-kafka-connect-v1.5.0.zip";
      hash = "sha256-U+MczZZzS4+x8lEnnaF4Y7mxk/B2SmEoBGGqMUBQ7MY=";
    };
    nativeBuildInputs = [pkgs.unzip];
  } ''
    mkdir -p $out/plugins/clickhouse
    unzip -q $src -d $out/plugins/clickhouse
  '';
in {
  deduplicator = n2c.buildImage {
    name = "ghcr.io/dialohq/durable-clickhouse-deduplicator";
    config = {
      Cmd = [
        "${pkgs.jre}/bin/java"
        "-Djava.io.tmpdir=/var/lib/deduplicator"
        "-cp"
        "/app/deduplicator.jar:${pkgs.apacheKafka}/libs/*"
        "io.dialo.durableclickhouse.Main"
      ];
      WorkingDir = "/var/lib/deduplicator";
      User = "65532:65532";
      Env = ["SSL_CERT_FILE=${pkgs.dockerTools.caCertificates}/etc/ssl/certs/ca-bundle.crt"];
    };
    layers = [
      (n2c.buildLayer {copyToRoot = deduplicator;})
      (n2c.buildLayer {
        copyToRoot = pkgs.buildEnv {
          name = "deduplicator-runtime";
          paths = [pkgs.apacheKafka pkgs.jre pkgs.dockerTools.caCertificates];
          pathsToLink = ["/bin" "/lib"];
        };
      })
    ];
  };

  connect = n2c.buildImage {
    name = "ghcr.io/dialohq/durable-clickhouse-connect";
    config = {
      Cmd = [
        "${pkgs.apacheKafka}/bin/connect-distributed.sh"
        "/etc/durable-clickhouse/connect-distributed.properties"
      ];
      User = "65532:65532";
      Env = ["SSL_CERT_FILE=${pkgs.dockerTools.caCertificates}/etc/ssl/certs/ca-bundle.crt"];
    };
    layers = [
      (n2c.buildLayer {
        copyToRoot = pkgs.buildEnv {
          name = "connect-runtime";
          paths = [
            pkgs.apacheKafka
            pkgs.jre
            pkgs.bash
            pkgs.coreutils
            pkgs.curl
            pkgs.jq
            pkgs.dockerTools.caCertificates
            backup
            connector
          ];
          pathsToLink = ["/bin" "/lib" "/plugins"];
        };
      })
    ];
  };
}
