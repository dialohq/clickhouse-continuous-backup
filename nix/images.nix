{
  pkgs,
  system,
  backup,
  nix2container,
}: let
  n2c = nix2container.packages.${system}.nix2container;
  connector = pkgs.callPackage ./clickhouse-kafka-connect.nix {};
in {
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
