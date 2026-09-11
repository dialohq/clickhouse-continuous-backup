{
  fetchurl,
  runCommand,
  unzip,
}:
runCommand "clickhouse-kafka-connect-1.5.0" {
  src = fetchurl {
    url = "https://github.com/ClickHouse/clickhouse-kafka-connect/releases/download/v1.5.0/clickhouse-kafka-connect-v1.5.0.zip";
    hash = "sha256-U+MczZZzS4+x8lEnnaF4Y7mxk/B2SmEoBGGqMUBQ7MY=";
  };
  nativeBuildInputs = [unzip];
} ''
  mkdir -p $out/plugins/clickhouse
  unzip -q $src -d $out/plugins/clickhouse
''
