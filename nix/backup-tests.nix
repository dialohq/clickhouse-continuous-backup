{runCommand, writeShellScriptBin, bash, coreutils, jq, ripgrep, shellcheck, backup}:
let
  mockCurl = writeShellScriptBin "mock-curl" (builtins.readFile ../backup/mock-curl.sh);
  mockKafkaProducer = writeShellScriptBin "mock-kafka-producer" (builtins.readFile ../backup/mock-kafka-producer.sh);
in
runCommand "durable-clickhouse-backup-tests" {
  nativeBuildInputs = [bash coreutils jq ripgrep shellcheck];
} ''
  shellcheck --exclude SC2016 ${../backup/backup.sh} ${../backup/restore-offsets.sh} ${../backup/mock-curl.sh} ${../backup/mock-kafka-producer.sh} ${../backup/test.sh}
  bash ${../backup/test.sh} ${backup}/bin/durable-clickhouse-backup ${mockCurl}/bin/mock-curl ${mockKafkaProducer}/bin/mock-kafka-producer ${backup}/bin/durable-clickhouse-restore-offsets
  touch $out
''
