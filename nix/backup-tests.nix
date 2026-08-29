{runCommand, writeShellScriptBin, bash, coreutils, jq, ripgrep, shellcheck, backup}:
let
  mockCurl = writeShellScriptBin "mock-curl" (builtins.readFile ../backup/mock-curl.sh);
in
runCommand "durable-clickhouse-backup-tests" {
  nativeBuildInputs = [bash coreutils jq ripgrep shellcheck];
} ''
  shellcheck --exclude SC2016 ${../backup/backup.sh} ${../backup/mock-curl.sh} ${../backup/test.sh}
  bash ${../backup/test.sh} ${backup}/bin/durable-clickhouse-backup ${mockCurl}/bin/mock-curl
  touch $out
''
