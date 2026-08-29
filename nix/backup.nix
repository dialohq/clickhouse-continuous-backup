{writeShellScriptBin}:
writeShellScriptBin "durable-clickhouse-backup" (builtins.readFile ../backup/backup.sh)
