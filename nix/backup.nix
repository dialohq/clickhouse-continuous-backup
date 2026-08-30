{symlinkJoin, writeShellScriptBin}:
symlinkJoin {
  name = "durable-clickhouse-recovery";
  paths = [
    (writeShellScriptBin "durable-clickhouse-backup" (builtins.readFile ../backup/backup.sh))
    (writeShellScriptBin "durable-clickhouse-restore-offsets" (builtins.readFile ../backup/restore-offsets.sh))
  ];
}
