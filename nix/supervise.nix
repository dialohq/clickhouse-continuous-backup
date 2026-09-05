{lib}: {
  name,
  start,
  readyWhen,
  onReady ? "",
  interval ? "0.2",
}: let
  quotedName = lib.escapeShellArg name;
in ''
  (
    exec ${start}
  ) &
  child=$!
  trap 'kill -TERM "$child" 2>/dev/null || true; wait "$child" 2>/dev/null || true' EXIT INT TERM

  until ${readyWhen}; do
    if ! kill -0 "$child" 2>/dev/null; then
      if wait "$child"; then
        echo ${quotedName}' exited before becoming ready' >&2
        exit 1
      else
        exit $?
      fi
    fi
    sleep ${lib.escapeShellArg interval}
  done

  ${onReady}
  wait "$child"
''
