#!/usr/bin/env bash
set -euo pipefail

method=GET
data=
url=
while (($# > 0)); do
  case $1 in
    --request)
      method=$2
      shift 2
      ;;
    --data-binary)
      data=$2
      shift 2
      ;;
    --config)
      shift 2
      ;;
    --connect-timeout | --max-time)
      shift 2
      ;;
    --fail | --fail-with-body | --silent | --show-error)
      shift
      ;;
    *)
      url=$1
      shift
      ;;
  esac
done

printf '%s\t%s\t%s\n' "$method" "$url" "$data" >>"$MOCK_LOG"

if [[ $url =~ /connectors/([^/]+)/status$ ]]; then
  connector=${BASH_REMATCH[1]}
  case $SCENARIO in
    zero-tasks)
      printf '%s\n' '{"connector":{"state":"RUNNING"},"tasks":[]}'
      ;;
    failed-task)
      printf '%s\n' '{"connector":{"state":"RUNNING"},"tasks":[{"state":"FAILED"}]}'
      ;;
    second-not-running)
      if [[ $connector == two ]]; then
        printf '%s\n' '{"connector":{"state":"PAUSED"},"tasks":[{"state":"PAUSED"}]}'
      else
        printf '%s\n' '{"connector":{"state":"RUNNING"},"tasks":[{"state":"RUNNING"}]}'
      fi
      ;;
    pause-timeout)
      printf '%s\n' '{"connector":{"state":"RUNNING"},"tasks":[{"state":"RUNNING"}]}'
      ;;
    *)
      if [[ -e $MOCK_STATE/$connector.stopped ]]; then
        if [[ $SCENARIO == restore-stop-timeout ]]; then
          printf '%s\n' '{"connector":{"state":"STOPPED"},"tasks":[{"state":"RUNNING"}]}'
        else
          printf '%s\n' '{"connector":{"state":"STOPPED"},"tasks":[]}'
        fi
      elif [[ -e $MOCK_STATE/$connector.paused ]]; then
        printf '%s\n' '{"connector":{"state":"PAUSED"},"tasks":[{"state":"PAUSED"}]}'
      else
        printf '%s\n' '{"connector":{"state":"RUNNING"},"tasks":[{"state":"RUNNING"}]}'
      fi
      ;;
  esac
  exit
fi

if [[ $url =~ /connectors/([^/]+)/offsets$ ]]; then
  connector=${BASH_REMATCH[1]}
  if [[ $method == PATCH ]]; then
    [[ $SCENARIO != restore-patch-failure ]] || exit 22
    printf '%s' "$data" >"$MOCK_STATE/$connector.offsets"
    printf '%s\n' '{"message":"The offsets for this connector have been altered successfully"}'
    exit
  fi
  case $SCENARIO in
    offsets-failure) exit 22 ;;
    malformed-offsets) printf '%s\n' '{"offsets":"wrong"}'; exit ;;
    empty-offsets) printf '%s\n' '{"offsets":[]}'; exit ;;
    restore-wrong-verification)
      printf '%s\n' '{"offsets":[{"partition":{"kafka_topic":"events.canonical","kafka_partition":0},"offset":{"kafka_offset":999}}]}'
      ;;
    *)
      if [[ -e $MOCK_STATE/$connector.offsets ]]; then
        printf '%s\n' "$(<"$MOCK_STATE/$connector.offsets")"
      elif [[ $connector == two ]]; then
        printf '%s\n' '{"offsets":[{"partition":{"kafka_topic":"calls.canonical","kafka_partition":1},"offset":{"kafka_offset":24}}]}'
      else
        printf '%s\n' '{"offsets":[{"partition":{"kafka_topic":"events.canonical","kafka_partition":0},"offset":{"kafka_offset":42}}]}'
      fi
      ;;
  esac
  exit
fi

if [[ $url =~ /connectors/([^/]+)/pause$ ]]; then
  [[ $SCENARIO != pause-failure ]] || exit 22
  touch "$MOCK_STATE/${BASH_REMATCH[1]}.paused"
  exit
fi

if [[ $url =~ /connectors/([^/]+)/resume$ ]]; then
  paused="$MOCK_STATE/${BASH_REMATCH[1]}.paused"
  [[ ! -e $paused ]] || unlink "$paused"
  exit
fi

if [[ $data == BACKUP\ * ]]; then
  [[ $SCENARIO != backup-failure ]] || exit 22
  destination=${data#* TO }
  printf '%s' "$destination" >"$MOCK_STATE/destination"
  if [[ $SCENARIO == blocked-backup ]]; then
    touch "$MOCK_STATE/blocked"
    while :; do sleep 1; done
  elif [[ $SCENARIO == malformed-response ]]; then
    printf '%s\n' 'not-a-backup'
  else
    printf '123e4567-e89b-12d3-a456-426614174000\tBACKUP_CREATED\n'
  fi
  exit
fi

if [[ $data == SELECT\ *FROM\ system.backups* ]]; then
  case $SCENARIO in
    missing-verification) exit ;;
    wrong-verification)
      printf '%s\n' '{"name":"wrong","status":"BACKUP_FAILED"}'
      ;;
    *)
      "$JQ_BIN" --null-input --compact-output --arg name "$(<"$MOCK_STATE/destination")" \
        '{name: $name, status: "BACKUP_CREATED", num_files: 3, uncompressed_size: 100, compressed_size: 50}'
      ;;
  esac
  exit
fi

echo "unexpected request: $method $url $data" >&2
exit 2
