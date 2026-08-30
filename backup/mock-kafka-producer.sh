#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >>"$MOCK_KAFKA_LOG"
tee -a "$MOCK_KAFKA_RECORDS" >/dev/null
[[ $SCENARIO != manifest-failure ]]
