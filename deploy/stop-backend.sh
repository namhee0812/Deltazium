#!/usr/bin/env bash
# backend 정지
set -uo pipefail
cd "$(dirname "$0")/.."
source deploy/env.sh

PID_FILE="$DZ_PID_DIR/backend.pid"
if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  kill "$(cat "$PID_FILE")" && echo "[backend] 정지"
  rm -f "$PID_FILE"
else
  echo "[backend] 실행 중 아님"
fi
