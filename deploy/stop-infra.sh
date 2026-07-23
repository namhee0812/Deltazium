#!/usr/bin/env bash
# Deltazium 인프라 정지 (역순). 데이터는 보존한다 — 데이터 삭제는 이 스크립트 범위 밖.
set -uo pipefail
cd "$(dirname "$0")/.."
source deploy/env.sh

stop_pid() { # name
  local f="$DZ_PID_DIR/$1.pid"
  if [ -f "$f" ] && kill -0 "$(cat "$f")" 2>/dev/null; then
    kill "$(cat "$f")" && echo "[$1] 정지"
    rm -f "$f"
  else
    echo "[$1] 실행 중 아님"
  fi
}

stop_pid connect
stop_pid kafka
stop_pid minio
"$DZ_PG_BIN/pg_ctl" -D "$DZ_PG_DATA" stop -m fast 2>/dev/null && echo "[pg] 정지" || echo "[pg] 실행 중 아님"
