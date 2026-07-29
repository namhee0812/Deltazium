#!/usr/bin/env bash
# backend(제어면) 기동 — bootJar 빌드 후 실행. 로그는 $DZ_LOG_DIR/backend.log (위치 고정).
# 재기동: 그냥 다시 실행 (기존 프로세스 정지 후 기동).
set -euo pipefail
cd "$(dirname "$0")/.."
source deploy/env.sh

BACKEND_PORT=8090
PID_FILE="$DZ_PID_DIR/backend.pid"
LOG_FILE="$DZ_LOG_DIR/backend.log"

# 기존 인스턴스 정지
if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  kill "$(cat "$PID_FILE")"
  for i in $(seq 1 15); do
    kill -0 "$(cat "$PID_FILE")" 2>/dev/null || break
    sleep 1
  done
  echo "[backend] 기존 인스턴스 정지"
fi

./gradlew :backend:bootJar -q

# 로그는 단순 크기 로테이션 (10MB 초과 시 .1로 밀기)
if [ -f "$LOG_FILE" ] && [ "$(stat -c%s "$LOG_FILE")" -gt 10485760 ]; then
  mv "$LOG_FILE" "$LOG_FILE.1"
fi

# 커넥터 템플릿 상대 경로(../connectors)가 맞도록 backend/ 를 작업 디렉터리로 실행
cd backend
nohup java -jar build/libs/backend-*.jar >>"$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"
cd ..

for i in $(seq 1 60); do
  curl -sf "http://localhost:$BACKEND_PORT/api/connections/db-types" >/dev/null 2>&1 && break
  sleep 1
done
if curl -sf "http://localhost:$BACKEND_PORT/api/connections/db-types" >/dev/null 2>&1; then
  echo "[backend] 기동 완료 (port $BACKEND_PORT, 로그: $LOG_FILE)"
else
  echo "[backend] 기동 실패 — 로그 확인: $LOG_FILE" >&2
  exit 1
fi
