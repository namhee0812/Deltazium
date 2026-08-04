#!/usr/bin/env bash
# backend(제어면) 기동 — bootJar 빌드 후 실행.
# 본 로그는 Spring logback이 $DZ_LOG_DIR/backend.log에 쓰고 일 단위로
# $DZ_LOG_DIR/yyyy-MM-dd/ 디렉터리에 롤오버한다 (application.yml logging 설정).
# 콘솔 리다이렉트 파일(backend-console.log)은 JVM 기동 초기 오류 캡처용 (기동마다 초기화).
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

# 커넥터 템플릿 상대 경로(../connectors)가 맞도록 backend/ 를 작업 디렉터리로 실행.
# 스크립트 기동 시엔 콘솔 로깅 OFF — 파일(logback)과의 이중 기록 방지 (IDE 실행은 콘솔 유지).
cd backend
LOGGING_THRESHOLD_CONSOLE=OFF \
nohup java -jar build/libs/backend-*.jar >"$DZ_LOG_DIR/backend-console.log" 2>&1 &
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
