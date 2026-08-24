#!/usr/bin/env bash
# deploy/watchdog.sh — 장애 자동 감지·재기동 워치독
#
# 배경: 26-08-20 디스크 풀로 Kafka가 죽고 나흘간 미검출된 장애의 재발 방지.
# `dzadmin status` 출력을 파싱해 DOWN인 infra(pg/minio/kafka/connect)·backend
# 컴포넌트를 자동 재기동한다. web(vite)은 개발 도구이므로 감시 대상에서 제외한다
# (docs/operations.md 워치독 절 참고).
#
# 재기동에는 dzadmin의 컴포넌트 start를 그대로 쓴다:
#   - infra start(=deploy/start-infra.sh)는 컴포넌트별로 이미 떠 있으면 건너뛰는
#     멱등 스크립트라 일부만 죽어 있어도 전체를 다시 호출하면 죽은 것만 재기동된다.
#   - backend start(=deploy/start-backend.sh)는 항상 기존 프로세스를 정리하고
#     재기동하지만, 여기서는 backend가 DOWN으로 확인된 경우에만 호출한다.
#
# crontab 등록은 이 스크립트의 범위 밖 (docs/operations.md 참고). 예:
#   */5 * * * * /home/nhchoi/deltazium/deploy/watchdog.sh
set -uo pipefail
cd "$(dirname "$0")/.."
source deploy/env.sh

# 동시 실행 방지 — cron 주기(5분)보다 재기동이 오래 걸리는 경우 중복 실행을 막는다.
LOCK_FILE=/tmp/dz-watchdog.lock
exec 200>"$LOCK_FILE"
flock -n 200 || exit 0

LOG_FILE="$DZ_LOG_DIR/watchdog.log"
ts() { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "[$(ts)] $*" >>"$LOG_FILE"; }

if ! STATUS_OUTPUT="$(./deploy/dzadmin status 2>&1)"; then
    log "ERROR dzadmin status 실행 실패 — 출력: $(echo "$STATUS_OUTPUT" | tr '\n' ' ')"
    exit 1
fi

# [infra]/[backend] 섹션에서 DOWN인 컴포넌트 라벨만 추출 ([web] 섹션은 보지 않음).
infra_down="$(echo "$STATUS_OUTPUT" | awk '/^\[infra\]/{f=1;next} /^\[/{f=0} f && /DOWN/{print $1}')"
backend_down="$(echo "$STATUS_OUTPUT" | awk '/^\[backend\]/{f=1;next} /^\[/{f=0} f && /DOWN/{print $1}')"

if [ -z "$infra_down" ] && [ -z "$backend_down" ]; then
    exit 0   # 정상 — 로그를 남기지 않는다 (5분 주기 실행 시 로그 오염 방지)
fi

if [ -n "$infra_down" ]; then
    log "WARN infra DOWN 감지: $(echo "$infra_down" | tr '\n' ' ')"
    log "재기동 시도: dzadmin infra start"
    if OUT="$(./deploy/dzadmin infra start 2>&1)"; then
        log "infra 재기동 완료"
    else
        log "ERROR infra 재기동 실패 — 출력: $(echo "$OUT" | tr '\n' ' ')"
    fi
fi

if [ -n "$backend_down" ]; then
    log "WARN backend DOWN 감지"
    log "재기동 시도: dzadmin backend start"
    if OUT="$(./deploy/dzadmin backend start 2>&1)"; then
        log "backend 재기동 완료"
    else
        log "ERROR backend 재기동 실패 — 출력: $(echo "$OUT" | tr '\n' ' ')"
    fi
fi
