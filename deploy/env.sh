# Deltazium 베어메탈 공통 환경 (docker-compose 전환 전까지의 1순위 배포 방식)
# 사용법: source deploy/env.sh

export DZ_RT="${DZ_RT:-$HOME/deltazium-runtime}"          # 바이너리·데이터 루트 (repo 밖)
export JAVA_HOME="${DZ_JAVA_HOME:-$HOME/java21}"
export PATH="$JAVA_HOME/bin:$DZ_RT/bin:$PATH"

# 저장소 프로파일 (architecture.md 2.2·3절, TODO ③): minio(기본, 온프레미스) | r2(Cloudflare R2 —
# SaaS DW 타깃 전제). r2로 전환하려면 deploy/env.local.sh(git-ignore)에서 오버라이드할 것 —
# deploy/env.local.sh.example 참고. 이 값 하나가 backend(deltazium.iceberg.profile)·start-infra·
# smoke-test·watchdog·dzadmin status 분기의 단일 진원지다.
export DZ_STORAGE_PROFILE="${DZ_STORAGE_PROFILE:-minio}"

# 포트 (SQueryDev 서버에서 기존 서비스와 충돌 없는 값으로 고정)
export DZ_KAFKA_PORT=9092
export DZ_KAFKA_CONTROLLER_PORT=9093
export DZ_CONNECT_PORT=8083
export DZ_PG_PORT=5433
export DZ_MINIO_PORT=9010
export DZ_MINIO_CONSOLE_PORT=9011

# PostgreSQL (메타데이터 + Iceberg JDBC 카탈로그)
export DZ_PG_BIN="${DZ_PG_BIN:-/home/dstream/dshome/postgresql-15.3/bin}"
export DZ_PG_DATA="$DZ_RT/pg/data"
export DZ_PG_USER=deltazium
export DZ_PG_DB=deltazium

# MinIO (dev 자격증명 — 토이 프로젝트 한정)
export DZ_MINIO_ROOT_USER=deltazium
export DZ_MINIO_ROOT_PASSWORD=deltazium123
export DZ_MINIO_BUCKET=deltazium-warehouse

export DZ_KAFKA_HOME="$DZ_RT/kafka"
export DZ_KAFKA_DATA="$DZ_RT/kafka-data"
export DZ_PLUGIN_PATH="$DZ_RT/connect-plugins"
export DZ_LOG_DIR="$DZ_RT/logs"
export DZ_PID_DIR="$DZ_RT/pids"
mkdir -p "$DZ_LOG_DIR" "$DZ_PID_DIR"

# 저장소 프로파일 로컬 오버라이드 (git-ignore, deploy/env.local.sh.example 참고) — r2 전환 시
# DZ_STORAGE_PROFILE=r2 + DZ_R2_* 를 여기서 재정의한다. 위 DZ_STORAGE_PROFILE 기본값(minio)
# 대입 이후에 소싱해야 오버라이드가 먹는다.
# 주의: `[ -f ] && .` 단축형은 파일이 없을 때 종료 코드 1을 남겨
# set -e 환경(start-*.sh)에서 스크립트를 죽인다 — 반드시 if 문으로.
if [ -f "$(dirname "${BASH_SOURCE[0]:-$0}")/env.local.sh" ]; then
  . "$(dirname "${BASH_SOURCE[0]:-$0}")/env.local.sh"
fi

# 비밀값(리포 밖): API 키 등은 conf/secrets.env에 둔다 (git 추적 안 됨)
# 주의: `[ -f ] && .` 단축형은 파일이 없을 때 종료 코드 1을 남겨
# set -e 환경(start-*.sh)에서 스크립트를 죽인다 — 반드시 if 문으로.
if [ -f "$DZ_RT/conf/secrets.env" ]; then
  . "$DZ_RT/conf/secrets.env"
fi
