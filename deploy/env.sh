# Deltazium 베어메탈 공통 환경 (docker-compose 전환 전까지의 1순위 배포 방식)
# 사용법: source deploy/env.sh

export DZ_RT="${DZ_RT:-$HOME/deltazium-runtime}"          # 바이너리·데이터 루트 (repo 밖)
export JAVA_HOME="${DZ_JAVA_HOME:-$HOME/java21}"
export PATH="$JAVA_HOME/bin:$DZ_RT/bin:$PATH"

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

# 비밀값(리포 밖): API 키 등은 conf/secrets.env에 둔다 (git 추적 안 됨)
[ -f "$DZ_RT/conf/secrets.env" ] && . "$DZ_RT/conf/secrets.env"
