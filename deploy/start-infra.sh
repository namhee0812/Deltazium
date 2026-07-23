#!/usr/bin/env bash
# Deltazium 베어메탈 인프라 기동: PostgreSQL → MinIO → Kafka → Kafka Connect
# 멱등: 이미 떠 있는 컴포넌트는 건너뛴다.
set -euo pipefail
cd "$(dirname "$0")/.."
source deploy/env.sh

is_up() { ss -ltn 2>/dev/null | awk '{print $4}' | grep -qE ":$1\$"; }

wait_port() { # port name timeout_sec
  local i
  for i in $(seq 1 "${3:-30}"); do is_up "$1" && return 0; sleep 1; done
  echo "ERROR: $2 (port $1) 기동 실패 — 로그: $DZ_LOG_DIR" >&2; return 1
}

render() { # src dst — ${DZ_*} 변수만 치환
  mkdir -p "$(dirname "$2")"
  envsubst "$(printf '${%s} ' $(compgen -v | grep '^DZ_'))" < "$1" > "$2"
}

## 1. PostgreSQL (메타데이터 + Iceberg JDBC 카탈로그)
if is_up "$DZ_PG_PORT"; then
  echo "[pg] 이미 기동됨 (port $DZ_PG_PORT)"
else
  if [ ! -d "$DZ_PG_DATA" ]; then
    echo "[pg] initdb: $DZ_PG_DATA"
    mkdir -p "$DZ_PG_DATA"
    "$DZ_PG_BIN/initdb" -D "$DZ_PG_DATA" -U "$DZ_PG_USER" -E UTF8 --locale=C >"$DZ_LOG_DIR/pg-initdb.log" 2>&1
  fi
  "$DZ_PG_BIN/pg_ctl" -D "$DZ_PG_DATA" -l "$DZ_LOG_DIR/pg.log" \
    -o "-p $DZ_PG_PORT -k /tmp -c listen_addresses=localhost" start
  wait_port "$DZ_PG_PORT" PostgreSQL
  # 데이터베이스 2개: 메타데이터(deltazium), Iceberg 카탈로그(iceberg_catalog)
  for db in "$DZ_PG_DB" iceberg_catalog; do
    "$DZ_PG_BIN/psql" -h localhost -p "$DZ_PG_PORT" -U "$DZ_PG_USER" -d postgres -tc \
      "SELECT 1 FROM pg_database WHERE datname='$db'" | grep -q 1 || \
      "$DZ_PG_BIN/createdb" -h localhost -p "$DZ_PG_PORT" -U "$DZ_PG_USER" "$db"
  done
fi

## 2. MinIO
if is_up "$DZ_MINIO_PORT"; then
  echo "[minio] 이미 기동됨 (port $DZ_MINIO_PORT)"
else
  MINIO_ROOT_USER="$DZ_MINIO_ROOT_USER" MINIO_ROOT_PASSWORD="$DZ_MINIO_ROOT_PASSWORD" \
    nohup "$DZ_RT/bin/minio" server "$DZ_RT/minio-data" \
      --address ":$DZ_MINIO_PORT" --console-address ":$DZ_MINIO_CONSOLE_PORT" \
      >"$DZ_LOG_DIR/minio.log" 2>&1 &
  echo $! > "$DZ_PID_DIR/minio.pid"
  wait_port "$DZ_MINIO_PORT" MinIO
  # 버킷 생성 (멱등)
  "$DZ_RT/bin/mc" alias set dz "http://localhost:$DZ_MINIO_PORT" \
    "$DZ_MINIO_ROOT_USER" "$DZ_MINIO_ROOT_PASSWORD" >/dev/null
  "$DZ_RT/bin/mc" mb --ignore-existing "dz/$DZ_MINIO_BUCKET" >/dev/null
fi

## 3. Kafka (KRaft 단일 노드)
if is_up "$DZ_KAFKA_PORT"; then
  echo "[kafka] 이미 기동됨 (port $DZ_KAFKA_PORT)"
else
  render deploy/kafka/server.properties "$DZ_RT/conf/server.properties"
  if [ ! -f "$DZ_KAFKA_DATA/meta.properties" ]; then
    CID=$("$DZ_KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
    "$DZ_KAFKA_HOME/bin/kafka-storage.sh" format -t "$CID" -c "$DZ_RT/conf/server.properties" \
      >"$DZ_LOG_DIR/kafka-format.log" 2>&1
  fi
  LOG_DIR="$DZ_LOG_DIR" nohup "$DZ_KAFKA_HOME/bin/kafka-server-start.sh" \
    "$DZ_RT/conf/server.properties" >"$DZ_LOG_DIR/kafka.log" 2>&1 &
  echo $! > "$DZ_PID_DIR/kafka.pid"
  wait_port "$DZ_KAFKA_PORT" Kafka 60
fi

## 4. Kafka Connect (단일 워커, distributed 모드)
if is_up "$DZ_CONNECT_PORT"; then
  echo "[connect] 이미 기동됨 (port $DZ_CONNECT_PORT)"
else
  render deploy/connect/connect-distributed.properties "$DZ_RT/conf/connect-distributed.properties"
  LOG_DIR="$DZ_LOG_DIR" nohup "$DZ_KAFKA_HOME/bin/connect-distributed.sh" \
    "$DZ_RT/conf/connect-distributed.properties" >"$DZ_LOG_DIR/connect.log" 2>&1 &
  echo $! > "$DZ_PID_DIR/connect.pid"
  wait_port "$DZ_CONNECT_PORT" "Kafka Connect" 90
fi

echo "인프라 기동 완료. 상태 확인: ./deploy/smoke-test.sh"
