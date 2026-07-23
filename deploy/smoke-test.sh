#!/usr/bin/env bash
# Deltazium 전 컴포넌트 헬스체크. 전부 통과하면 exit 0.
set -uo pipefail
cd "$(dirname "$0")/.."
source deploy/env.sh

FAIL=0
check() { # name cmd...
  local name="$1"; shift
  if "$@" >/dev/null 2>&1; then echo "OK   $name"; else echo "FAIL $name"; FAIL=1; fi
}

check "PostgreSQL ($DZ_PG_PORT)"      "$DZ_PG_BIN/psql" -h localhost -p "$DZ_PG_PORT" -U "$DZ_PG_USER" -d "$DZ_PG_DB" -c "SELECT 1"
check "PostgreSQL iceberg_catalog"     "$DZ_PG_BIN/psql" -h localhost -p "$DZ_PG_PORT" -U "$DZ_PG_USER" -d iceberg_catalog -c "SELECT 1"
check "MinIO ($DZ_MINIO_PORT)"         curl -sf "http://localhost:$DZ_MINIO_PORT/minio/health/live"
check "MinIO bucket $DZ_MINIO_BUCKET"  "$DZ_RT/bin/mc" ls "dz/$DZ_MINIO_BUCKET"
check "Kafka ($DZ_KAFKA_PORT)"         "$DZ_KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server "localhost:$DZ_KAFKA_PORT"
check "Kafka Connect ($DZ_CONNECT_PORT)" curl -sf "http://localhost:$DZ_CONNECT_PORT/"

# Connect 플러그인 로드 확인 (source / jdbc-sink / iceberg-sink)
plugins=$(curl -sf "http://localhost:$DZ_CONNECT_PORT/connector-plugins" 2>/dev/null)
for p in "io.debezium.connector.oracle.OracleConnector" \
         "io.debezium.connector.jdbc.JdbcSinkConnector" \
         "org.apache.iceberg.connect.IcebergSinkConnector"; do
  if echo "$plugins" | grep -q "$p"; then echo "OK   plugin $p"; else echo "FAIL plugin $p"; FAIL=1; fi
done

exit $FAIL
