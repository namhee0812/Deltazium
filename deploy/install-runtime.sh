#!/usr/bin/env bash
# ~/deltazium-runtime 최초 구축: 바이너리 다운로드 + Connect 플러그인 설치. 멱등.
# (start-infra.sh 실행 전 1회 필요. 이미 설치된 항목은 건너뜀)
set -euo pipefail
cd "$(dirname "$0")/.."
source deploy/env.sh

KAFKA_VER=4.3.1
DEBEZIUM_VER=3.6.0.Final
ICEBERG_TAG=apache-iceberg-1.11.0
PG_DRIVER_VER=42.7.8

DL="$DZ_RT/downloads"
mkdir -p "$DL" "$DZ_RT/bin" "$DZ_PLUGIN_PATH" "$DZ_RT/minio-data"

fetch() { # url dst
  [ -f "$2" ] && { echo "[skip] $2"; return; }
  echo "[down] $1"
  curl -sSL -o "$2" "$1"
}

## Kafka
if [ ! -x "$DZ_RT/kafka_2.13-$KAFKA_VER/bin/kafka-server-start.sh" ]; then
  fetch "https://downloads.apache.org/kafka/$KAFKA_VER/kafka_2.13-$KAFKA_VER.tgz" "$DL/kafka_2.13-$KAFKA_VER.tgz"
  tar xzf "$DL/kafka_2.13-$KAFKA_VER.tgz" -C "$DZ_RT"
fi
ln -sfn "$DZ_RT/kafka_2.13-$KAFKA_VER" "$DZ_RT/kafka"

## MinIO + mc
[ -x "$DZ_RT/bin/minio" ] || { fetch https://dl.min.io/server/minio/release/linux-amd64/minio "$DZ_RT/bin/minio"; chmod +x "$DZ_RT/bin/minio"; }
[ -x "$DZ_RT/bin/mc" ]    || { fetch https://dl.min.io/client/mc/release/linux-amd64/mc "$DZ_RT/bin/mc"; chmod +x "$DZ_RT/bin/mc"; }

## Debezium 플러그인 (Oracle source에는 ojdbc11 동봉)
for c in oracle jdbc; do
  if [ ! -d "$DZ_PLUGIN_PATH/debezium-connector-$c" ]; then
    fetch "https://repo1.maven.org/maven2/io/debezium/debezium-connector-$c/$DEBEZIUM_VER/debezium-connector-$c-$DEBEZIUM_VER-plugin.tar.gz" \
      "$DL/debezium-connector-$c-$DEBEZIUM_VER-plugin.tar.gz"
    tar xzf "$DL/debezium-connector-$c-$DEBEZIUM_VER-plugin.tar.gz" -C "$DZ_PLUGIN_PATH"
  fi
done

## Iceberg sink — 공식 프리빌드 배포판이 없어 소스 빌드 (최초 1회 ~6분)
ICEBERG_PLUGIN=$(ls -d "$DZ_PLUGIN_PATH"/iceberg-kafka-connect-runtime-* 2>/dev/null | head -1 || true)
if [ -z "$ICEBERG_PLUGIN" ]; then
  if [ ! -d "$DZ_RT/iceberg-src" ]; then
    git clone --depth 1 --branch "$ICEBERG_TAG" https://github.com/apache/iceberg.git "$DZ_RT/iceberg-src"
  fi
  (cd "$DZ_RT/iceberg-src" && JAVA_HOME="$JAVA_HOME" ./gradlew -x test -x integrationTest \
    :iceberg-kafka-connect:iceberg-kafka-connect-runtime:build)
  unzip -q -o "$DZ_RT"/iceberg-src/kafka-connect/kafka-connect-runtime/build/distributions/iceberg-kafka-connect-runtime-*[0-9T].zip \
    -d "$DZ_PLUGIN_PATH"
  ICEBERG_PLUGIN=$(ls -d "$DZ_PLUGIN_PATH"/iceberg-kafka-connect-runtime-* | head -1)
fi

## Iceberg 배포판에는 JDBC 드라이버 미포함(공식 문서 명시) — JDBC 카탈로그용 PG 드라이버 추가
if ! ls "$ICEBERG_PLUGIN/lib/postgresql-"*.jar >/dev/null 2>&1; then
  fetch "https://repo1.maven.org/maven2/org/postgresql/postgresql/$PG_DRIVER_VER/postgresql-$PG_DRIVER_VER.jar" \
    "$ICEBERG_PLUGIN/lib/postgresql-$PG_DRIVER_VER.jar"
fi

echo "런타임 설치 완료: $DZ_RT"
