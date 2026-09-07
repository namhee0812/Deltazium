#!/usr/bin/env bash
#
# PostgreSQL을 두 번째 CDC 소스로 준비 — 다중 소스·다중 타깃 ② (docs/TODO.md, 2026-09-07).
# 기존 메타데이터·Iceberg 카탈로그용 PostgreSQL 인스턴스(env.sh DZ_PG_*)를 그대로 쓰고,
# 그 안에 새 스키마 cdc_src(+ 캡처 계정 dz_capture)를 마련해 Debezium PostgreSQL 소스가
# 캡처할 테스트 테이블 2개를 둔다.
#
# ⚠ 작성만 — 이 스크립트는 자동 실행되지 않는다. 다음을 반드시 먼저 확인할 것:
#   1. wal_level=logical 변경은 PostgreSQL **재시작**이 필요하다(postmaster-context 파라미터).
#      메타데이터·Iceberg 카탈로그를 같은 인스턴스가 서비스 중이므로, 재시작 시점을
#      backend·recovery-job이 조용한 시간으로 잡을 것 — 실행 중 등록/복구 작업과 겹치지 않게.
#   2. REPLICA IDENTITY FULL은 여기서 설정하지 않는다 — architecture.md 8절 승인 UX(등록
#      사전 점검에서 DDL을 보여주고 승인 후 적용, supplemental logging과 같은 방식)를 따른다.
#   3. dz_capture 비밀번호는 DZ_PG_CAPTURE_PASSWORD 환경변수로 넘길 것 (기본값은 토이 전용).
#
# 사용법 (사용자 확인 후 직접 실행):
#   source deploy/env.sh && ./deploy/pg-source-setup.sh
#
set -euo pipefail
cd "$(dirname "$0")/.."
source deploy/env.sh

PSQL="$DZ_PG_BIN/psql"
PGHOST=localhost
PGPORT="$DZ_PG_PORT"
ADMIN_USER="${DZ_PG_ADMIN_USER:-$DZ_PG_USER}"   # 스키마·롤 생성 권한이 있는 계정
ADMIN_DB="$DZ_PG_DB"
CAPTURE_ROLE=dz_capture
CAPTURE_PASSWORD="${DZ_PG_CAPTURE_PASSWORD:-dz_capture_pw}"

run() { # sql [psql 추가 옵션...] — 첫 인자가 SQL, 나머지(-tA 등)는 psql에 그대로 전달
  local sql="$1"; shift
  "$PSQL" -h "$PGHOST" -p "$PGPORT" -U "$ADMIN_USER" -d "$ADMIN_DB" -v ON_ERROR_STOP=1 "$@" -c "$sql"
}

echo "=== 1. wal_level 확인 ==="
CURRENT_WAL_LEVEL=$(run "SHOW wal_level;" -tA | tail -1)
if [ "$CURRENT_WAL_LEVEL" != "logical" ]; then
  echo "[안내] 현재 wal_level=$CURRENT_WAL_LEVEL — logical로 바꾼다(ALTER SYSTEM, 적용엔 재시작 필요)."
  run "ALTER SYSTEM SET wal_level = 'logical';"
  echo "[중요] postgresql.conf(자동 갱신됨) 적용을 위해 PostgreSQL을 재시작해야 한다:"
  echo "  \"$DZ_PG_BIN/pg_ctl\" -D \"$DZ_PG_DATA\" restart"
  echo "  재시작 전까지 wal_level은 그대로다 — 이 스크립트를 재실행해 확인할 것."
else
  echo "[ok] wal_level=logical (변경 불필요)"
fi

echo "=== 2. 캡처 롤 $CAPTURE_ROLE (REPLICATION + LOGIN) ==="
ROLE_EXISTS=$(run "SELECT 1 FROM pg_roles WHERE rolname = '$CAPTURE_ROLE';" -tA)
if [ -z "$ROLE_EXISTS" ]; then
  run "CREATE ROLE $CAPTURE_ROLE LOGIN REPLICATION PASSWORD '$CAPTURE_PASSWORD';"
  echo "[생성] $CAPTURE_ROLE"
else
  echo "[ok] $CAPTURE_ROLE 이미 존재"
fi

echo "=== 3. 테스트 스키마 cdc_src + PK 테이블 2개 ==="
run "CREATE SCHEMA IF NOT EXISTS cdc_src AUTHORIZATION $CAPTURE_ROLE;"
run "CREATE TABLE IF NOT EXISTS cdc_src.test_table_01 (
       id          INTEGER PRIMARY KEY,
       status      VARCHAR(40),
       updated_at  TIMESTAMP DEFAULT now()
     );"
run "CREATE TABLE IF NOT EXISTS cdc_src.test_table_02 (
       id          INTEGER PRIMARY KEY,
       amount      NUMERIC(12,2),
       memo        TEXT
     );"
run "ALTER TABLE cdc_src.test_table_01 OWNER TO $CAPTURE_ROLE;"
run "ALTER TABLE cdc_src.test_table_02 OWNER TO $CAPTURE_ROLE;"
# $CAPTURE_ROLE이 스키마·테이블 소유자라 publication.autocreate.mode=filtered(테이블을 publication에
# 추가)에 필요한 권한을 스스로 갖는다 — 별도 GRANT 불필요 (connectors/README.md 근거 참고).

echo
echo "=== 완료 — 다음 값으로 DB 연결(SOURCE·POSTGRESQL)을 등록할 것 ==="
echo "  host=$PGHOST port=$PGPORT database=$ADMIN_DB user=$CAPTURE_ROLE"
echo "  등록 패턴 예: cdc_src.*"
