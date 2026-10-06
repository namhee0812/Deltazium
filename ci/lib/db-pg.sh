# E2E DB 어댑터 — PostgreSQL (SRC_DB_TYPE=POSTGRESQL)
#
# ci/e2e-cdc.sh가 `source`한다. 본체는 DB 중립이고, "DB에 직접 psql을 치는 부분"만 이 파일에 있다.
# 다른 DB를 지원하려면 같은 함수 이름·계약으로 ci/lib/db-<type>.sh 를 만들면 된다.
#
# 어댑터 계약 (본체가 호출하는 함수):
#   db_check_ready       캡처 계정 존재 확인 — 실패 시 비0
#   db_prepare           소스·타깃 스키마/테이블 준비 + 초기 3행 — 실패 시 비0
#   db_dml               소스 DML (INSERT 3 + UPDATE 2 + DELETE 1) — 실패 시 비0
#   db_drop_replication  이 E2E가 남긴 복제 슬롯·publication 삭제 (cleanup_all의 (4))
#   db_drop_schemas      이 E2E의 테스트 스키마 삭제 (cleanup_all의 (6))
#   db_residual          잔여 객체를 "종류 이름" 줄로 출력 (residual_check가 모은다)
# 어댑터가 쓰는 본체 변수: SRC_*, E2E_PREFIX, SRC_SCHEMA, TGT_SCHEMA, TABLE, CLEAN_N(정리 건수 카운터)
# 어댑터가 쓰는 본체 함수: guard(삭제 가드), poll(대기)

PSQL="${DZ_PG_BIN}/psql"

# 스키마·데이터 준비/정리용 관리 계정. 캡처 계정(SRC_USER)은 CREATE SCHEMA 권한이 없을 수 있어
# deploy/env.sh의 관리 계정(DZ_PG_USER)으로 접속한다. 이 인스턴스의 pg_hba는 trust라 비밀번호가 없다.
# 필요하면 PGPASSWORD 환경변수를 넘길 것.
PG_ADMIN="${DZE2E_PG_ADMIN:-$DZ_PG_USER}"

# 소스 DB에 관리 계정으로 접속하는 psql (v1: 타깃 스키마도 같은 인스턴스·DB에 만든다)
psql_admin() { "$PSQL" -h "$SRC_HOST" -p "$SRC_PORT" -U "$PG_ADMIN" -d "$SRC_DB" -v ON_ERROR_STOP=1 -qtA "$@"; }

# 복제 슬롯이 비활성(active != t)인지 — 삭제 전 대기 조건
slot_inactive() { [ "$(psql_admin -c "SELECT active FROM pg_replication_slots WHERE slot_name='$CUR_SLOT'")" != t ]; }

db_check_ready() {
  [ "$(psql_admin -c "SELECT count(*) FROM pg_roles WHERE rolname='$SRC_USER'" 2>/dev/null)" = 1 ]
}

# 소스: 여러 타입 컬럼 + PK 테이블 + 초기 3행(스냅샷 검증용). 타깃은 스키마만 만든다 —
# 타깃 테이블은 등록 시 createTarget=true 로 backend가 소스 스키마를 읽어 생성한다.
# 테이블 소유자는 dz_capture(Debezium publication 생성에 필요한 소유권, pg-source-setup.sh 참고).
db_prepare() {
psql_admin <<SQL
CREATE SCHEMA "$SRC_SCHEMA" AUTHORIZATION $SRC_USER;
CREATE SCHEMA "$TGT_SCHEMA" AUTHORIZATION $SRC_USER;
CREATE TABLE "$SRC_SCHEMA".$TABLE (
  id         integer PRIMARY KEY,
  name       varchar(40) NOT NULL,
  amount     numeric(12,2),
  qty        bigint,
  memo       text,
  active     boolean,
  due_date   date,
  created_at timestamp(3)
);
ALTER TABLE "$SRC_SCHEMA".$TABLE OWNER TO $SRC_USER;
INSERT INTO "$SRC_SCHEMA".$TABLE VALUES
  (1,'alpha',10.50,100,'first',true,'2026-01-01','2026-01-01 10:00:00.123'),
  (2,'beta',20.00,200,NULL,false,'2026-02-02','2026-02-02 11:00:00.456'),
  (3,'gamma',30.75,300,'한글 메모',true,NULL,NULL);
SQL
}

# INSERT 3 + UPDATE 2 + DELETE 1 = 6개 변경 이벤트(스냅샷 3건은 op=r로 별도).
db_dml() {
psql_admin <<SQL
INSERT INTO "$SRC_SCHEMA".$TABLE VALUES
  (4,'delta',40.00,400,'ins4',true,'2026-04-04','2026-04-04 12:00:00.789'),
  (5,'epsilon',50.25,500,NULL,false,NULL,'2026-05-05 13:00:00.000'),
  (6,'zeta',60.00,600,'to-delete',true,'2026-06-06',NULL);
UPDATE "$SRC_SCHEMA".$TABLE SET amount = 99.99, memo = 'updated' WHERE id = 1;
UPDATE "$SRC_SCHEMA".$TABLE SET active = NOT active, qty = qty + 1 WHERE id = 2;
DELETE FROM "$SRC_SCHEMA".$TABLE WHERE id = 6;
SQL
}

# PG 복제 슬롯·publication — unregister가 지우는 것이 정상 경로. 남았다면(예: 등록 전 실패)
# 여기서 제거한다. 슬롯 누수는 WAL을 무한 적재시키므로 반드시 확인한다. active면 잠시 대기 후 재시도.
db_drop_replication() {
  local slot pub
  for slot in $(psql_admin -c "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'dz\_${E2E_PREFIX}%'"); do
    guard "$slot"
    CUR_SLOT="$slot"; poll 30 "slot inactive" slot_inactive
    psql_admin -c "SELECT pg_drop_replication_slot('$slot')" >/dev/null && { echo "  복제 슬롯 삭제: $slot"; CLEAN_N=$((CLEAN_N + 1)); }
  done
  for pub in $(psql_admin -c "SELECT pubname FROM pg_publication WHERE pubname LIKE 'dz\_${E2E_PREFIX}%'"); do
    guard "$pub"
    psql_admin -c "DROP PUBLICATION IF EXISTS \"$pub\"" && { echo "  publication 삭제: $pub"; CLEAN_N=$((CLEAN_N + 1)); }
  done
}

# 테스트 스키마(소스·타깃) — dze2e 로 시작하는 스키마만, 테이블째 CASCADE
db_drop_schemas() {
  local schema
  for schema in $(psql_admin -c "SELECT nspname FROM pg_namespace WHERE nspname LIKE '${E2E_PREFIX}%'"); do
    guard "$schema"
    psql_admin -c "DROP SCHEMA \"$schema\" CASCADE" && { echo "  스키마 삭제: $schema"; CLEAN_N=$((CLEAN_N + 1)); }
  done
}

db_residual() {
  psql_admin -c "SELECT '슬롯 '||slot_name FROM pg_replication_slots WHERE slot_name LIKE '%${E2E_PREFIX}%'"
  psql_admin -c "SELECT 'publication '||pubname FROM pg_publication WHERE pubname LIKE '%${E2E_PREFIX}%'"
  psql_admin -c "SELECT '스키마 '||nspname FROM pg_namespace WHERE nspname LIKE '${E2E_PREFIX}%'"
}
