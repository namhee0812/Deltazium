#!/usr/bin/env bash
#
# CDC E2E 테스트 — PostgreSQL 소스 → Kafka → PostgreSQL 타깃(+ Iceberg changelog) 전 구간을
# 실제 기동 중인 backend(8090)·Connect·Kafka·PG(5433)에 대해 한 번 돌려 본다 (v1: PG→PG).
#
# 사용:
#   ./deploy/e2e-cdc.sh                # E2E 실행. 성공 시 만든 객체를 정리하고, 실패 시 보존한다
#   ./deploy/e2e-cdc.sh --cleanup-only # 이전 실행이 남긴 dze2e* 객체만 전부 정리
#
# 종료 코드: 0 = 전 단계 PASS, 1 = 실패(원인은 [FAIL] 줄 + 마지막 응답 본문)
#
# ── 정리 정책 (사용자 승인된 "데이터 삭제" 예외) ───────────────────────────────────────
#  * 자동 삭제 대상은 E2E 전용 prefix `dze2e` 가 붙은 객체뿐이다: 등록·연결·커넥터·Kafka 토픽·
#    복제 슬롯/publication·Iceberg changelog·테스트 스키마. 삭제 직전에 guard()가 이름을 검사하고,
#    prefix가 아니면 삭제하지 않고 FAIL 한다. 기존 연결·cdc_src·cdc_tmp 등은 절대 건드리지 않는다.
#  * 실패 시에는 정리를 건너뛴다 — 원인 분석용 상태 보존. 나중에 --cleanup-only 로 치운다.
#
# ── 의존: curl, jq, psql(deploy/env.sh의 DZ_PG_BIN) ─────────────────────────────────────

set -uo pipefail
cd "$(dirname "$0")/.."
# shellcheck disable=SC1091
source deploy/env.sh

# ─────────────────────────────────────────────────────────────────────────────
# 0. 설정 — 전부 환경변수로 오버라이드 가능. 비밀번호는 리포에 두지 않는다.
# ─────────────────────────────────────────────────────────────────────────────
API="${DZE2E_API:-http://localhost:8090}"
PG_HOST="${DZE2E_PG_HOST:-127.0.0.1}"
PG_PORT="${DZE2E_PG_PORT:-$DZ_PG_PORT}"
PG_DB="${DZE2E_PG_DB:-$DZ_PG_DB}"
# psql(스키마·데이터 준비/정리)는 관리 계정(deploy/env.sh DZ_PG_USER)으로 접속한다.
# 이 인스턴스의 pg_hba는 trust라 비밀번호가 없다. 필요하면 PGPASSWORD 환경변수를 넘길 것.
PG_ADMIN="${DZE2E_PG_ADMIN:-$DZ_PG_USER}"
# backend에 등록하는 연결(Debezium 캡처·JDBC sink 겸용)은 pg-source-setup.sh가 만든 dz_capture 롤.
# 비밀번호는 pg-source-setup.sh와 같은 DZ_PG_CAPTURE_PASSWORD 환경변수(없으면 빈 값 = trust 인증).
CAP_USER="${DZE2E_PG_USER:-dz_capture}"
CAP_PASSWORD="${DZE2E_PG_PASSWORD:-${DZ_PG_CAPTURE_PASSWORD:-}}"
PSQL="${DZ_PG_BIN}/psql"
KAFKA_TOPICS="${DZ_KAFKA_HOME}/bin/kafka-topics.sh"
KAFKA_GROUPS="${DZ_KAFKA_HOME}/bin/kafka-consumer-groups.sh"
KAFKA_BOOTSTRAP="localhost:${DZ_KAFKA_PORT}"

E2E_PREFIX="dze2e"        # 모든 E2E 객체의 이름 접두 — 정리 가드의 기준
# timeout(초): 단계별로 따로 둔다. changelog는 Iceberg sink commit 주기(60s)가 있어 길다.
# (환경변수 DZE2E_T_<이름>으로 오버라이드 — 실패 보존 경로를 시험할 때 짧게 줄여 쓴다)
T_CONNECTOR="${DZE2E_T_CONNECTOR:-120}"; T_SNAPSHOT="${DZE2E_T_SNAPSHOT:-120}"; T_APPLY="${DZE2E_T_APPLY:-120}"
T_VERIFY="${DZE2E_T_VERIFY:-60}"; T_CHANGELOG="${DZE2E_T_CHANGELOG:-240}"

# ─────────────────────────────────────────────────────────────────────────────
# 공통 함수 — 출력·판정·HTTP·psql·대기
# ─────────────────────────────────────────────────────────────────────────────
RESULTS=()                # 최종 요약용 "PASS|FAIL 단계 (N초)"
STEP_NO=0
STEP_START=$SECONDS
LAST_BODY=""              # 마지막 API 응답 본문 (실패 시 출력)
TMP="$(mktemp)"; trap 'rm -f "$TMP"' EXIT

pass() { RESULTS+=("PASS  $1 ($((SECONDS - STEP_START))s)"); echo "[PASS] $1 ($((SECONDS - STEP_START))s)${2:+ — $2}"; }
fail() {
  RESULTS+=("FAIL  $1 ($((SECONDS - STEP_START))s)"); echo "[FAIL] $1 — $2"
  [ -n "$LAST_BODY" ] && echo "       마지막 응답: ${LAST_BODY:0:600}"
  summary_and_exit 1
}
step() { STEP_NO=$((STEP_NO + 1)); STEP_START=$SECONDS; CUR_STEP="$STEP_NO. $1"; }

summary_and_exit() {
  echo; echo "=== E2E 요약 (총 ${SECONDS}s) ==="
  printf '  %s\n' "${RESULTS[@]}"
  if [ "$1" -ne 0 ]; then
    echo "결과: FAIL — 실패 시점 상태를 보존했다(원인 분석용). 치우려면: ./deploy/e2e-cdc.sh --cleanup-only"
    [ -n "${RUN_ID:-}" ] && echo "      이번 실행 식별자: ${E2E_PREFIX}${RUN_ID}"
  else
    echo "결과: PASS"
  fi
  exit "$1"
}

# 삭제 가드 — E2E prefix가 아닌 이름은 절대 지우지 않는다.
# 허용 형태: dze2e..., dz_dze2e...(복제 슬롯·publication), changelog_dze2e...(Iceberg namespace),
#           control-iceberg-dze2e...(Iceberg control 토픽), dz-<종류>-dze2e...(커넥터),
#           connect-dz-<종류>-dze2e...(Connect consumer group)
guard() {
  if [[ ! "$1" =~ ^(dz_|changelog_|control-iceberg-|connect-|connect-dz-[a-z-]+-|dz-[a-z-]+-|dz-recovery\.)?${E2E_PREFIX}[a-z0-9_.-]*$ ]]; then
    echo "[FAIL] 삭제 가드 — E2E prefix(${E2E_PREFIX})가 아닌 이름은 삭제하지 않는다: '$1'"
    summary_and_exit 1
  fi
}

# api METHOD PATH [JSON] — 2xx면 본문을 LAST_BODY에 담고 0, 아니면 1.
api() {
  local method="$1" path="$2" data="${3:-}" code
  if [ -n "$data" ]; then
    code=$(curl -sS -o "$TMP" -w '%{http_code}' -X "$method" -H 'Content-Type: application/json' \
      --max-time 60 -d "$data" "$API$path" 2>&1) || { LAST_BODY="curl 실패: $code"; return 1; }
  else
    code=$(curl -sS -o "$TMP" -w '%{http_code}' -X "$method" --max-time 60 "$API$path" 2>&1) \
      || { LAST_BODY="curl 실패: $code"; return 1; }
  fi
  LAST_BODY="$(cat "$TMP")"
  [[ "$code" =~ ^2 ]] || { LAST_BODY="HTTP $code $LAST_BODY"; return 1; }
}

# Iceberg JDBC 카탈로그 DB (env.sh 기준 iceberg_catalog, smoke-test.sh와 동일)
psql_cat() { "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -U "$PG_ADMIN" -d "${DZE2E_CATALOG_DB:-iceberg_catalog}" -v ON_ERROR_STOP=1 -qtA "$@"; }
psql_admin() { "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -U "$PG_ADMIN" -d "$PG_DB" -v ON_ERROR_STOP=1 -qtA "$@"; }

# poll TIMEOUT_SEC DESC CMD... — CMD가 0을 돌려줄 때까지 2초 간격 polling. 고정 sleep 대기 금지(타임아웃만 있다).
poll() {
  local timeout="$1" desc="$2"; shift 2
  local deadline=$((SECONDS + timeout))
  while ! "$@"; do
    [ "$SECONDS" -ge "$deadline" ] && { POLL_TIMED_OUT=1; return 1; }
    sleep 2
  done
  return 0
}

# ─────────────────────────────────────────────────────────────────────────────
# 정리 — E2E prefix 객체만. 실행 성공 후와 --cleanup-only 가 같은 함수를 쓴다.
# 상태 파일 없이 "prefix로 시작하는 것을 찾아 지우는" 방식이라 어느 실행의 잔재든 치운다.
# ─────────────────────────────────────────────────────────────────────────────
slot_inactive() { [ "$(psql_admin -c "SELECT active FROM pg_replication_slots WHERE slot_name='$CUR_SLOT'")" != t ]; }

cleanup_all() {
  local n=0 id name

  # (1) 등록 해제 — 백엔드가 jdbc-sink·source·iceberg-sink 커넥터, offset, PG 슬롯·publication,
  #     (dropChangelog=true) Iceberg changelog 테이블을 함께 정리한다 (RegistrationService.unregister).
  while read -r id name; do
    [ -n "$id" ] || continue
    guard "$name"
    api DELETE "/api/registrations/$id?dropChangelog=true" || { echo "  [WARN] 등록 해제 실패 id=$id: $LAST_BODY"; }
    echo "  등록 해제: id=$id ($name)"; n=$((n + 1))
  done < <(curl -sf "$API/api/registrations" | jq -r --arg p "$E2E_PREFIX" \
      '.[] | select(.sourceTopicPrefix|startswith($p)) | "\(.id) \(.sourceTopicPrefix)"')

  # (2) 커넥터 잔재 — 등록 해제가 대부분 지우지만, 실패 상태 등에서 남은 dze2e 커넥터를 직접 제거
  for name in $(curl -sf "$API/api/connectors" | jq -r 'keys[]' 2>/dev/null | grep -E "${E2E_PREFIX}" || true); do
    guard "$name"
    api DELETE "/api/connectors/$name" && { echo "  커넥터 삭제: $name"; n=$((n + 1)); }
  done

  # (3) 연결(소스·타깃)
  for id in $(curl -sf "$API/api/connections" | jq -r --arg p "$E2E_PREFIX" '.[] | select(.name|startswith($p)) | .id'); do
    name=$(curl -sf "$API/api/connections/$id" | jq -r .name)
    guard "$name"
    api DELETE "/api/connections/$id" && { echo "  연결 삭제: id=$id ($name)"; n=$((n + 1)); }
  done

  # (4) PG 복제 슬롯·publication — unregister가 지우는 것이 정상 경로. 남았다면(예: 등록 전 실패)
  #     여기서 제거한다. 슬롯 누수는 WAL을 무한 적재시키므로 반드시 확인한다. active면 잠시 대기 후 재시도.
  local slot pub
  for slot in $(psql_admin -c "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'dz\_${E2E_PREFIX}%'"); do
    guard "$slot"
    CUR_SLOT="$slot"; poll 30 "slot inactive" slot_inactive
    psql_admin -c "SELECT pg_drop_replication_slot('$slot')" >/dev/null && { echo "  복제 슬롯 삭제: $slot"; n=$((n + 1)); }
  done
  for pub in $(psql_admin -c "SELECT pubname FROM pg_publication WHERE pubname LIKE 'dz\_${E2E_PREFIX}%'"); do
    guard "$pub"
    psql_admin -c "DROP PUBLICATION IF EXISTS \"$pub\"" && { echo "  publication 삭제: $pub"; n=$((n + 1)); }
  done

  # (5) Kafka 토픽 — backend는 토픽을 지우지 않는다(캡처·notification·control·schema-history 등).
  local topic
  if [ -x "$KAFKA_TOPICS" ]; then
    for topic in $("$KAFKA_TOPICS" --bootstrap-server "$KAFKA_BOOTSTRAP" --list 2>/dev/null \
        | grep -E "^(${E2E_PREFIX}|control-iceberg-${E2E_PREFIX}|dz-recovery\.${E2E_PREFIX}|__debezium-heartbeat\.${E2E_PREFIX})" || true); do
      guard "${topic#__debezium-heartbeat.}"
      "$KAFKA_TOPICS" --bootstrap-server "$KAFKA_BOOTSTRAP" --delete --topic "$topic" >/dev/null 2>&1 \
        && { echo "  Kafka 토픽 삭제: $topic"; n=$((n + 1)); }
    done
  fi

  # (5b) Connect consumer group — 커넥터를 지워도 그룹 메타데이터는 offsets.retention까지 남는다
  local grp
  if [ -x "$KAFKA_GROUPS" ]; then
    for grp in $("$KAFKA_GROUPS" --bootstrap-server "$KAFKA_BOOTSTRAP" --list 2>/dev/null | grep -E "^connect-dz-.*${E2E_PREFIX}" || true); do
      guard "$grp"
      "$KAFKA_GROUPS" --bootstrap-server "$KAFKA_BOOTSTRAP" --delete --group "$grp" >/dev/null 2>&1 \
        && { echo "  consumer group 삭제: $grp"; n=$((n + 1)); }
    done
  fi

  # (5c) Iceberg namespace — backend의 dropChangelog는 changelog "테이블"만 지우고 namespace(빈 껍데기)는
  #      남긴다(DROP NAMESPACE API 없음). Iceberg JDBC 카탈로그의 namespace 행만, 그 안에 테이블이 0개일 때
  #      지운다(테이블이 있으면 건드리지 않음 → 실데이터 보호).
  local ns
  for ns in $(psql_cat -c "SELECT DISTINCT namespace FROM iceberg_namespace_properties WHERE namespace LIKE 'changelog\_${E2E_PREFIX}%'"); do
    guard "$ns"
    if [ "$(psql_cat -c "SELECT count(*) FROM iceberg_tables WHERE table_namespace='$ns'")" = 0 ]; then
      psql_cat -c "DELETE FROM iceberg_namespace_properties WHERE namespace='$ns'" >/dev/null \
        && { echo "  Iceberg namespace 삭제: $ns"; n=$((n + 1)); }
    else
      echo "  [WARN] namespace $ns 에 테이블이 남아 있어 삭제하지 않음"
    fi
  done

  # (6) 테스트 스키마(소스·타깃) — dze2e 로 시작하는 스키마만, 테이블째 CASCADE
  local schema
  for schema in $(psql_admin -c "SELECT nspname FROM pg_namespace WHERE nspname LIKE '${E2E_PREFIX}%'"); do
    guard "$schema"
    psql_admin -c "DROP SCHEMA \"$schema\" CASCADE" && { echo "  스키마 삭제: $schema"; n=$((n + 1)); }
  done
  echo "  정리 완료 — ${n}건"
}

# 잔여 검사 — 정리 후 dze2e 객체가 하나도 없는지 (연결·등록·커넥터·토픽·슬롯·publication·스키마·Iceberg namespace)
residual_check() {
  local left=0 out
  out=$(curl -sf "$API/api/connections" | jq -r --arg p "$E2E_PREFIX" '.[]|select(.name|startswith($p))|"연결 "+.name')
  out+=$'\n'$(curl -sf "$API/api/registrations" | jq -r --arg p "$E2E_PREFIX" '.[]|select(.sourceTopicPrefix|startswith($p))|"등록 "+.sourceTopicPrefix')
  out+=$'\n'$(curl -sf "$API/api/connectors" | jq -r 'keys[]' 2>/dev/null | grep "${E2E_PREFIX}" | sed 's/^/커넥터 /')
  out+=$'\n'$(psql_admin -c "SELECT '슬롯 '||slot_name FROM pg_replication_slots WHERE slot_name LIKE '%${E2E_PREFIX}%'")
  out+=$'\n'$(psql_admin -c "SELECT 'publication '||pubname FROM pg_publication WHERE pubname LIKE '%${E2E_PREFIX}%'")
  out+=$'\n'$(psql_admin -c "SELECT '스키마 '||nspname FROM pg_namespace WHERE nspname LIKE '${E2E_PREFIX}%'")
  out+=$'\n'$("$KAFKA_TOPICS" --bootstrap-server "$KAFKA_BOOTSTRAP" --list 2>/dev/null | grep "${E2E_PREFIX}" | sed 's/^/Kafka 토픽 /')
  out+=$'\n'$(psql_cat -c "SELECT 'Iceberg namespace '||namespace FROM iceberg_namespace_properties WHERE namespace LIKE '%${E2E_PREFIX}%' GROUP BY namespace")
  out+=$'\n'$("$KAFKA_GROUPS" --bootstrap-server "$KAFKA_BOOTSTRAP" --list 2>/dev/null | grep "${E2E_PREFIX}" | sed 's/^/consumer group /')
  out+=$'\n'$(curl -sf "$API/api/changelog" | jq -r '.[]|select(.table|contains("'"$E2E_PREFIX"'"))|"changelog "+.table')
  out=$(echo "$out" | sed '/^$/d')
  if [ -n "$out" ]; then echo "$out" | sed 's/^/  [잔여] /'; return 1; fi
  echo "  잔여 객체 없음"
}

# ─────────────────────────────────────────────────────────────────────────────
# --cleanup-only 모드
# ─────────────────────────────────────────────────────────────────────────────
need_tools() {
  local t miss=()
  for t in curl jq; do command -v "$t" >/dev/null || miss+=("$t"); done
  [ -x "$PSQL" ] || miss+=("psql($PSQL)")
  if [ "${#miss[@]}" -gt 0 ]; then
    echo "[FAIL] 필요한 도구가 없다: ${miss[*]} — 설치/경로 확인 후 다시 실행 (psql 경로는 deploy/env.sh의 DZ_PG_BIN)"
    exit 1
  fi
}

if [ "${1:-}" = "--cleanup-only" ]; then
  need_tools
  echo "=== E2E 잔재 정리 (prefix: ${E2E_PREFIX}) ==="
  cleanup_all
  residual_check || { echo "[FAIL] 일부 객체가 남았다 — 위 목록 확인"; exit 1; }
  exit 0
fi
[ $# -eq 0 ] || { echo "사용: $0 [--cleanup-only]"; exit 2; }

# ═════════════════════════════════════════════════════════════════════════════
# E2E 본 흐름
# ═════════════════════════════════════════════════════════════════════════════
# 실행마다 고유한 식별자 — 연결 이름이 곧 topic_prefix 슬러그가 된다(DbConnectionService.slug:
# 소문자·영숫자 외 '_' 치환). 그래서 이름을 처음부터 슬러그 규칙에 맞는 dze2e<MMDDhhmmss>로 만든다.
RUN_ID="$(date +%m%d%H%M%S)"
PREFIX="${E2E_PREFIX}${RUN_ID}"          # 소스 연결 이름 = topic_prefix = 커넥터·슬롯·namespace 접두
SRC_SCHEMA="${PREFIX}_src"; TGT_SCHEMA="${PREFIX}_tgt"; TABLE="t_orders"
SRC_NAME="$PREFIX"; TGT_NAME="${PREFIX}_tgt"
TOPIC="${PREFIX}.${SRC_SCHEMA}.${TABLE}"
CHANGELOG_TABLE="changelog_${PREFIX}.${SRC_SCHEMA}_${TABLE}"
echo "=== CDC E2E 시작 — 식별자 ${PREFIX} ==="

# ── 1. 사전 확인 ─────────────────────────────────────────────────────────────
# 도구·backend·dz_capture 롤이 없으면 이후 단계가 의미 없으므로 즉시 FAIL.
step "사전 확인"
need_tools
api GET /api/connections || fail "$CUR_STEP" "backend($API) 응답 없음 — ./deploy/dzadmin status 확인"
[ "$(psql_admin -c "SELECT count(*) FROM pg_roles WHERE rolname='$CAP_USER'" 2>/dev/null)" = 1 ] \
  || fail "$CUR_STEP" "PG 롤 $CAP_USER 없음 — deploy/pg-source-setup.sh 선행 필요"
pass "$CUR_STEP" "backend·PG·도구 OK"

# ── 2. 소스·타깃 스키마/테이블 준비 (psql) ────────────────────────────────────
# 소스: 여러 타입 컬럼 + PK 테이블 + 초기 3행(스냅샷 검증용). 타깃은 스키마만 만든다 —
# 타깃 테이블은 5단계 등록 시 createTarget=true 로 backend가 소스 스키마를 읽어 생성한다.
# 테이블 소유자는 dz_capture(Debezium publication 생성에 필요한 소유권, pg-source-setup.sh 참고).
step "스키마·테이블 준비"
psql_admin <<SQL || fail "$CUR_STEP" "psql 준비 실패"
CREATE SCHEMA "$SRC_SCHEMA" AUTHORIZATION $CAP_USER;
CREATE SCHEMA "$TGT_SCHEMA" AUTHORIZATION $CAP_USER;
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
ALTER TABLE "$SRC_SCHEMA".$TABLE OWNER TO $CAP_USER;
INSERT INTO "$SRC_SCHEMA".$TABLE VALUES
  (1,'alpha',10.50,100,'first',true,'2026-01-01','2026-01-01 10:00:00.123'),
  (2,'beta',20.00,200,NULL,false,'2026-02-02','2026-02-02 11:00:00.456'),
  (3,'gamma',30.75,300,'한글 메모',true,NULL,NULL);
SQL
pass "$CUR_STEP" "$SRC_SCHEMA.$TABLE 초기 3행"
SNAPSHOT_ROWS=3

# ── 3. 연결 등록 ─────────────────────────────────────────────────────────────
# 소스·타깃 연결을 backend에 등록. 둘 다 같은 PG 인스턴스(동종이라 체크섬 비교 가능).
step "연결 등록"
conn_json() { # name role
  jq -n --arg n "$1" --arg r "$2" --arg h "$PG_HOST" --argjson p "$PG_PORT" --arg d "$PG_DB" \
        --arg u "$CAP_USER" --arg pw "$CAP_PASSWORD" \
    '{name:$n,dbType:"POSTGRESQL",role:$r,host:$h,port:$p,databaseName:$d,username:$u,password:$pw}'
}
api POST /api/connections "$(conn_json "$SRC_NAME" SOURCE)" || fail "$CUR_STEP" "소스 연결 등록 실패"
SRC_ID=$(echo "$LAST_BODY" | jq -r .id)
GOT_PREFIX=$(echo "$LAST_BODY" | jq -r .topicPrefix)
[ "$GOT_PREFIX" = "$PREFIX" ] || fail "$CUR_STEP" "topicPrefix가 기대($PREFIX)와 다름: $GOT_PREFIX"
api POST /api/connections "$(conn_json "$TGT_NAME" TARGET)" || fail "$CUR_STEP" "타깃 연결 등록 실패"
TGT_ID=$(echo "$LAST_BODY" | jq -r .id)
pass "$CUR_STEP" "source id=$SRC_ID(prefix=$GOT_PREFIX) target id=$TGT_ID"

# ── 4. 사전 점검 ─────────────────────────────────────────────────────────────
# DB 레벨(wal_level)·캡처 계정 권한. blocking 항목이 ok=false면 FAIL(등록 불가), 비차단은 경고만.
step "사전 점검"
for chk in db-checks privilege-checks; do
  api GET "/api/registrations/$chk/$SRC_ID" || fail "$CUR_STEP" "$chk 호출 실패"
  bad=$(echo "$LAST_BODY" | jq -r '.[]|select(.ok==false and .blocking==true)|.label+": "+.detail')
  [ -z "$bad" ] || fail "$CUR_STEP" "$chk 실패 항목 — $bad"
  warn=$(echo "$LAST_BODY" | jq -r '.[]|select(.ok==false and .blocking==false)|.label')
  [ -z "$warn" ] || echo "       [경고] $chk 비차단 항목 미충족: $warn"
done
# 캡처 사전조건(REPLICA IDENTITY FULL) 적용 — 사용자 승인 UX의 API 단계(preview→apply)를 그대로 호출한다.
# 이걸 건너뛰면 5단계가 "캡처 사전조건 미충족"으로 거부된다.
TABLES_REQ=$(jq -n --argjson s "$SRC_ID" --arg t "$SRC_SCHEMA.$TABLE" '{sourceConnectionId:$s,tables:[$t]}')
api POST /api/registrations/capture-setup/preview "$TABLES_REQ" || fail "$CUR_STEP" "capture-setup preview 실패"
api POST /api/registrations/capture-setup/apply "$TABLES_REQ" || fail "$CUR_STEP" "capture-setup apply 실패"
pass "$CUR_STEP" "db-checks·privilege-checks 통과, REPLICA IDENTITY 적용"

# ── 5. 테이블 등록 (스냅샷 INITIAL) ───────────────────────────────────────────
# createTarget=true → 타깃 테이블 자동 생성(RegistrationService.createTargetTable, 이미 있으면 거부).
# 등록과 동시에 source·iceberg-sink·jdbc-sink 커넥터가 배포된다(deployConnectors).
step "테이블 등록"
REG_REQ=$(jq -n --argjson s "$SRC_ID" --argjson t "$TGT_ID" --arg st "$SRC_SCHEMA.$TABLE" \
                --arg ts "$TGT_SCHEMA" --arg tt "$TABLE" \
  '{sourceConnectionId:$s,targetConnectionId:$t,snapshotMode:"INITIAL",
    tables:[{source:$st,targetSchema:$ts,targetTable:$tt,createTarget:true,ddlPolicy:"MANUAL"}]}')
api POST /api/registrations "$REG_REQ" || fail "$CUR_STEP" "등록 실패"
REG_ID=$(echo "$LAST_BODY" | jq -r --argjson s "$SRC_ID" '[.[]|select(.sourceConnectionId==$s)][0].id')
[ -n "$REG_ID" ] && [ "$REG_ID" != null ] || fail "$CUR_STEP" "등록 응답에서 id를 못 찾음"
pass "$CUR_STEP" "registeredTableId=$REG_ID"

# ── 6. 커넥터 RUNNING → 스냅샷 완료 대기 ──────────────────────────────────────
# 커넥터 이름 규칙은 ConnectorNames(dz-source-/dz-iceberg-/dz-jdbc-sink-<prefix>-<schema_table>).
# FAILED 상태면 timeout까지 기다리지 않고 즉시 FAIL.
step "커넥터 RUNNING 대기"
SUFFIX="$(echo "${SRC_SCHEMA}_${TABLE}" | tr 'A-Z' 'a-z')"
CONNECTORS=("dz-source-$PREFIX" "dz-iceberg-$PREFIX" "dz-jdbc-sink-$PREFIX-$SUFFIX")
connectors_running() {
  local c st
  for c in "${CONNECTORS[@]}"; do
    api GET "/api/connectors/$c/status" || return 1
    st=$(echo "$LAST_BODY" | jq -r '[.connector.state, (.tasks[]?.state)] | join(",")')
    [[ "$st" == *FAILED* ]] && { CONN_FAILED="$c: $st"; return 0; }   # 즉시 중단 신호
    # connector 상태 + task 1개 이상이 전부 RUNNING 이어야 통과 (예: "RUNNING,RUNNING")
    [[ "$st" =~ ^RUNNING(,RUNNING)+$ ]] || return 1
  done
}
CONN_FAILED=""
poll "$T_CONNECTOR" "connectors" connectors_running || fail "$CUR_STEP" "${T_CONNECTOR}s 내 RUNNING 안 됨"
[ -z "$CONN_FAILED" ] || fail "$CUR_STEP" "커넥터 FAILED — $CONN_FAILED"
pass "$CUR_STEP" "${CONNECTORS[*]}"

# 스냅샷 완료 판정 — 1순위는 Debezium notification 기반 /api/capture/snapshot 의 소스별
# bySource[prefix].phase == COMPLETED (전체 phase가 아니라 소스별 값을 봐야 다른 소스와 섞이지 않는다).
# 단, SnapshotNotificationPoller는 backend 기동 시점의 SOURCE 연결 토픽만 구독한다("새 소스는
# backend 재기동 후 반영" — 코드 주석). 이 E2E는 실행 중에 소스 연결을 새로 만들므로 bySource에
# 이 prefix가 나타나지 않을 수 있다. 그 경우의 보조 판정: 타깃 행수·체크섬이 초기 행수(3)와 일치
# (= 스냅샷 데이터가 타깃에 반영됨). 어느 경로로 통과했는지 PASS 메시지에 남긴다.
# → 결정 필요: poller의 동적 구독(재기동 없이 새 소스 반영) 여부 — docs/TODO.md 참조.
step "스냅샷 완료 대기"
SNAP_VIA=""
snapshot_done() {
  if api GET /api/capture/snapshot && \
     [ "$(echo "$LAST_BODY" | jq -r --arg p "$PREFIX" '.bySource[$p].phase // "NONE"')" = COMPLETED ]; then
    SNAP_VIA="notification bySource[$PREFIX].phase=COMPLETED"; return 0
  fi
  api POST /api/recovery/verify "{\"registeredTableId\":$REG_ID}" || return 1
  if echo "$LAST_BODY" | jq -e --argjson n "$SNAPSHOT_ROWS" \
       '.targetCount==$n and .sourceCount==$n and .checksumSupported==true and .sourceChecksum==.targetChecksum' >/dev/null; then
    SNAP_VIA="notification 미관측(poller는 기동 시점 소스만 구독) → 타깃 행수·체크섬이 초기 ${SNAPSHOT_ROWS}행과 일치"; return 0
  fi
  return 1
}
poll "$T_SNAPSHOT" snapshot snapshot_done || fail "$CUR_STEP" "${T_SNAPSHOT}s 내 스냅샷 완료 신호 없음"
pass "$CUR_STEP" "$SNAP_VIA"

# ── 7. 소스 DML ──────────────────────────────────────────────────────────────
# INSERT 3 + UPDATE 2 + DELETE 1 = 6개 변경 이벤트(스냅샷 3건은 op=r로 별도).
step "소스 DML"
psql_admin <<SQL || fail "$CUR_STEP" "소스 DML 실패"
INSERT INTO "$SRC_SCHEMA".$TABLE VALUES
  (4,'delta',40.00,400,'ins4',true,'2026-04-04','2026-04-04 12:00:00.789'),
  (5,'epsilon',50.25,500,NULL,false,NULL,'2026-05-05 13:00:00.000'),
  (6,'zeta',60.00,600,'to-delete',true,'2026-06-06',NULL);
UPDATE "$SRC_SCHEMA".$TABLE SET amount = 99.99, memo = 'updated' WHERE id = 1;
UPDATE "$SRC_SCHEMA".$TABLE SET active = NOT active, qty = qty + 1 WHERE id = 2;
DELETE FROM "$SRC_SCHEMA".$TABLE WHERE id = 6;
SQL
DML_EVENTS=6
EXPECTED_EVENTS=$((SNAPSHOT_ROWS + DML_EVENTS))
pass "$CUR_STEP" "I3·U2·D1 (기대 이벤트 ≥ $EXPECTED_EVENTS = 스냅샷 $SNAPSHOT_ROWS + DML $DML_EVENTS)"

# ── 8. 타깃 반영 대기 (고정 sleep 금지) ───────────────────────────────────────
# (a) /api/metrics/tables: 이 토픽의 totalEvents가 기대 이상이고 jdbcLag==0 — sink가 토픽을 다 소비.
#     lag만 보면 "아직 이벤트가 안 들어온" 상태도 0이라, totalEvents 하한을 같이 건다.
# (b) 그래도 최종 판정은 9단계 verify 이므로, (a)는 "기다릴 만큼 기다렸나"의 신호일 뿐이다.
step "타깃 반영 대기"
applied() {
  api GET /api/metrics/tables || return 1
  echo "$LAST_BODY" | jq -e --arg t "$TOPIC" --argjson n "$EXPECTED_EVENTS" \
    '.[]|select(.topic==$t)|(.totalEvents>=$n and .jdbcLag==0)' >/dev/null
}
poll "$T_APPLY" apply applied || fail "$CUR_STEP" "${T_APPLY}s 내 $TOPIC totalEvents≥$EXPECTED_EVENTS & jdbcLag=0 안 됨"
pass "$CUR_STEP" "totalEvents≥$EXPECTED_EVENTS, jdbcLag=0"

# ── 9. 정합성 검증 (체크섬) → changelog 검증 ──────────────────────────────────
# PASS 조건: sourceCount==targetCount && checksumSupported==true && 체크섬 동일.
# (checksumSupported=false면 행수만 비교한 것이라 PASS로 인정하지 않는다 — 이 E2E는 동종 PG→PG)
step "정합성 검증(체크섬)"
verified() {
  api POST /api/recovery/verify "{\"registeredTableId\":$REG_ID}" || return 1
  echo "$LAST_BODY" | jq -e '.sourceCount==.targetCount and .checksumSupported==true
                             and (.sourceChecksum!=null) and .sourceChecksum==.targetChecksum' >/dev/null
}
poll "$T_VERIFY" verify verified || fail "$CUR_STEP" "${T_VERIFY}s 내 count·checksum 일치 안 됨"
pass "$CUR_STEP" "count=$(echo "$LAST_BODY" | jq -r .sourceCount) checksum=$(echo "$LAST_BODY" | jq -r .sourceChecksum)"

# changelog 검증은 정확 일치가 아니라 "이상"으로 판정한다 — 파이프라인은 exactly-once가 아니라
# at-least-once라 재전달 시 같은 이벤트가 changelog에 중복 append될 수 있다 (architecture.md 8절).
# 정확성은 위 체크섬(PK upsert 멱등으로 타깃 수렴)이 보증하고, changelog는 "유실 없음"만 본다.
# Iceberg sink는 60초 주기로 commit하므로 totalRecords가 바로 오르지 않는다 → polling.
step "changelog 검증"
changelog_ok() {
  api GET /api/changelog || return 1
  CL_COUNT=$(echo "$LAST_BODY" | jq -r --arg t "$CHANGELOG_TABLE" '[.[]|select(.table==$t)][0].totalRecords // 0')
  [ "$CL_COUNT" -ge "$EXPECTED_EVENTS" ]
}
CL_COUNT=0
poll "$T_CHANGELOG" changelog changelog_ok || fail "$CUR_STEP" "${T_CHANGELOG}s 내 $CHANGELOG_TABLE totalRecords($CL_COUNT) ≥ $EXPECTED_EVENTS 안 됨"
pass "$CUR_STEP" "$CHANGELOG_TABLE totalRecords=$CL_COUNT ≥ $EXPECTED_EVENTS"

# ── 10. 정리 (성공했을 때만) ──────────────────────────────────────────────────
# 여기까지 왔다는 것은 모두 PASS. 실패 시에는 fail()이 먼저 종료해 이 단계에 오지 않는다(상태 보존).
step "정리"
cleanup_all
residual_check || fail "$CUR_STEP" "정리 후 잔여 객체가 있다 — 위 목록 확인"
pass "$CUR_STEP" "dze2e 객체 전부 제거·잔여 없음"

summary_and_exit 0
