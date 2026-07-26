# connectors — 커넥터 설정 템플릿

`{{var}}` 자리는 backend가 렌더링해 Connect REST(`POST /connectors`)로 배포한다.
설정 키는 전부 공식 문서에서 확인한 것만 사용 (Debezium 3.6 / Iceberg kafka-connect 1.11 기준, 2026-07-24 검증).

| 템플릿 | 커넥터 | 역할 |
|---|---|---|
| source.json.tmpl | Debezium Oracle source | SRC 캡처. `table.include.list` = 등록 테이블 목록 |
| jdbc-sink.json.tmpl | Debezium JDBC sink | 실 적재 (PK upsert 멱등 + delete) |
| iceberg-sink.json.tmpl | Apache Iceberg sink | changelog append 적재 |
| recovery-sink.json.tmpl | Debezium JDBC sink | jdbc-sink와 동일 설정, 구독 토픽만 복구 토픽. 평시 정지 |

## 확정 선택의 근거

- `log.mining.strategy=redo_log_catalog` (기본값 명시): DDL 승인 워크플로(architecture.md 7절)가
  schema change 이벤트에 의존하므로 DDL 추적이 되는 전략을 쓴다. `online_catalog`는 빠르지만 DDL 추적 불가.
- `schema.evolution=none` (jdbc-sink): 스키마 변경은 7절 승인 워크플로가 처리한다. sink가 임의로 타깃 DDL을 치면 안 됨.
- `iceberg.tables.auto-create-enabled=false`: changelog 테이블 스키마는 5절에 고정 —
  테이블은 backend가 명시 스키마로 생성하고 sink는 append만 한다.
- `iceberg.control.commit.interval-ms=60000`: 기본 5분은 토이 검증 피드백에 너무 길다.

## 미결 (마일스톤 2·3에서 실배선으로 확정)

1. ~~envelope → changelog 스키마 변환~~ **해소(2026-07-26)**: 5.1절을 envelope-as-is로 개정.
   changelog 테이블은 backend가 사전 생성(기본 골격 + `truncate(source.ts_ms, 1일)` 파티션),
   sink는 `evolve-schema-enabled=true`로 before/after를 첫 레코드에서 채운다.
   라우팅: 단일 iceberg-sink + `route-field=source.table` + 테이블별 route-regex
   (동명 테이블의 스키마 간 동시 등록은 등록 시점 거부).
   **주의: JdbcCatalog는 catalog_name으로 스코핑 — backend와 sink 모두 "iceberg" 이름 사용.**
2. ~~jdbc-sink 토픽→타깃 테이블 매핑~~ **확정(2026-07-25)**: RegexRouter로 토픽명에서
   `<prefix>.<schema>.` 접두를 제거해 테이블명만 남기고, apply는 **TARGET 연결 계정의
   기본 스키마**에 수행한다 (`collection.name.format=${topic}`). 스키마 한정자 문제 회피 —
   타깃 스키마 = TARGET 접속 계정 스키마라는 규약. 실 Oracle 배선에서 최종 검증 예정.
3. source의 `database.pdb.name`: 대상 Oracle이 CDB/PDB 구성일 때만 추가.
