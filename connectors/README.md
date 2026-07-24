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

1. **envelope → changelog 스키마(5.1절) 변환.** Iceberg sink는 레코드 value를 그대로 쓰므로
   raw envelope이면 컬럼이 `op/before/after/source(struct)/ts_ms`가 되고, 5.1의
   `scn/tx_id/ts_ms/source_table` 평탄화와 다르다. 스톡 SMT 체인으로는 "source 하위 필드만 승격,
   before/after는 struct 유지"가 안 되고, Debezium ExtractNewRecordState나 Iceberg DebeziumTransform은
   **before를 버려서** 5.1의 무손실 재조립 불변식을 깬다. 커스텀 SMT는 절대 규칙 위반.
   → 마일스톤 3에서 스키마 검증 테스트를 먼저 만들고, envelope-as-is 저장(source struct 유지)으로
   5.1을 개정할지 사용자와 논의 필요. (재조립 관점에선 envelope-as-is가 오히려 무손실에 유리)
2. **jdbc-sink 토픽→타깃 테이블 매핑.** 토픽명 `<prefix>.<schema>.<table>`을 타깃 `TGT.<table>`로
   보내는 `collection.name.format`/RegexRouter 조합은 Oracle 스키마 한정자 처리 방식을
   실 Oracle 배선에서 확인 후 고정한다.
3. source의 `database.pdb.name`: 대상 Oracle이 CDB/PDB 구성일 때만 추가.
