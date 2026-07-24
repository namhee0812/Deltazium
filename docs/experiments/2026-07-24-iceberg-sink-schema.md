# 실험: iceberg-sink가 만드는 changelog 스키마 실측 (Oracle 없이)

> 목적: architecture.md 5.1절 고정 스키마와 실제 sink 동작의 간극 확인
> — connectors/README.md 미결 1번의 판단 근거.

## 방법

1. Debezium Oracle envelope 형식(JSON converter, schemas.enabled=true)의 샘플 4건(op=r/c/u/d)을
   수동 생성해 `dz.SRC.ORDERS` 토픽에 발행 (키: `{"ID": n}` struct).
2. iceberg-sink를 `iceberg.tables=changelog.src_orders`, `auto-create-enabled=true`로 배포.
3. JDBC 카탈로그(PG)와 MinIO의 metadata.json에서 생성된 스키마·파티션 스펙을 직접 확인.

## 결과

### 자동 생성된 테이블 스키마 (envelope 그대로)

```
before: struct(ID long, AMOUNT string?, STATUS string?)   (optional)
after:  struct(ID long, AMOUNT string?, STATUS string?)   (optional)
source: struct(version, connector, name, ts_ms long, snapshot?,
               db, schema, table, txId?, scn string?, commit_scn string?)
op: string
ts_ms: long (optional)          ← 커넥터 처리 시각 (소스 커밋 시각 아님)
partition spec: []               ← 파티션 없음
```

### 5.1절 고정 스키마와의 차이

| 5.1절 | 실측 (auto-create) |
|---|---|
| `scn` long (top-level) | `source.scn` string (중첩) |
| `tx_id` string | `source.txId` (중첩) |
| `ts_ms` timestamp = 소스 커밋 시각 | top-level `ts_ms`는 처리 시각, 커밋 시각은 `source.ts_ms` |
| `source_table` string | `source.schema` + `source.table` (중첩) |
| `days(ts_ms)` 파티션 | 파티션 없음 |

### 평탄화가 스톡 구성으로 불가능함의 실증

- Iceberg transforms의 `CopyValue` SMT: `Struct.get(sourceField)` — **top-level 필드만**.
  `source.scn` 같은 경로 해석 없음 (소스 코드 확인).
- Iceberg `DebeziumTransform`·Debezium `ExtractNewRecordState`: **before를 버림** →
  5.1의 무손실 재조립 불변식 위반.
- 커스텀 SMT는 CLAUDE.md 절대 규칙(자체 데이터 경로 코드 금지) 위반.

### 적재·커밋 동작 확인

- 8건(원 4건은 Connect 재시작 후 재소비로 중복) append 커밋 확인 — `added-records=8`.
  changelog 중복은 append-only 설계상 허용, 재생 시 PK upsert 멱등으로 흡수(6.2절 부합).
- **태스크는 새 레코드가 들어와 put()이 불려야 control 이벤트(커밋 라운드)에 응답한다.**
  idle 토픽에서는 마지막으로 쓴 데이터 파일의 카탈로그 커밋이 다음 레코드 유입까지 지연될 수 있음
  — 커밋 지연 모니터링 시 오탐 주의. (커밋 전 파일은 카탈로그에 안 보임 = 정합성 문제는 없음)

### 부수 발견

- Iceberg 배포판에 JDBC 드라이버 미포함(문서 명시) → 플러그인 `lib/`에
  `postgresql-42.7.8.jar` 수동 추가 필요 (deploy/README.md에 반영).
- Connect sink consumer 기본 `auto.offset.reset`이 earliest가 아니어서 커넥터 생성 전
  발행분을 건너뜀 → worker 설정에 `consumer.auto.offset.reset=earliest` 추가.
- 네임스페이스(`changelog`)는 auto-create가 함께 생성.

## 제안 (사용자 결정 필요 — 5.1절 개정안)

5.1을 "**envelope-as-is + 사전 생성 파티션**"으로 개정하는 것을 제안:

1. changelog 테이블 스키마 = envelope 구조 그대로 (실측 스키마와 동일).
   재조립 관점에서는 오히려 완전 무손실 (source 부가 필드까지 보존).
2. 테이블은 backend가 **사전 생성** (`auto-create-enabled=false` 유지):
   파티션 `days(source.ts_ms)` — Iceberg는 중첩 필드 파티셔닝 가능 (사전 생성 시 지정).
3. 복구 쿼리는 `CAST(source.scn AS long) >= X ORDER BY ...` — scn이 string이라
   파일 프루닝은 문자열 min/max로 부정확하지만, 1차 프루닝은 ts_ms 파티션이 담당하고
   경계 중복은 PK upsert 멱등으로 안전 (6.2절 논리 그대로).

대안(5.1 원안 유지)은 현 스택에서 구현 경로가 없음 — 커스텀 SMT 금지를 풀거나
자체 writer(no-kafka 모드 수렴, 10절)를 앞당겨야 한다.

## 재현

- 샘플 생성 스크립트: (scratchpad) `gen_envelope.py` — 필요 시 `deploy/`로 승격
- 커넥터 설정: connectors/iceberg-sink.json.tmpl에서 `auto-create-enabled=true`로 변경한 것
- 확인: `psql -h localhost -p 5433 -U deltazium -d iceberg_catalog -c "SELECT * FROM iceberg_tables"`,
  `mc cat dz/deltazium-warehouse/warehouse/changelog/src_orders/metadata/<latest>.metadata.json`
