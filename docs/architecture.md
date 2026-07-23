# Deltazium — 아키텍처 설계 문서

> 작성일: 2026-07-23
> 상태: 설계 논의 확정분 정리 (Claude Code 구현 기준 문서)
> 성격: **토이 프로젝트.** DeltaStream NG와 별개 트랙 — Kafka 스택으로 CDC 시맨틱을 학습·검증. 제품화 시 지식은 NG(trail 기반)로 이전.

---

## 1. 개요와 목표

Debezium + Kafka로 Oracle CDC를 캡처하고, 같은 토픽에서 두 갈래로 병행 적재한다:

- **실 적재**: Debezium JDBC sink → Oracle 타깃 (현재 상태 유지)
- **changelog 보관**: Apache Iceberg sink (append-only) → Iceberg/MinIO — **미러링이 아니라 특정 시점부터 복구(replay)를 위한 이력 저장**

여기에 제어면(backend + Web UI)을 얹는다: 테이블 등록·사전 점검, DDL 승인 워크플로, 복구 트리거, 모니터링.

## 2. 아키텍처

```
Oracle(SRC) ──Debezium source(LogMiner)──▶ Kafka(KRaft) ──┬─▶ Debezium JDBC sink ──▶ Oracle(TGT)
                                                          └─▶ Iceberg sink (append) ─▶ Iceberg / MinIO(S3)
                                                                카탈로그: JDBC (PostgreSQL)

복구: recovery-job ──(Iceberg scan · envelope 재조립)──▶ 복구 토픽 ──▶ JDBC sink(동일 설정, 토픽만 다름) ──▶ Oracle(TGT)

[Web UI] ⇄ [backend(Spring Boot): Connect REST 프록시 · 테이블 등록 · DDL 승인 · 복구 트리거]
```

두 sink는 같은 토픽을 독립 consumer group으로 소비 — 타깃별 lag 격리 (Iceberg가 느려져도 실 적재에 영향 없음).

### 2.1 역할 분담 (확정)

| 컴포넌트 | 역할 | 보존 |
|---|---|---|
| Kafka | 짧은 버퍼, fan-out | retention 짧게 (수 시간~1일). 이력 보관은 Kafka 책임이 아님 |
| Iceberg/MinIO | 장기 이력, 복구 원본 | 파티션 drop으로 보존 정책 관리. "며칠 전까지 복구 가능한가"를 결정하는 명시적 파라미터 |
| Oracle 타깃 | 현재 상태 | — |

## 3. 컴포넌트 스택

| 영역 | 선택 | 비고 |
|---|---|---|
| 소스 캡처 | Debezium Oracle connector (LogMiner) | 처리량·LOB 제약이 최대 리스크 (9절) |
| 실 적재 | Debezium JDBC sink | envelope을 이해하고 upsert/delete 변환. PK upsert 멱등 |
| changelog 적재 | Apache Iceberg Kafka Connect sink | append 모드 (upsert 미사용 — 지원도 안 됨) |
| 스토리지 | MinIO (S3 호환) | |
| Iceberg 카탈로그 | JDBC 카탈로그 (PostgreSQL 재활용) | REST 카탈로그는 필요 시 교체 |
| converter | JSON (schemas.enabled=true) | Avro/Schema Registry는 미결 (10절) — 컴포넌트 수 절약 |
| 인프라 | docker-compose | Kafka는 KRaft 단일 노드, Connect는 단일 워커 |

## 4. 토픽·커넥터 구성

- Debezium 기본대로 **테이블당 토픽 1개** (`<prefix>.<schema>.<table>`). 이 구성이 DDL 워크플로의 "테이블 단위 정지"(7절)를 가능하게 하는 전제다.
- 커넥터 4종 (설정 템플릿은 `connectors/`, backend가 렌더링·배포):
  1. **source**: Debezium Oracle. `table.include.list`가 등록 테이블 목록. 트랜잭션 메타데이터·schema change topic 활성화.
  2. **jdbc-sink**: 등록 테이블 토픽 구독, PK 기반 upsert, delete 처리 활성화.
  3. **iceberg-sink**: 같은 토픽 구독, append 모드, 테이블 라우팅으로 소스 테이블당 changelog 테이블 1개.
  4. **recovery-sink**: jdbc-sink와 동일 설정에 구독 토픽만 복구 토픽. 평시 정지 상태, 복구 시에만 기동.
- 초기 스냅샷: Debezium `initial` 그대로 (스냅샷 레코드 op='r'도 Iceberg에 쌓임 — 초기 상태+변경분이 한 테이블에 완결). 대형 테이블 incremental snapshot은 미결(10절).

## 5. Iceberg changelog 테이블 스펙 (고정 — 임의 변경 금지)

### 5.1 스키마

소스 테이블당 changelog 테이블 1개. 컬럼:

| 컬럼 | 타입 | 원천 |
|---|---|---|
| `op` | string | envelope op (c/u/d/r) |
| `before` | struct (소스 테이블 스키마) | envelope before (nullable) |
| `after` | struct (소스 테이블 스키마) | envelope after (nullable) |
| `scn` | long | source.scn |
| `tx_id` | string | source.txId |
| `ts_ms` | timestamp | source.ts_ms (소스 커밋 시각) |
| `source_table` | string | source.schema + table |

**설계 불변식: 이 컬럼들만으로 Debezium envelope을 손실 없이 재조립할 수 있어야 한다.** recovery-job의 왕복 테스트(원본 envelope → Iceberg 레코드 → 재조립 envelope 동등성)가 이 불변식의 회귀 방어선이다.

### 5.2 파티셔닝·특성

- Hidden partitioning: `days(ts_ms)` (볼륨 크면 `hours`). "SCN X부터" 스캔이 파티션 프루닝으로 저렴해야 함.
- **append-only.** equality delete 없음 → compaction 압박 낮음, small file 정리는 볼륨 보고 추후(10절).
- 보존 정책 = 오래된 파티션 drop.
- 저장 순서는 무순서 허용 — 순서 복원은 재생 시점 책임(6.2절).

### 5.3 읽기 계층 (확정)

Iceberg는 테이블 포맷일 뿐 엔진이 아니다. 읽기는 **iceberg-data(Java API) 단일 프로세스** — 카탈로그 조회→파티션 프루닝→parquet 디코딩까지 라이브러리가 처리한다. **Spark/Trino 도입 금지** (분산 SQL 분석 필요가 생기면 그때 별도 논의). 검증·탐색용으로 DuckDB/PyIceberg 사용은 무방.

## 6. 복구 (replay) 설계

### 6.1 방식: 재발행 (확정)

recovery-job은 **Iceberg scan → envelope 재조립 → 복구 토픽 발행**까지만 한다. apply는 recovery-sink(= 동일 JDBC sink 설정)가 담당 → **apply 시맨틱 단일 경로.** 복구 결과가 live 적재와 미묘하게 달라지는 사고를 원천 차단한다.

- 타깃 직접 apply(A안)는 만들지 않는다. 단, 스키마 불변식(5.1)이 유지되는 한 나중에 추가 가능하도록 닫아두지 않는다 (Kafka 자체 장애까지 커버해야 할 때의 카드).

### 6.2 순서와 멱등

- 재생 쿼리: `WHERE scn >= X ORDER BY scn, tx_id`. 전역 순서는 여기서 복원한다.
- 경계 중복 적용은 필연 — **PK upsert 멱등이 전제**라서 안전하다 (I/U는 덮어쓰기, D는 no-op). 정밀한 경계 절단이 필요 없어지는 근거.

### 6.3 Kafka retention과의 관계

retention을 넘긴 과거 구간은 Kafka에 없다 — 그 구간의 유일한 원천이 Iceberg이고, 복구 토픽 재발행이 그 경로다. DDL 거부로 장기 정지된 테이블의 캐치업(7절)도 같은 메커니즘을 재사용한다.

### 6.4 복구 리허설 (구현 후 함께 개선)

시나리오: ① 타깃 테이블 훼손(행 삭제/절단) → ② UI에서 SCN X 지정 복구 트리거 → ③ recovery-job 재발행 → ④ recovery-sink apply → ⑤ SRC/TGT 정합 검증 스크립트(행 수 + 체크섬). 이 리허설이 통과해야 "복구 기능이 있다"고 말할 수 있다.

## 7. DDL 승인 워크플로 (확정)

1. **감지**: backend가 Debezium schema change topic 구독 → DDL 이벤트를 메타데이터 DB에 기록, UI에 경고 표시.
2. **승인**: 사용자가 [적용] 클릭 → backend가 타깃에 DDL 적용 → 해당 테이블 CDC 계속.
3. **거부**: 사용자가 [거부] 클릭 → **jdbc-sink 설정에서 해당 테이블 토픽 제외** (Connect REST 설정 갱신) → 해당 테이블 apply만 정지. 사용자가 수동으로 스키마 정리.
4. **재개**: 정리 후 [재개] → 토픽 복원 → 밀린 것부터 캐치업. **정지 중에도 changelog는 Kafka·Iceberg에 계속 축적**되므로 데이터 유실 없음. retention 초과분은 6.3절 재발행 경로로 캐치업.

- Iceberg sink 쪽은 스키마 진화를 자동 수용 (ADD COLUMN 등) — 정지 대상은 jdbc-sink만.
- UI 표현은 ui-reference v3의 DDL 타임라인 패턴 (ADD=자동 승인 후보, DROP/TRUNCATE=승인 대기).

## 8. 테이블 등록과 사전 점검

등록 API가 배포 전 검사:

- **PK 존재 — 필수.** 없으면 등록 거부 (at-least-once + upsert 멱등의 전제, 6.2절).
- supplemental logging (테이블 단위) — 미설정 시 안내 + DDL 제시.
- ARCHIVELOG 모드, LogMiner 권한 — 소스 수준 점검.
- 통과 시: source의 `table.include.list` 갱신 + sink 토픽 목록 갱신을 Connect REST로 배포.

## 9. 리스크와 검증 순서

1. **Debezium Oracle 소스가 단일 최대 리스크.** LogMiner 처리량 상한, supplemental logging 전제, LOB 제약. **실 볼륨 PoC를 최우선으로.** 탈출구: OpenLogReplicator, XStream(라이선스).
2. 듀얼 sink 배선 검증 (스키마 고정 테스트 포함).
3. 복구 리허설 (6.4절) — 이게 통과해야 아키텍처의 존재 이유가 증명됨.

## 10. 미결 / 나중에

- **no-kafka 모드** (제품화 방향): Debezium Engine embed + 자체 버퍼 = NG의 trail 설계가 답. 이때 Iceberg 적재는 자체 writer(`worker-iceberg`)를 만들고 kafka 모드도 그 writer로 수렴시켜 구현 1개 유지. 핵심: 소비 포지션을 Iceberg 스냅샷 메타데이터에 원자 커밋(exactly-once). **5절 스키마 고정이 이 수렴의 전제조건.**
- 대형 테이블 초기 스냅샷 (incremental snapshot + 시그널 테이블)
- small file compaction (시간 파티셔닝 + commit interval로 버티다 볼륨 보고 추가)
- Avro/Schema Registry 도입 (envelope 스키마 진화 관리에 유리, 토이 단계는 JSON으로 충분)
- 모니터링·알람·DLQ 정책 — 운영 제약은 나중에
- Kafka 경량화 옵션 (Redpanda, standalone Connect) — 온프레미스 포장 시
