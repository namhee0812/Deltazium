# Deltazium — 아키텍처 설계 문서

> 작성일: 2026-07-23
> 상태: 설계 논의 확정분 정리 (Claude Code 구현 기준 문서)
> 성격: **토이 프로젝트.** Kafka 스택으로 CDC 시맨틱을 학습·검증한다.

---

## 1. 개요와 목표

Debezium + Kafka로 Oracle CDC를 캡처하고, 같은 토픽에서 두 갈래로 병행 적재한다:

- **실 적재**: Debezium JDBC sink → Oracle 타깃 (현재 상태 유지)
- **changelog 보관**: Apache Iceberg sink (append-only) → Iceberg/MinIO — **미러링이 아니라 특정 시점부터 복구(replay)를 위한 이력 저장**

여기에 제어면(backend + Web UI)을 얹는다: 테이블 등록·사전 점검, DDL 승인 워크플로, 복구 트리거, 모니터링.

> **2026-09-05 방향 확정 — 다중 소스·다중 타깃.** 소스 N개(종류·인스턴스 모두)와 타깃 M개
> (OLTP + DW)를 changelog 하나 위에서 다룬다. 설계 축: **changelog를 소스·타깃 중립 계약으로
> 세우고(5절), 소스별 분기는 캡처 층(8절)에, 타깃별 분기는 수렴 층(6절)에 가둔다.** 이 개정으로
> 5·6절이 바뀌었다. 현재 구현은 Oracle 1 → Oracle 1이며, 아래 서술 중 "현행"은 그 상태를 뜻한다.

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
| Iceberg changelog | 장기 이력, 복구 원본, **DW 계열의 랜딩 겸용**(6.5절) | 파티션 drop으로 보존 정책 관리. "며칠 전까지 복구 가능한가"를 결정하는 명시적 파라미터. DW MERGE는 최신 워터마크 이후만 읽으므로 보존 정책은 복구 요건이 계속 지배 |
| 타깃 DB | 현재 상태 | — |

### 2.2 다중 소스·다중 타깃 구조 (2026-09-05)

```
[캡처 층 — 소스별 분기]        [changelog 층 — 중립 계약]         [수렴 층 — 타깃 계열별 분기]
Oracle  ─Debezium Oracle─┐                                    ┌─ JDBC sink(행 upsert) ─▶ OLTP 타깃 (Oracle·PG…)
PG      ─Debezium PG────┼─▶ Kafka ─▶ Iceberg sink(소스별) ─▶ changelog ─┤
MySQL   ─Debezium MySQL─┘        (topic.prefix = 소스 식별자)   (5절)   └─ 타깃 컴퓨트의 집합 MERGE ─▶ DW 타깃 (Snowflake·Databricks)
복구: OLTP = recovery-job 재발행(6.1) · DW = 워터마크 되감기(6.5). 복구 원본은 changelog 하나.
```

- 소스 식별자 = Debezium `topic.prefix`(커넥터당 고유). 토픽 이름과 envelope `source.name`에 박히므로 별도 태깅 없이 소스를 구분한다.
- changelog 하류(recovery-job·DW MERGE·backend 수렴 로직)는 **`source` 블록 내부 필드에 의존하지 않는다** — 순서·증분 기준은 5.1의 위치 컬럼. 이 불변식이 소스 N개를 지탱한다.
- 소스 N개를 타깃 테이블 하나로 합치는 fan-in은 범위 밖(10절). 다루는 것은 "소스 테이블 1 → changelog 1 → 타깃 테이블 M"의 fan-out뿐.
- changelog 저장소는 **설치 프로파일**(3절)로 정한다. 타깃별·테이블별로 저장소를 나누지 않는다 — changelog는 설치당 하나.

## 3. 컴포넌트 스택

| 영역 | 선택 | 비고 |
|---|---|---|
| 소스 캡처 | Debezium source connector — 소스 종류별 (현행 Oracle/LogMiner, 예정 PostgreSQL·MySQL) | Oracle은 처리량·LOB 제약이 최대 리스크 (9절) |
| OLTP 적재 | Debezium JDBC sink | envelope을 이해하고 upsert/delete 변환. PK upsert 멱등 |
| DW 수렴 | 타깃 컴퓨트의 집합 MERGE (Snowflake Task / Databricks Job) | 커넥터 0개. changelog를 외부 Iceberg 카탈로그로 읽는다 (6.5절) |
| changelog 적재 | Apache Iceberg Kafka Connect sink — **소스별 인스턴스 1개** | append 모드 (upsert 미사용 — 지원도 안 됨). 플러그인 설치는 1회 |
| 스토리지 | **설치 프로파일**: MinIO(온프레미스, 현행) / Cloudflare R2(SaaS DW 타깃 시) | SaaS DW의 컴퓨트가 사내 MinIO에 닿을 수 없어 R2가 DW 계열의 전제. 프로파일 전환은 changelog 이전이 따르는 설치 작업 |
| Iceberg 카탈로그 | 프로파일에 따름: JDBC(PostgreSQL, MinIO 프로파일) / Iceberg REST(R2 Data Catalog, R2 프로파일) | **설치당 카탈로그 1개, 소스별 namespace**(5.1). 카탈로그를 소스별로 쪼개지 않는다 |
| converter | JSON (schemas.enabled=true) | Avro/Schema Registry는 미결 (10절) — 컴포넌트 수 절약 |
| 인프라 | docker-compose | Kafka는 KRaft 단일 노드, Connect는 단일 워커 |

## 4. 토픽·커넥터 구성

- Debezium 기본대로 **테이블당 토픽 1개** (`<prefix>.<schema>.<table>`). 이 구성이 DDL 워크플로의 "테이블 단위 정지"(7절)를 가능하게 하는 전제다.
- 커넥터 템플릿 4종 (`connectors/`, backend가 렌더링·배포). 인스턴스 수는 소스·타깃에 비례한다 (2026-09-05):
  1. **source**: 소스당 1개. `table.include.list`가 등록 테이블 목록. 트랜잭션 메타데이터·schema change topic 활성화. `topic.prefix`가 소스 식별자.
  2. **jdbc-sink**: OLTP 타깃 테이블당 1개. PK 기반 upsert, delete 처리 활성화.
  3. **iceberg-sink**: **소스당 1개** (단일 인스턴스에서 전환 — 소스 하나의 실패 레코드·설정 변경이 다른 소스의 changelog를 멈추지 않게). 그 소스의 토픽만 구독, append 모드, 토픽 이름 기준 라우팅으로 소스 테이블당 changelog 테이블 1개. **복구 토픽은 구독하지 않는다**(6.2).
  4. **recovery-sink**: jdbc-sink와 동일 설정에 구독 토픽만 복구 토픽. 평시 정지 상태, 복구 시에만 기동.
  - DW 타깃에는 커넥터가 없다 (6.5).
- 초기 스냅샷: Debezium `initial` 그대로 (스냅샷 레코드 op='r'도 Iceberg에 쌓임 — 초기 상태+변경분이 한 테이블에 완결). 대형 테이블 incremental snapshot은 미결(10절).

## 5. Iceberg changelog 테이블 스펙 (고정 — 임의 변경 금지)

> **2026-07-26 개정.** 원안(평탄화 스키마: op/before/after/scn/tx_id/ts_ms/source_table)은
> 스톡 커넥터·SMT 조합으로 구현 경로가 없음이 실증되어(스톡 SMT는 중첩 필드 승격 불가,
> Debezium·Iceberg의 변환 SMT는 before를 소실 — docs/experiments/2026-07-24-iceberg-sink-schema.md)
> **envelope-as-is 저장**으로 개정했다. 재조립 관점에서는 오히려 완전 무손실이 된다.

> **2026-09-05 개정 (다중 소스·다중 타깃).** ① 위치 컬럼 `_pos` 추가 — 순서·증분 기준을
> `source.scn`(Oracle 전용)에서 파이프라인이 부여하는 값으로 전환. ② namespace를 소스별로
> 분리. ③ 라우팅을 토픽 이름 기준으로 전환(동명 테이블 제약 해소). 기존 changelog는 이전하지
> 않고 재등록(재스냅샷)으로 새로 쌓는다 — 실행 전 사용자 확인.

### 5.1 스키마 (envelope-as-is + 위치 컬럼)

소스 테이블당 changelog 테이블 1개. 이름은 `changelog_<topic.prefix>.<schema>_<table>` 소문자
— namespace가 소스별이라 소스가 여럿이고 스키마·테이블 이름이 같아도 충돌하지 않는다.
카탈로그는 설치당 하나(3절). namespace는 단일 레벨로 고정한다 (카탈로그 구현별 다단계 지원
차이를 피하기 위함). 컬럼 = **Debezium envelope 구조 그대로 + `_pos`**:

| 컬럼 | 타입 | 원천 |
|---|---|---|
| `op` | string | envelope op (c/u/d/r) |
| `before` | struct (소스 테이블 스키마) | envelope before (nullable) |
| `after` | struct (소스 테이블 스키마) | envelope after (nullable) |
| `source` | struct | envelope source 전체. **재조립·포렌식용 — 하류 로직은 이 안의 필드(scn·lsn·pos 등 소스 전용 위치)를 읽지 않는다** |
| `ts_ms` | long | envelope ts_ms (커넥터 처리 시각) |
| `_pos` | struct `{partition: int, offset: long}` | **파이프라인 부여 위치.** Kafka 모드: 이벤트가 온 토픽 파티션·offset (Iceberg sink의 Kafka 메타데이터 SMT로 부착). no-kafka 모드(10절): 자체 writer가 partition 0에 단조 시퀀스 부여 |

- `_pos`의 의미: **Debezium이 방출한 순서**를 보존한다. Debezium은 트랜잭션을 커밋 순서로
  방출하고 Kafka는 파티션 안에서 그 순서를 지키며, 키가 PK라 같은 PK는 같은 파티션에 들어간다.
  따라서 (partition, offset) 오름차순 = PK별 변경 순서. 파티션 간 순서는 정의하지 않고
  필요하지도 않다 (수렴에 필요한 건 PK별 순서뿐).
- 왜 `source.scn`이 아닌가: Oracle 전용이라 소스가 늘면 하류가 갈라지는 것이 첫째. 둘째,
  `source.scn`은 변경 시점 SCN이지 커밋 SCN이 아니라 **장기 트랜잭션에서 커밋 순서와 어긋난다**
  (T1: scn 100에 X=1, scn 300 커밋 / T2: scn 200에 X=2, scn 250 커밋 → Oracle 최종값 1, Debezium
  방출 순서 T2→T1이라 라이브 타깃도 1, scn 정렬 재생은 T1→T2로 2). 라이브와 복구 결과가
  달라지는 사고이고, offset 순서는 이 문제가 없다.
- 테이블은 **backend가 사전 생성**한다 (`auto-create-enabled=false` 유지). 사전 생성 시
  기본 골격(op/ts_ms/source 핵심 필드/_pos)만 정의하고, before/after 등 나머지는
  sink의 `evolve-schema-enabled=true`가 첫 레코드에서 채운다 — 소스→Iceberg 타입
  매핑을 backend가 중복 구현하지 않기 위함.
- 토픽→테이블 라우팅: 소스별 iceberg-sink + 토픽 이름(Kafka 메타데이터 필드) 기준 route-regex.
  `_pos` 부착과 라우팅 필드를 같은 SMT가 제공한다 (SMT 설정 키는 구현 시 Iceberg 커넥터 문서로
  확인). 종전 `route-field=source.table`의 동명 테이블 제약은 해소.

**설계 불변식 1: changelog 한 행에서 Debezium envelope을 손실 없이 재조립할 수 있어야 한다.**
envelope-as-is라 자명하게 성립하며(`_pos`는 재조립에서 제외), recovery-job의 왕복 테스트가 회귀 방어선이다.

**설계 불변식 2: changelog 하류는 소스 중립이다.** recovery-job·backend 수렴/복구 로직·DW MERGE는
op/before/after/키/`_pos`/ts_ms만 읽는다. `source` 내부 필드 참조는 `deploy/rule-check.sh`가 차단한다
(구현 시 추가).

### 5.2 파티셔닝·특성

- Hidden partitioning: `truncate(source.ts_ms, 86400000)` — source.ts_ms(epoch millis, long)의
  1일 단위 truncate. long 타입이라 `days()` 변환은 불가하고 truncate가 동등한 프루닝을 제공한다.
  (볼륨 크면 3600000=1시간)
- 복구 진입점은 **시각**(소스 중립 — SCN·LSN은 UI·API에서 쓰지 않는다). 1차 프루닝은 ts_ms
  파티션이고 정밀 절단은 불필요 — 경계 중복은 PK upsert 멱등으로 안전(6.2절). 단, `source.ts_ms`도
  변경 시각이라 자정 직전 변경·자정 이후 커밋 행은 전날 파티션에 있을 수 있다 → **진입 시각의
  한 파티션 앞부터 읽는다**(늘어난 중복도 멱등이 흡수).
- **append-only.** equality delete 없음 → compaction 압박 낮음, small file 정리는 볼륨 보고 추후(10절).
- 보존 정책 = 오래된 파티션 drop.
- 저장 순서는 무순서 허용 — 순서 복원은 재생 시점에 `_pos`로 한다(6.2절).

### 5.3 읽기 계층 (확정)

Iceberg는 테이블 포맷일 뿐 엔진이 아니다. 읽기는 **iceberg-data(Java API) 단일 프로세스** — 카탈로그 조회→파티션 프루닝→parquet 디코딩까지 라이브러리가 처리한다. **Spark/Trino 도입 금지** (분산 SQL 분석 필요가 생기면 그때 별도 논의). 검증·탐색용으로 DuckDB/PyIceberg 사용은 무방.

- SaaS DW가 외부 카탈로그로 changelog를 읽는 것(6.5)은 이 금지의 대상이 아니다 — 타깃 컴퓨트는 타깃의 일부다.
- 엔진이 필요해지는 경우는 셋뿐이며 전부 현재 범위 밖: small file compaction(Java API에 엔진 없는 rewrite가 없음), 온프레미스 lakehouse 타깃(Iceberg 최종 테이블을 우리 인프라에서 MERGE — Trino가 후보), 그때 재논의.

## 6. 복구 (replay) 설계

### 6.1 방식: 복구 원본은 changelog 하나, 되돌리는 방식은 타깃 계열별 하나 (2026-09-05 개정)

- **OLTP 계열 — 재발행.** recovery-job은 **Iceberg scan → envelope 재조립 → 복구 토픽 발행**까지만 한다. apply는 recovery-sink(= 동일 JDBC sink 설정)가 담당.
- **DW 계열 — 워터마크 되감기.** 랜딩이 곧 changelog라 재발행할 것이 없다. MERGE 워터마크를 과거로 되돌리고 다시 돌리면 그것이 복구다(6.5). 재발행하면 복구 토픽의 offset이 원본과 비교 불가라 `_pos`의 의미가 깨지므로 **DW 계열에 재발행을 쓰지 않는다.**
- 원칙: **계열 안에서 라이브와 복구는 같은 경로.** 복구 결과가 live 적재와 미묘하게 달라지는 사고를 원천 차단한다. (종전 "apply 시맨틱 단일 경로"의 일반화)
- recovery-job의 타깃 직접 apply(A안)는 만들지 않는다. 단, 스키마 불변식(5.1)이 유지되는 한 나중에 추가 가능하도록 닫아두지 않는다 (Kafka 자체 장애까지 커버해야 할 때의 카드).

### 6.2 순서와 멱등

- 순서의 원천은 **Debezium이 방출한 순서**이고, 그것을 보존하는 것이 `_pos`(5.1)다. 재생은
  `_pos.partition`별로 `_pos.offset` 오름차순 재발행. 파티션 간 인터리빙은 임의 — 같은 PK는
  같은 파티션이라 upsert 정합에 영향 없다. 소스 전용 위치(scn·lsn 등)로 정렬하지 않는다
  (근거: 5.1의 장기 트랜잭션 예).
- 진입점은 시각 → ts_ms 파티션 프루닝(한 파티션 앞부터, 5.2) → 그 이후 전부 재생.
- 경계 중복 적용은 필연 — **PK upsert 멱등이 전제**라서 안전하다 (I/U는 덮어쓰기, D는 no-op). 정밀한 경계 절단이 필요 없어지는 근거.
- **복구 토픽은 changelog에 append하지 않는다.** iceberg-sink는 원본 토픽만 구독한다(4절). 복구 이벤트가 섞이면 `_pos`가 원본과 비교 불가가 된다.

### 6.3 Kafka retention과의 관계

retention을 넘긴 과거 구간은 Kafka에 없다 — 그 구간의 유일한 원천이 Iceberg이고, 복구 토픽 재발행이 그 경로다. DDL 거부로 장기 정지된 테이블의 캐치업(7절)도 같은 메커니즘을 재사용한다.

### 6.4 복구 리허설 (구현 후 함께 개선)

시나리오(OLTP): ① 타깃 테이블 훼손(행 삭제/절단) → ② UI에서 시각 지정 복구 트리거 → ③ recovery-job 재발행 → ④ recovery-sink apply → ⑤ SRC/TGT 정합 검증 스크립트(행 수 + 체크섬). 이 리허설이 통과해야 "복구 기능이 있다"고 말할 수 있다.

시나리오(DW, 6.5 구현 후): ① 최종 테이블 훼손 → ② 워터마크를 시각 기준 `_pos`로 되감기 → ③ MERGE 재실행 → ④ 정합 검증. 추가로 **MERGE 멱등 증명**: 같은 이벤트를 두 번 append해도 최종 테이블은 1행.

### 6.5 DW 계열 수렴 (2026-09-05 방향 — 세부는 docs/TODO.md)

Snowflake·Databricks에는 Debezium JDBC sink의 dialect가 없고, 행 단위 upsert가 구조적으로
비싸다(파일 재작성 단위 과금). 별도 랜딩 경로를 만들지 않고 **changelog를 랜딩으로 겸용**한다.

| | OLTP 계열 | DW 계열 |
|---|---|---|
| 수렴 실행 주체 | JDBC sink (행 단위 upsert) | 타깃 컴퓨트의 집합 MERGE (Snowflake Task / Databricks Job) |
| 멱등 책임 | sink의 PK upsert | MERGE 문 — PK별 최신 1건 선별(`_pos` 내림차순 ROW_NUMBER) 후 병합, op='d'는 DELETE |
| 증분 기준 | Kafka offset | **파티션별 `_pos.offset` 워터마크** (ts_ms 파티션 프루닝 병행) |
| 타깃 반영 지연 | 초 단위 | Iceberg 커밋 주기(60s) + MERGE 주기 **1분** + 카탈로그 동기화 → end-to-end 2분대 |
| PK 필수 | 유지 | 유지 (MERGE ON 키) |
| 복구 | 재발행 → 동일 sink | 워터마크 되감기 → MERGE 재실행 |
| 커넥터 | jdbc-sink | 없음 |

- 전제: 저장소 프로파일이 R2(3절). DW가 외부 Iceberg 카탈로그(REST)로 changelog namespace를 읽는다.
- backend는 DW 자격(웨어하우스·토큰)만 받고, changelog 접속 정보는 설치 설정에서 꺼내 catalog
  integration 생성 SQL에 채운다. DW에 주는 카탈로그 토큰은 읽기 전용으로 분리 발급.
- DW lag = "마지막 MERGE 워터마크 `_pos` vs changelog 최신 `_pos`"로 정의해 기존 lag 화면에 통합.
- 범위 밖: DW 스키마 전파(7절 워크플로 확장 — 별도 설계), 초 단위 신선도(테이블 단위로
  Snowpipe Streaming 등 스트리밍 ingest 하이브리드 — 1분 합의가 유지되는 한 도입하지 않음).
- 미확인(구현 전 공식 문서 확인): R2 Data Catalog 상태·한도, Snowflake catalog integration의
  외부 REST 지원 범위·인증, Databricks(serverless 포함)의 Iceberg REST federation, Iceberg sink의
  REST 카탈로그 인증 키·Kafka 메타데이터 SMT 설정 키.

## 7. DDL 승인 워크플로 (확정)

1. **감지**: backend가 Debezium schema change topic 구독 → DDL 이벤트를 메타데이터 DB에 기록, UI에 경고 표시.
2. **승인**: 사용자가 [적용] 클릭 → backend가 타깃에 DDL 적용 → 해당 테이블 CDC 계속.
3. **거부**: 사용자가 [거부] 클릭 → **jdbc-sink 설정에서 해당 테이블 토픽 제외** (Connect REST 설정 갱신) → 해당 테이블 apply만 정지. 사용자가 수동으로 스키마 정리.
4. **재개**: 정리 후 [재개] → 토픽 복원 → 밀린 것부터 캐치업. **정지 중에도 changelog는 Kafka·Iceberg에 계속 축적**되므로 데이터 유실 없음. retention 초과분은 6.3절 재발행 경로로 캐치업.

- Iceberg sink 쪽은 스키마 진화를 자동 수용 (ADD COLUMN 등) — 정지 대상은 jdbc-sink만.
- UI 표현은 ui-reference v3의 DDL 타임라인 패턴 (ADD=자동 승인 후보, DROP/TRUNCATE=승인 대기).

## 8. 테이블 등록과 사전 점검

등록 API가 배포 전 검사한다. **소스별 분기가 공식적으로 존재하는 유일한 자리**(2.2) — 소스
종류별 점검 목록을 소제목으로 나란히 둔다. 등록 테이블의 식별 키는 (source_id, schema, table).

**공통 (소스·타깃 계열 불문):**

- **PK 존재 — 필수.** 없으면 등록 거부 (at-least-once + 멱등 수렴의 전제, 6.2·6.5절).
  (unique index 대체 허용 안 함 — 2026-07-25 결정, PK만)

**Oracle 소스:**

- supplemental logging (ALL) COLUMNS (테이블 단위) — 미설정 시 실행될 DDL을 보여주고
  **"적용하겠습니까?" 승인 후에만 적용**, 권한 부족 시 Oracle 에러 그대로 노출 (2026-07-25 결정).
- ARCHIVELOG 모드, LogMiner 권한 — 소스 수준 점검.
  **캡처 계정 최소 권한 8개 (2026-07-28 확정, 실검증 기반):**
  `CREATE SESSION` · `LOGMINING` · `SELECT ANY DICTIONARY`(V$ 뷰 일괄 커버) ·
  `SELECT ANY TABLE` · `EXECUTE ON DBMS_LOGMNR` · `EXECUTE ON DBMS_LOGMNR_D`
  (redo_log_catalog 전략 필수 — online catalog면 불필요하나 DDL 워크플로 전제가 깨짐) ·
  `FLASHBACK ANY TABLE`(초기 스냅샷 전용, `no_data`면 생략 가능) ·
  `CREATE TABLE`(+quota, LOG_MINING_FLUSH 테이블용).
- 통과 시: source의 `table.include.list` 갱신 + sink 토픽 목록 갱신을 Connect REST로 배포.

**PostgreSQL 소스 (예정 — 항목은 구현 시 Debezium 문서로 확정):** `wal_level=logical`, 테이블
REPLICA IDENTITY(before 이미지 필요 시 FULL), replication·publication 권한, 슬롯 존재.

**MySQL 소스 (예정):** `binlog_format=ROW`, `binlog_row_image=FULL`, REPLICATION SLAVE/CLIENT 권한, GTID 여부.

**타깃 점검:**

- OLTP: 접속·타깃 스키마 존재 (현행).
- DW: **저장소 프로파일이 R2인지** — MinIO 프로파일이면 "changelog 저장소가 외부 접근 불가"로
  거부(PK 거부와 같은 방식). 통과 시 DW 접속 + catalog integration이 changelog namespace를
  실제로 조회할 수 있는지까지 확인. R2 접속 정보는 여기서 받지 않는다(설치 설정, 6.5).

## 9. 리스크와 검증 순서

1. **Debezium Oracle 소스가 단일 최대 리스크.** LogMiner 처리량 상한, supplemental logging 전제, LOB 제약. **실 볼륨 PoC를 최우선으로.** 탈출구: OpenLogReplicator, XStream(라이선스).
2. 듀얼 sink 배선 검증 (스키마 고정 테스트 포함).
3. 복구 리허설 (6.4절) — 이게 통과해야 아키텍처의 존재 이유가 증명됨.

## 10. 미결 / 나중에

- **no-kafka 모드** (제품화 방향): Debezium Engine embed + 자체 버퍼 = NG의 trail 설계가 답. 이때 Iceberg 적재는 자체 writer(`worker-iceberg`)를 만들고 kafka 모드도 그 writer로 수렴시켜 구현 1개 유지. 핵심: 소비 포지션을 Iceberg 스냅샷 메타데이터에 원자 커밋(exactly-once). **5절 스키마 고정이 이 수렴의 전제조건 — `_pos` 부여는 writer 책임**(Kafka 모드는 SMT가 offset을, 자체 writer는 자기 시퀀스를 넣는다. 컬럼 정의가 같으므로 두 writer가 같은 테이블에 이어 쓴다). 스냅샷 메타데이터에 커밋하는 포지션도 이 값.
- **fan-in** (소스 N개 → 타깃 테이블 1개, 샤딩된 소스 통합): 범위 밖. 소스 간 PK 충돌 규칙(타깃 PK에 source_id 결합)과 소스 간 순서 미정의를 다뤄야 한다 — 필요해지면 별도 설계.
- **DW 스키마 전파**: 7절 워크플로를 DW 타깃으로 확장 — 별도 설계.
- 대형 테이블 초기 스냅샷 (incremental snapshot + 시그널 테이블)
- small file compaction (시간 파티셔닝 + commit interval로 버티다 볼륨 보고 추가)
- Avro/Schema Registry 도입 (envelope 스키마 진화 관리에 유리, 토이 단계는 JSON으로 충분)
- 모니터링·알람·DLQ 정책 — 운영 제약은 나중에
- Kafka 경량화 옵션 (Redpanda, standalone Connect) — 온프레미스 포장 시
