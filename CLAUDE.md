# CLAUDE.md — Deltazium

Debezium + Kafka 기반 CDC 파이프라인 토이 프로젝트.
Oracle → Kafka → (실 적재: Oracle) + (changelog 보관: Iceberg/MinIO) 병행 적재와, 이를 제어하는 Web UI를 만든다.

**모든 설계 판단의 기준 문서는 `docs/architecture.md`다. 이 파일과 충돌하면 architecture.md가 우선한다. 구현 전에 해당 절을 반드시 읽을 것.**

> 참고: 이 프로젝트는 Kafka 스택으로 CDC 시맨틱(envelope·스냅샷·DDL·복구)을 학습·검증하는 것이 목적이다.

## 절대 규칙

- **데이터 경로는 기성 커넥터만 사용한다.** Debezium Oracle source, Debezium JDBC sink, Apache Iceberg sink. 커스텀 Kafka Connect 커넥터를 작성하지 말 것. 자체 코드는 backend(제어면), recovery-job, ui 세 곳뿐이다.
- **Iceberg changelog 테이블 스키마·파티셔닝은 architecture.md 5절에 고정돼 있다. 임의 변경 금지.** 나중에 자체 writer가 같은 테이블에 이어 쓸 수 있어야 한다 (no-kafka 모드 수렴 전략).
- **복구는 재발행 방식만.** recovery-job은 Iceberg에서 읽어 Debezium envelope으로 재조립 → 복구 토픽 발행까지만 한다. **타깃 DB에 직접 apply하는 코드를 recovery-job에 만들지 말 것** — apply 시맨틱은 JDBC sink 단일 경로다. (architecture.md 6절)
- **Spark/Trino 도입 금지.** Iceberg 읽기는 iceberg-data(Java API) 단일 프로세스로 한다. (architecture.md 5.3절)
- **exactly-once를 가정하지 말 것.** JDBC sink는 at-least-once다. 모든 apply는 PK upsert 멱등이 전제이며, **PK 없는 테이블은 등록 단계에서 거부**한다. (architecture.md 8절)
- `ui-reference/`는 UI 프로토타입 v1~v4 — **읽기 전용 디자인 기준.** 수정 금지, 디자인 토큰·화면 구성 참조용.
- 신규 코드는 **Java 21** (backend는 Spring Boot 3, recovery-job은 플레인 Java), UI는 React + TypeScript.
- **데이터를 지우는 작업은 실행 전 사용자 확인.** 특히 `docker compose down -v`(볼륨 삭제 — Oracle SRC/TGT 스키마·MinIO 오브젝트·Iceberg JDBC 카탈로그가 한꺼번에 소실), MinIO 버킷 비우기, Oracle 스키마 DROP, Iceberg 테이블 DROP.
  컨테이너만 재시작할 땐 `down`(볼륨 유지) 또는 `restart`를 쓴다. 복구 리허설 중 상태를 날리면 재현이 불가능해진다.

## 아키텍처 한 줄 요약 (상세: docs/architecture.md 2절)

```
Oracle ──Debezium source(LogMiner)──▶ Kafka ──┬─▶ Debezium JDBC sink ──▶ Oracle 타깃 (실 적재)
                                              └─▶ Iceberg sink (append) ─▶ Iceberg/MinIO (changelog)
복구: recovery-job(Iceberg 읽기·재발행) → 복구 토픽 → 동일 JDBC sink 설정 → 타깃
[Web UI] ⇄ [backend: Connect REST 프록시 · DDL 승인 · 복구 트리거 · 사전 점검]
```

- 역할 분담: Kafka = 짧은 버퍼(retention 짧게), Iceberg = 장기 이력·복구 원본, 타깃 DB = 현재 상태.
- Iceberg는 미러링이 아니라 **append-only changelog**. upsert/MERGE/compaction 없음.

## 모듈 구조

| 모듈 | 내용 | 비고 |
|---|---|---|
| `deploy/` | **베어메탈 기동 스크립트** (2026-07-23 결정: docker-compose는 나중에): Kafka(KRaft 단일 노드), Kafka Connect(커넥터 플러그인 포함), MinIO, PostgreSQL(메타데이터 + Iceberg JDBC 카탈로그). 바이너리·데이터는 `~/deltazium-runtime/`, Oracle은 미연결(추후 결정) | smoke test 스크립트 포함. 상세: `deploy/README.md` |
| `connectors/` | 커넥터 설정 JSON 템플릿 4종: source / jdbc-sink / iceberg-sink / recovery-sink | backend가 이 템플릿을 렌더링해 Connect REST로 배포 |
| `backend/` | Spring Boot 3 제어면: Connect REST 프록시, 테이블 등록(사전 점검), DDL 승인 워크플로, 복구 잡 트리거, 상태·lag 조회 | 메타데이터는 PostgreSQL |
| `recovery-job/` | 플레인 Java: Iceberg scan(SCN 범위) → envelope 재조립 → 복구 토픽 발행 | Spring 의존 없음. backend가 프로세스로 기동 |
| `ui/` | React + TypeScript + shadcn/ui + Tailwind + TanStack Table + React Flow | 화면 기준은 `ui-reference/` |
| `ui-reference/` | UI 프로토타입 v1~v4 (읽기 전용) | |
| `docs/` | architecture.md 등 | |

## 에이전트 분업 (토큰 효율 — 2026-08-06 결정)

- **구현 위임**: 설계가 확정된 기능의 코드 작성·테스트·빌드·feature 브랜치 커밋은
  `implementer` 에이전트(Sonnet)에게 위임한다. 설계 판단이 필요한 작업은 위임하지 않는다.
  보고를 받은 메인 세션이 main 병합·push를 수행한다.
- **탐색 위임**: "어디에 구현돼 있나"류의 코드·로그 조사는 `explorer` 에이전트(Haiku)에게
  위임한다 — 파일 내용을 메인 컨텍스트에 싣지 않기 위함.
- 예외: 한두 파일의 작은 수정, 에이전트 보고만으로 판단이 어려운 정밀 디버깅은 직접 한다.
- **0층 — 절대 규칙 검사 (자동, 토큰 0)**: `deploy/rule-check.sh`가 pre-commit hook으로
  걸려 있어 모든 커밋이 반드시 통과한다 (`core.hooksPath=deploy/hooks`). grep으로 판정
  가능한 절대 규칙(recovery-job의 타깃 apply, 커스텀 커넥터, Spark/Trino 의존)만 보고,
  위반이면 커밋을 차단한다. 변경 규모·데이터 정합 경로 포함 여부도 함께 출력하므로
  아래 게이트를 어디까지 적용할지는 그 출력을 근거로 판단한다.
  **`--no-verify`로 우회하지 않는다.** 규칙이 틀렸다고 판단되면 우회가 아니라 사용자에게
  '결정 필요'로 보고한다. 판단이 필요한 리뷰는 이 스크립트의 범위가 아니다.
- **병합 전 리뷰 게이트** (implementer 보고를 받은 메인 세션이 위험도로 차등 적용):
  - 데이터 정합에 닿는 변경(복구·재스냅샷·apply 경로·offset 조작·롤업 SQL 등):
    병합 전 diff 직접 검토 필수. 특히 중요하면 `/code-review` 추가 실행.
  - 일반 기능(API·화면·문서): implementer 보고 + 테스트 통과로 충분.
  - 기계적 변경(헤더·스크린샷·문구): 게이트 없음.
  - 규모가 큰 변경(대략 5파일·200줄 초과)은 메인 컨텍스트에 전체 diff를 얹지 않기 위해
    리뷰 위임을 검토한다. 작은 변경은 직접 읽는 편이 싸므로 위임하지 않는다.
  - 리뷰에서 문제 발견 시 직접 고치지 말고 implementer에 수정 지시 (작성·검토 분리 유지).

## 빌드·테스트

```
./deploy/dzadmin all start                          # 전체 기동: infra(pg→minio→kafka→connect)→backend→web
./deploy/dzadmin status                             # 전체 상태 (컴포넌트: infra/backend[engine]/web[ui])
./deploy/dzadmin backend restart                    # 코드 반영 재기동. web은 vite dev(--host 고정)
./deploy/smoke-test.sh                              # 심층 헬스체크 (Connect 플러그인 포함)
./gradlew build                                     # backend + recovery-job
./gradlew :backend:test
```

- **커밋·push는 Claude가 수행한다** (이 리포 한정 — 사용자 레벨 규칙보다 우선. 2026-07-24 사용자 지시).
  단, **관련 모듈 테스트 통과 후에만** 커밋하고, push 단위는 의미 있는 작업 묶음별로 Claude가 정한다.
  커밋 메시지: prefix(`feat:` `fix:` `docs:` `refactor:` `test:` `build:`) + 한국어 본문, `Co-Authored-By` 없음.
- recovery-job은 envelope 재조립의 왕복 테스트 필수 (원본 envelope → Iceberg 레코드 → 재조립 envelope 동등성).

## 마일스톤 (순서 준수)

1. **deploy/**: compose 환경 전체 기동 + smoke test. Oracle XE는 SRC/TGT 스키마 분리, supplemental logging·archivelog 설정 포함.
2. **커넥터 배선 1**: source + JDBC sink 템플릿 → Oracle SRC→TGT 복제 동작 확인 (수동 curl로 배포).
3. **커넥터 배선 2**: Iceberg sink 추가. **changelog 테이블 스키마가 architecture.md 5절과 일치하는지 검증하는 테스트를 먼저 작성**한 뒤 설정을 맞춘다.
4. **backend 1차**: Connect REST 프록시 + 테이블 등록 API(사전 점검: PK 존재·supplemental logging) + 커넥터 템플릿 렌더링·배포.
5. **recovery-job + 복구 리허설**: SCN X부터 재발행 → 정합 검증 스크립트. 리허설 시나리오는 architecture.md 6.4절.
6. **DDL 승인 워크플로**: schema change topic 구독 → 경고 저장 → 승인(타깃 DDL 적용·재개) / 거부(해당 토픽 sink에서 제외). architecture.md 7절.
7. **UI**: ui-reference 기준 화면을 backend API에 연동 — 토폴로지 캔버스(v1), 테이블 그리드(v2·v3), DDL 타임라인·승인(v3).

- 허용된 초기 단순화: 초기 스냅샷은 Debezium `initial` 그대로, converter는 JSON(schemas.enabled=true), 모니터링·알람·DLQ는 범위 외.
- 단, **Iceberg 스키마 고정(5절)과 복구 재발행 원칙(6절)은 처음부터 정확히** — 여기가 이 프로젝트의 존재 이유다.

## 문서 규칙 (2026-08-06 결정)

- **문서는 작업이 아니라 주제 단위** — 새 작업이 생기면 새 md를 만들지 말고 기존 문서의
  해당 절을 갱신한다: 구현 판단 → `docs/internals.md`, 설계 기준 변경 → `architecture.md`,
  운영 절차·화면 → `docs/operations.md`, 할 일 → `docs/TODO.md`.
- **설계 판단이 포함된 변경은 해당 문서 절 갱신까지가 구현 완료다** ("왜"를 git history에만
  남기지 않는다).
- 새 문서 신설은 예외적으로만: 기존 문서 어느 절에도 안 맞는 새 주제, 시점 기록
  (`incidents/`·`experiments/` — 건별 파일), 비대해진 문서의 분리. 신설 시 README 문서 표에
  링크를 추가한다.
- 세션 종료 시 미완 작업은 TODO.md 해당 항목에 진행 상태를 남긴다.

## 컨텍스트 절약 규칙

- architecture.md는 필요한 절만 읽는다 (절 번호가 본 문서 곳곳에 표기됨).
- ui-reference 프로토타입은 구현할 화면의 파일만 읽는다.
- 커넥터 설정 키는 추측하지 말고 해당 커넥터 공식 문서를 확인한다 (Debezium Oracle / Debezium JDBC sink / Apache Iceberg Kafka Connect).
