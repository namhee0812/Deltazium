# 2026-09-28 — SingleStore 타깃 실배선 검증

## 목적

SingleStore를 OLTP 타깃 계열로 붙일 수 있는지 실측한다. 소스 지원은 범위 밖(별도 검토, TODO).

## 환경

- `ghcr.io/singlestore-labs/singlestoredb-dev`(라이선스 키 불필요, 개발용 라이선스 내장)를
  **podman**(이 서버에 docker 없음, rootless)으로 기동. 컨테이너 `singlestoredb-dev`,
  호스트 포트 3307(DB)·8081(Studio)·9002(Data API), 볼륨 `singlestore_data`. 서버 9.0.44.
  구 `singlestore/cluster-in-a-box` 이미지는 라이선스 키가 필요하다(받아는 뒀으나 미사용).
- CLI는 컨테이너 번들 클라이언트: `podman exec -it singlestoredb-dev singlestore -u root -p`
  (호스트에 mysql 클라이언트 없음). 접속 정보는 `~/deltazium-runtime/conf/secrets.env`의
  `SINGLESTORE_*`.
- 소스는 기존 PG 소스 `nhtest_src`(5433/`source`), 테스트 테이블 `cdc_tmp.s2_demo`
  (PK id, text, numeric(12,2), int, timestamptz).

## 사전 실측 (구현 전)

| 항목 | 결과 |
|---|---|
| 버전 보고 | `@@version` = **5.7.32**, product name "MySQL" → Debezium JDBC sink가 MySQL dialect 사용 |
| upsert | PK 있는 COLUMNSTORE(기본) 테이블에서 `INSERT ... ON DUPLICATE KEY UPDATE` 정상 |
| delete | 정상 |
| JDBC | `mysql-connector-j 9.1.0`(sink 플러그인에 이미 존재) 접속·`getColumns`·`getPrimaryKeys` 정상. 단 `getPrimaryKeys`는 **catalog 인자에 DB명을 넣어야** 한다(schema 인자는 무시, catalog=null이면 내부 `cluster` DB를 훑다가 권한 오류) |
| 식별자 | **대소문자 보존** — `CREATE TABLE MixedCase` 후 `select * from mixedcase`는 1146 오류. Oracle(대문자 폴딩)·PostgreSQL(소문자 폴딩)과 다른 제3 규칙 |

## 구현 (feature/singlestore-target, 5496204 병합)

`DbType.SINGLESTORE`(타깃 전용 `sourceCapable=false`), `foldIdentifier` 3분기,
`quoteIdentifier`(백틱), 타깃 컬럼 조회(`SingleStoreDictionaryService`), 연결 테스트·DDL 실행·
체크섬 경로의 드라이버 타임아웃 분기, 타입 매핑, UI 역할 필터. 상세는 internals.md.

## 라이브 검증

| 단계 | 결과 |
|---|---|
| 연결 등록·테스트 | `s2_tgt`(SINGLESTORE/TARGET) 등록, 테스트 ok `5.7.32` |
| 소스로 등록 시도 | HTTP 400 "SingleStore는 소스로 지원하지 않는다(타깃 전용)" — 의도대로 차단 |
| CREATE 초안 | ``CREATE TABLE `cdc_tgt`.`s2_demo` (`id` BIGINT, `name` TEXT, `price` DECIMAL(65,30), `qty` INT, `created_at` DATETIME(6), PRIMARY KEY (`id`))`` — 백틱 인용, SingleStore 타입 |
| 등록(createTarget=true) | 타깃 테이블 자동 생성 후 등록 성공 |
| 초기 스냅샷 | 21행 적재, 소스와 건수 일치 |
| 라이브 DML | INSERT 201 → UPDATE 2회(연속) → DELETE id=1 모두 반영(최종 `live-upd/9`, id=1 삭제됨, 총 21행) |
| 정합 검증 | `{"sourceCount":21,"targetCount":21,"match":true,"checksumSupported":false}` — 이종 조합이라 행수만 비교 |

## 관찰·제약

1. **`DATETIME`에는 타임존이 없다.** PG `timestamptz` 값이 UTC로 저장된다(소스 KST 01:12 →
   SingleStore `16:12` 전일). JDBC sink의 `database.time_zone` 기본값(UTC)을 따른 결과로,
   PG 타깃(timestamptz 보존)과 표시가 달라진다. 타깃별 시각 해석을 문서에 명시해야 한다.
2. **DECIMAL 매핑이 거칠다.** 소스 `numeric(12,2)`가 `DECIMAL(65,30)`(SingleStore 최대치)로
   생성된다 — 절삭은 없지만 표시·저장이 과하다. 길이·정밀도 보존은 타입 매핑의 기존 미해결
   항목(internals.md)과 같은 줄기다.
3. **기동 중인 소스 커넥터에 테이블을 추가하면 초기 스냅샷이 없다.** s2_demo를 처음 등록했을
   때 토픽 offset이 0이었고 등록 이후 변경분만 흘렀다. 스냅샷을 보려면 해제·재등록으로 소스
   커넥터를 새로 만들어야 했다(TODO "테이블별 incremental snapshot"이 이 갭).
4. **재스냅샷 API는 이 환경에서 쓸 수 없다.** `ResnapshotOrchestrator.start`가 등록 테이블
   전체의 소스 커넥션이 하나일 때만 동작하는데 현재 소스가 3개다(설계상 단일 소스 전제,
   TODO ② 기록). 이번 검증은 해제·재등록으로 우회했다.
5. 컬럼 리네임·비활성 매핑(`field.include.list`), DDL 승인 워크플로는 이번에 검증하지 않았다.
   특히 **Oracle 소스 + SingleStore 타깃의 schema change topic DDL 승인은 미지원**
   (`DdlEventService.rewriteForTarget`이 Oracle 원문 DDL을 재사용하는 구조 — TODO 등재).

## 남긴 상태

- 컨테이너 `singlestoredb-dev` 가동, DB `cdc_tgt`에 `orders`(수동 생성 테스트용)·`MixedCase`
  (대소문자 확인용)·`s2_demo`(21행) 존재.
- 연결 `s2_tgt`(id 9), 등록 `cdc_tmp.s2_demo`(id 34) → SingleStore. PG 소스의 orders·items는
  35·36으로 재등록(PG 타깃). nhtest_src 커넥터는 이번 검증으로 offset이 리셋됐다.
