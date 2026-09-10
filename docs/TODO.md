# TODO — 백로그

우선순위·순서 미정. 결정된 것만 아래 마일스톤으로 승격한다.

## 모니터링

- [x] **장애 자동 감지·재기동(watchdog) + 경고 센터** (2026-08-24, 26-08-20 디스크 풀
      미검출 장애 재발 방지)
  - `deploy/watchdog.sh` — infra(pg/minio/kafka/connect)·backend DOWN 자동 재기동
    (crontab 등록은 사용자 몫, `docs/operations.md` 참고)
  - 헤더 경고 센터(`GET /api/system/warnings`) — 디스크 사용률·Kafka 연결·커넥터 상태
    3종, backend 다운 시에도 칩이 뜨도록 클라이언트 합성 경고 포함
  - `KafkaMetricsService` AdminClient에 5초 타임아웃 — 지표 API가 Kafka 다운 시
    60초+ 매달리지 않게
  - 남은 갭: watchdog·경고 센터는 감지·재기동까지다. 디스크가 실제로 가득 차는
    근본 원인(로그 보존 정책, 파티션 분리 등)은 미해결 — 아래 Prometheus 도입과
    함께 재검토
- [ ] **Prometheus + Grafana 도입** (2026-08-06 결정: 추후 학습 겸 도입)
  - Kafka·Connect에 JMX exporter 붙여 JVM(힙·GC)·커넥터 메트릭 노출
  - 현재 자체 경량 수집(1분 샘플러 → PG)과 병행 비교 후 대체 여부 판단
  - Debezium 커넥터 자체 메트릭(스냅샷·스트리밍 lag)도 JMX로 노출됨
- [ ] 소스 대비 캡처 지연(초 단위) 지표 — source.ts_ms 대비 수신 시각
- [ ] Oracle 쪽 지표(리두 생성량, LogMiner 세션 상태) — 원격 DB라 별도 수집 경로 필요

## 기능

- [ ] **다중 소스·다중 타깃** (2026-09-05 방향 확정 — architecture.md 2.2·5·6.5·8절에 반영 완료.
      순서대로 진행, 각 항목이 독립 마일스톤)
  - 기준: changelog = 소스·타깃 중립 계약. 소스 분기는 캡처 층(8절), 타깃 분기는 수렴 층(6절).
    2026-09-05 확정 결정: `_pos` struct {partition, offset} 1개 / namespace 단일 레벨
    `changelog_<prefix>` / 기존 changelog는 이전 없이 재등록(재스냅샷, **삭제 전 사용자 확인**) /
    UI는 시각·`_pos` 기본, SCN·LSN은 참고 텍스트 / rule-check.sh에 `source` 내부 필드 참조 차단 추가
  - [ ] **① changelog 중립 계약** — DW 없이 단독 가치(scn 정렬은 장기 트랜잭션에서 커밋 순서와 어긋남, 5.1)
    - **진행 상태 (2026-09-05)**: 코드 병합 완료(e0164e0, 테스트 140건 통과·통합 테스트 통과). 남은 것:
      실 배선 스모크(Oracle 테스트 테이블 DDL·DML 필요 — 사용자 실행), 기존 등록 테이블 4개
      해제·재등록(기존 changelog 삭제 수반 — 사용자 확인 후), 구 `dz-iceberg-sink` 커넥터 정리
      (backend 재기동 후 등록/해제 액션 1회로 `dz-iceberg-dz` 전환). 절차: operations.md
    - Iceberg sink: `KafkaMetadataTransform`(`field_name=_pos`, `nested=true` → topic/partition/offset/timestamp,
      공식 문서 확인 2026-09-05)로 `_pos` 부착 + `route-field=_pos.topic` 라우팅, 소스별 인스턴스로 전환,
      복구 토픽 구독 제외
    - backend: changelog 사전 생성 스키마에 `_pos`, namespace `changelog_<prefix>`, 복구 진입점
      SCN → 시각(한 파티션 앞부터), 복구 drawer·changelog 현황의 SCN 표시를 시각·`_pos`로 교체
    - recovery-job: 재생 정렬 `_pos` 파티션별 offset 순, 왕복 테스트에서 `_pos` 제외 처리
    - rule-check.sh: recovery-job·backend 수렴/복구 코드의 `source.scn` 등 참조 차단 (코드 전환과 동시에)
    - 기존 등록 테이블 해제·재등록 절차 (operations.md에 기록)
    - 확인: 소스 토픽 파티션 수(브로커 기본값) — 1이면 `_pos.partition`은 항상 0, 컬럼은 유지
  - [ ] **② 두 번째 소스·타깃: PostgreSQL** — 캡처 층 분기 증명 (2026-09-07 구체화·위임)
    - **진행 상태 (2026-09-07)**: 코드 구현 완료(feature/pg-source 병합, main). 라이브 전환
      완료 — backend 재기동·기존 등록 테이블 4개 해제·재등록·PostgreSQL 소스 준비까지
      메인 세션이 수행. **PG 소스 실 배선 스모크 실행** — PG→Oracle 타깃 적재, changelog
      `_pos`, 스키마 지문 감지(ADD COLUMN → 다음 주기 DETECTED, 초안 정확)까지 동작 확인.
      스모크 중 드러난 결함 3건은 feature/pg-source-fix에서 수정: ① FINGERPRINT ddl_text에
      요약+초안이 섞여 승인 시 ORA-00900(ddl_events id=39) — ddl_text는 실행문 전용,
      요약은 note로 분리. ② PG 캡처 롤에 database CREATE 권한이 없으면 publication 자동
      생성이 실패(`Unable to create filtered publication`) — 사전 점검·준비 스크립트에
      GRANT 추가. ③ 설정 3건(use.reduction.buffer, iceberg.control 토픽 소스별,
      offset.flush.interval.ms=10000)은 f703b7d로 이미 반영, 근거는 connectors/README.md·
      docs/internals.md "PG 소스 실 배선 스모크 결과"에 기록. 수정 병합(3517375) 후 재감지
      → 이벤트 40 승인 → 타깃 ALTER 적용 → sink 재개 → NOTE 컬럼 포함 행 적재까지 확인(2026-09-07
      14:00). 트리클 INSERT 24건 SRC/TGT 합계 일치. 잘못 저장된 이벤트 39는 거부 처리.
      **② 완료.** 남은 소소한 것: PG 소스의 스냅샷 notification 이벤트가 테이블명 자리에
      `dz-source`로 표기됨(레거시 라벨, 표시만) / RecoveryService.verify() 체크섬 PG 소스 미지원
      (PG 소스 복구 리허설 시 결정).
      결정 필요로 남은 것: RecoveryService.verify()의 체크섬(ORA_HASH)이 Oracle 전용이라
      PostgreSQL 소스의 SRC측 체크섬 검증은 아직 미지원(docs/internals.md 기록) — PG 소스
      복구 리허설에서 정합 검증을 어떻게 할지 결정 필요.
    - **소스 식별자를 커넥션 속성으로**: `db_connections.topic_prefix`(소스 커넥션 필수, 유일,
      `[a-z][a-z0-9_]*`, 기본값 = 이름 슬러그). 전역 `deltazium.topic-prefix` 제거. 기존 소스
      커넥션(orcl225)은 마이그레이션으로 `dz` 유지
    - **커넥터 이름**: `dz-source-<prefix>` · `dz-iceberg-<prefix>` · `dz-jdbc-sink-<prefix>-<suffix>` ·
      `dz-recovery-sink-<prefix>-<suffix>`. changelog namespace `changelog_<prefix>`(①과 동일).
      KafkaMetricsService consumer group 이름 동기화. **기존 `dz-source`·`dz-jdbc-sink-*`는 이름이
      바뀌므로 해제·재등록으로 전환**(Debezium offset이 커넥터 이름에 묶임 — 절차 operations.md)
    - **등록 키** `registered_tables` UNIQUE → (source_connection_id, schema_name, table_name).
      기동 시 멱등 마이그레이션
    - **DbType 분기**: POSTGRESQL 활성화, `jdbcUrl()` 타입별, 식별자 정규화(Oracle 대문자 / PG 그대로).
      딕셔너리·사전 점검을 `SourceDictionary` 인터페이스 + Oracle·PostgreSQL 구현으로.
      PG 점검: `wal_level=logical`, 캡처 롤 REPLICATION(또는 superuser)·테이블 SELECT, PK 존재,
      REPLICA IDENTITY FULL(미설정 시 ALTER 문 보여주고 승인 후 적용 — supp.log와 같은 UX),
      publication 자동 생성 권한. 항목·SQL은 Debezium PostgreSQL 문서로 확정
    - **source 템플릿 분리**: `source-oracle.json.tmpl`(현행 이동), `source-postgresql.json.tmpl`
      (PostgresConnector, pgoutput, slot/publication 이름 `dz_<prefix>`, autocreate filtered,
      snapshot.mode·notification·transaction metadata는 Oracle과 동일 구성). 설정 키는 공식 문서 확인
    - **DDL 감지 — 스키마 지문 비교** (PG는 schema change topic이 없음, 7절 개정): backend 상주
      consumer 1개(`assign`, group 없음)가 1분마다 감시 대상 파티션의 마지막 메시지 1건(tombstone이면
      최대 20건 거슬러)을 읽어 value.schema의 after struct 지문(필드명·타입·optional·파라미터 정렬
      해시)을 `registered_tables.schema_fingerprint`와 비교. 첫 지문은 이벤트 없이 저장. 변경 시
      ddl_events에 origin=FINGERPRINT로 기록(diff: 추가/삭제/타입변경 컬럼) + 타깃 DDL 초안
      (ADD/DROP COLUMN은 Debezium 타입→타깃 타입 소형 매핑으로 생성, 타입 변경은 초안 없이 확인만).
      감지 대상은 schema change topic이 없는 소스 타입만(DbType 플래그). `DdlEventParser`의
      `source.scn` 의존 제거(위치는 nullable 참고 문자열)
    - **deploy**: install-runtime.sh에 debezium-connector-postgres 3.6.0.Final 추가.
      PG 소스 준비 스크립트 `deploy/pg-source-setup.sh`(wal_level, 캡처 롤, 테스트 스키마
      `cdc_src` + PK 테이블) — 작성만, 실행은 사용자 확인 후
    - **UI**: 연결 카드 타입 선택(PG 필드 라벨), 위저드 사전 점검을 backend 점검 목록 기반 범용
      렌더링, 토폴로지 소스 노드 소스별, 테이블 그리드에 소스 표기
    - **docs**: architecture.md 4절 이름 규칙·7절 감지 방식·8절 PG 점검 확정치, operations.md
      재등록(커넥터 이름 전환) 절차, internals.md 지문 감지 구현 판단
    - 검증: 단위·통합 테스트, PG 소스 실 배선 스모크(사용자 실행 후) — PG→Oracle 타깃 + changelog `_pos`
  - [ ] **③ 저장소 프로파일: MinIO / R2** — R2 프로파일 = Cloudflare R2(10GB·egress 무료) +
        R2 Data Catalog(Iceberg REST) (2026-09-07 구체화·위임)
    - **R2 규격(공식 문서 확인 2026-09-07)**: REST 카탈로그 `type=rest`, `uri`(카탈로그 활성화 시 표시),
      `warehouse`(표시값), `token`(R2 API 토큰, R2+catalog 권한 "Admin Read & Write"; 읽기 전용
      클라이언트는 "Admin Read only"). Spark 예시가 S3 키 없이 동작 → 카탈로그가 S3 자격을 위임
      (vended credentials). S3 엔드포인트 `<account_id>.r2.cloudflarestorage.com`, 리전 `auto`.
      Snowflake는 `STORAGE_PROVIDER='S3COMPAT'` 외부 볼륨 + `CATALOG_SOURCE=ICEBERG_REST` bearer 통합
      공식 예시 있음(④ 전제 확인, 읽기 전용).
    - **카탈로그 속성 단일 진원지**: backend `IcebergProperties.catalogProperties()`가 프로파일별
      Iceberg 카탈로그 속성 맵을 만든다 — minio: 현행 JDBC(+S3FileIO·MinIO 키), r2: rest+uri+warehouse+
      token(+io-impl S3FileIO, `client.region=auto`, 위임 실패 대비 선택적 S3 키). 이 맵을
      ① backend 자신(`CatalogUtil.buildIcebergCatalog`, `JdbcCatalog` 직접 참조 제거),
      ② iceberg-sink 배포(템플릿의 catalog 블록 제거 → `iceberg.catalog.<key>`를 extraConfig로),
      ③ recovery-job 기동 인자(`catalog.<key>=<value>`, recovery-job도 CatalogUtil로) 세 곳이 공유
    - **설정 파일**: `deploy/env.sh`에 `DZ_STORAGE_PROFILE=minio`(기본), 존재하면 `deploy/env.local.sh`
      (git-ignore, R2 uri·warehouse·token·엔드포인트·선택적 키)를 source. `deploy/env.local.sh.example`
      제공. backend는 같은 환경변수를 읽는다(`deltazium.iceberg.profile` 등)
    - **deploy 분기**: r2 프로파일이면 start-infra가 MinIO·iceberg_catalog DB를 띄우지 않고,
      smoke-test·watchdog·dzadmin status도 프로파일을 따른다
    - **UI**: 연결 화면에 읽기 전용 "changelog 저장소" 카드 — `GET /api/system/changelog-storage`
      (profile·catalogType·catalogUri 호스트·warehouse/버킷·externallyReachable=profile==r2, 비밀값 없음)
      + `POST .../test`(namespace 목록 조회 + 데이터 파일 경로 접근). DW 타깃 사전 점검(8절)이 같은
      externallyReachable을 쓴다(④에서 연결) — **2026-09-10: 푸시 모델 확정으로 이 연결은 불필요.
      externallyReachable은 카드 표시용 정보로만 남긴다**
    - **검증**: 단위 테스트(프로파일별 속성 맵), minio 프로파일 통합 테스트 회귀, REST 프로파일은
      `org.apache.iceberg:iceberg-open-api` 테스트 픽스처(RESTCatalogServer, test scope)로 통합 테스트
      시도(불가 시 사유 기록), recovery-job 왕복 테스트. **R2 실 스모크는 사용자 계정 준비 후**:
      버킷·카탈로그 활성화·토큰 → env.local.sh → 프로파일 전환 → 테이블 재등록 → 커밋 시간 실측
    - 프로파일 전환은 changelog 이전이 따르는 설치 작업 — 절차를 operations.md에
    - **진행 상태(2026-09-07, feature/storage-profile)**: 코드 구현·단위 테스트·문서 반영
      완료(속성 단일 진원지, sink extraConfig 주입, recovery-job `catalog.<key>` 인자,
      deploy 분기 5종, UI changelog 저장소 카드+API). iceberg-open-api REST 픽스처는
      의존성 비용 과다로 보류(사유 internals.md) — 대체로 단위 테스트만 존재.
      **2026-09-08 병합(59a9b30)·라이브 확인**: minio 프로파일 무변경 재기동 — 커넥터 10개 RUNNING,
      changelog 조회(CatalogUtil 경로) 정상, 저장소 카드 API 비밀값 없음, 연결 테스트 OK(데이터
      경로 확인을 metadata.json 존재로 교정). **③ 완료.**
    - **2026-09-10 재정의**: DW 푸시 모델 확정으로 "R2 = SaaS DW 전제"는 소멸. 프로파일의 의미는
      "번들 MinIO(개발·PoC) / 외부 S3 호환 엔드포인트(프로덕션, 고객 운영 스토리지 BYO)"다. 코드상
      `r2` 프로파일은 외부 S3의 한 사례로 남긴다. R2 실 스모크는 **선택**(외부 S3 경로·REST 카탈로그
      검증용) — 계정 준비가 되면 하고, 안 해도 ④ 진행에 지장 없음. 프로파일명을 `external`로 일반화
      하는 리네임은 Helm 패키징 때 함께.
  - [ ] **④ DW 계열: Snowflake · Databricks — 푸시 모델 stage-and-merge** (2026-09-10 확정,
        architecture.md 6.5 개정. 설계 문서 v2의 당김 모델은 폐기)
    - **결정 근거**: 상용 CDC(Qlik Replicate·GoldenGate·Striim) 공통 관행 = 도구가 마이크로 배치를 DW
      스테이징에 밀어 넣고 MERGE. DW 컴퓨트가 고객 온프레미스 스토리지로 들어오는 구성은 운영·보안상
      불가. 고객은 DW 자격만 제공.
    - **DW apply 워커**(자체 코드, 절대 규칙 예외 — CLAUDE.md 갱신): DW 타깃 커넥션당 1 프로세스
      (recovery-job처럼 플레인 Java, backend가 기동·감시). Kafka 소비(소스 토픽 + 복구 토픽) → 배치
      (1분 또는 N MB) → DW 어댑터로 스테이징 bulk 적재 → MERGE(PK별 최신 1건 `_pos` 순, op='d'
      DELETE) → offset 커밋. Iceberg·`source.*` 미접근(rule-check 대상). 무상태.
    - **DW 어댑터 인터페이스**: `stage(batch) → merge(stagingTable, targetTable, keys) → cleanup`.
      Snowflake: 내부 스테이지 PUT(JDBC 드라이버) + COPY INTO 스테이징 + MERGE. Databricks: UC
      Volume Files API 업로드 + COPY INTO + MERGE (고객 클라우드 버킷 스테이징은 후속 옵션).
    - **타깃 옵션**: MERGE까지(기본) / 랜딩만(스테이징 테이블에 append, 고객이 DLT APPLY CHANGES 등으로
      수렴 — Databricks 생태계 관행).
    - **등록 분기**: DW 타깃 사전 점검(8절: 접속·스키마·스테이징 권한·warehouse 권한) → 최종·스테이징
      테이블 DDL 초안 승인 → 생성 → 워커 기동. lag = 워커 커밋 offset vs 토픽 end offset(기존 화면 통합).
    - **복구**: OLTP와 동일 재발행(6.1) — 워커가 복구 토픽을 추가 구독. 리허설(6.4 DW 시나리오) + MERGE
      멱등 증명(같은 배치 2회 → 1행).
    - **순서**: Snowflake 어댑터(트라이얼 계정) → 멱등 증명 → 복구 리허설 → Databricks 어댑터(무료
      에디션, UC Volume) → 등록 분기·lag → docs. 사전 확인: Snowflake JDBC PUT/COPY 권한, Databricks
      Files API 한도·COPY INTO, 무료 계정 제약.
    - 범위 밖: fan-in, DW 스키마 전파, 초 단위 스트리밍 ingest(Snowpipe Streaming 하이브리드), 고객
      클라우드 버킷 스테이징(Databricks 후속)
- [ ] 테이블별 incremental snapshot (Kafka signal) — 기동 중 테이블 추가 시 초기적재,
      테이블 단위 reload(Qlik per-table reload에 해당). architecture.md 10절 미결
- [ ] 컬럼 리네임의 적재 반영 방침 결정 — 스톡 sink 한계로 현재 저장만
      ("데이터 경로 자체 코드 금지" 규칙 개정 필요 여부 포함)
- [ ] changelog 파일 목록·row 미리보기 UI (SCN·op 컬럼 스팟체크)
- [ ] DLQ (현재 범위 외 선언 상태 — at-least-once 실패 격리가 필요해지면)

## 운영

- [ ] docker-compose 패키징 (베어메탈 안정화 후) — 개발·PoC용, 번들 MinIO·PostgreSQL·KRaft 포함
- [ ] **Kubernetes(Helm) 배포** (2026-09-10 방향, architecture.md 10절) — compose 다음 단계.
      제품은 무상태 컨테이너(제어면 API·워커·UI)만, 오브젝트 스토리지·메타데이터 DB·Kafka는 고객
      운영 것을 Secret으로 받는 BYO. 번들 MinIO는 프로덕션 차트 기본 off. 선행: 제어면 API·워커
      분리(무상태 경계), 저장소 프로파일명 `external` 일반화
- [ ] UI 프로덕션 서빙 (vite build 산출물을 backend 정적 리소스 또는 nginx로)
