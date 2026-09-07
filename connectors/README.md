# connectors — 커넥터 설정 템플릿

`{{var}}` 자리는 backend가 렌더링해 Connect REST(`POST /connectors`)로 배포한다.
설정 키는 전부 공식 문서에서 확인한 것만 사용 (Debezium 3.6 / Iceberg kafka-connect 1.11 기준, 2026-07-24 검증).

| 템플릿 | 커넥터 | 역할 |
|---|---|---|
| source-oracle.json.tmpl | Debezium Oracle source | Oracle 소스 캡처. `table.include.list` = 그 소스의 등록 테이블 목록 |
| source-postgresql.json.tmpl | Debezium PostgreSQL source | PostgreSQL 소스 캡처 (**2026-09-07 추가**, 다중 소스·다중 타깃 ②) — pgoutput, publication 자동 생성 |
| jdbc-sink.json.tmpl | Debezium JDBC sink | 실 적재 (PK upsert 멱등 + delete) |
| iceberg-sink.json.tmpl | Apache Iceberg sink | changelog append 적재 (소스별 인스턴스 1개 — 4절) |
| recovery-sink.json.tmpl | Debezium JDBC sink | jdbc-sink와 동일 설정, 구독 토픽만 복구 토픽. 평시 정지 |

RegistrationService가 소스 커넥션의 `dbType`으로 `source-oracle`/`source-postgresql` 중 하나를
고른다(DictionaryRouter와 같은 분기 지점, architecture.md 8절 "소스별 분기가 공식적으로 존재하는
유일한 자리"). 인스턴스 이름·개수 규칙은 4절 참고 — source·iceberg-sink는 소스 커넥션당 1개.

## source-postgresql 확정 선택의 근거 (2026-09-07, 다중 소스·다중 타깃 ②)

Debezium PostgreSQL 커넥터 3.6/stable 문서 확인(WebSearch 요약 기준 — 원문 페이지가 403으로
직접 fetch 불가했음):
https://debezium.io/documentation/reference/stable/connectors/postgresql.html

- `plugin.name=pgoutput`: PostgreSQL 10+ 네이티브 논리 복제 플러그인 — 별도 서버 확장 설치 불필요.
- `publication.autocreate.mode=filtered`: `table.include.list`에 있는 테이블만 묶은 publication을
  커넥터가 자동 생성한다. **2026-09-07 PG 소스 실 배선 스모크로 확정**: 테이블 소유권과는
  별개로 연결 계정에 **database CREATE 권한**이 있어야 한다 — 없으면 source task가
  `Unable to create filtered publication dz_pg`로 죽는다(`GRANT CREATE ON DATABASE`로 해결).
  `PostgresDictionaryService.privilegeChecks`가 `has_database_privilege(..., 'CREATE')`로
  이 항목을 blocking 사전 점검에 포함하고, `deploy/pg-source-setup.sh`가 캡처 롤에
  `GRANT CREATE ON DATABASE`를 부여한다. `slot.name`·`publication.name`은
  `dz_<topic.prefix>`로 소스별로 겹치지 않게 한다.
  `topic.prefix`가 소스 식별자이므로 슬롯·publication 충돌은 사실상 없다.
  Debezium은 `snapshot.mode` 값(initial/no_data/initial_only/never)을 Oracle 커넥터와 공유하는
  이름으로 지원해 backend가 두 템플릿에 같은 값("initial"/"no_data")을 넘길 수 있다.
- **schema change topic·schema history 없음** — PostgreSQL 커넥터는 DDL을 로그에서 파싱하지
  않고 논리 복제 프로토콜 + JDBC introspection으로 스키마를 얻는다. 그래서
  `include.schema.changes`·`schema.history.internal.*` 설정 자체가 없다(Oracle 템플릿에서
  그대로 가져오지 말 것). DDL 감지는 스키마 지문 비교로 대체한다(SchemaFingerprintService, 7절).
- `notification.enabled.channels`/`provide.transaction.metadata`는 Debezium 엔진 공용 기능이라
  Oracle과 동일하게 설정한다.
- REPLICA IDENTITY FULL은 테이블 DDL(연결 대상 DB에서 실행)이지 커넥터 설정이 아니다 —
  등록 사전 점검에서 미설정 시 승인 후 적용한다(Oracle supplemental logging과 같은 UX, 8절).

## 확정 선택의 근거

- `log.mining.strategy=online_catalog` (**2026-07-30 변경**): 원래 redo_log_catalog였으나
  공유 dev Oracle에서 ORA-1371(complete dictionary not found) 재시도 루프로 스트리밍이
  멈추는 문제가 재발해 전환. schema change 이벤트(DDL 승인 워크플로 전제)는 **두 전략 모두
  발행되므로** 7절 워크플로는 유지된다. online_catalog의 제약은 "DDL 직전 redo를 DDL 이후
  딕셔너리로 해석할 수 있다"는 것 — 실시간 스트리밍에선 그 창이 초 단위라 수용.
  부수 이점: 마이닝 빠름, 딕셔너리 redo 기록 없음, DBMS_LOGMNR_D 권한 불필요해짐(grant는 유지 무방).
- `internal.log.mining.log.query.max.retries=20` (**2026-08-19 추가**): 기본 5회(마지막 검사가
  스위치 후 +31s)는 이 DB의 4 GB redo 아카이빙(~50s)보다 짧아 로그 스위치 때 확률적으로 task가
  죽었다. 스위치 직후 직전 시퀀스는 online(CURRENT만 수집)에도 archived(완료 후 등록)에도 없는
  창이 생기기 때문. **공식 문서에 없는 internal 설정** — `Field.createInternal`이 `internal.`
  접두를 붙이므로 접두 없이 넣으면 조용히 무시된다(1차 조치가 그렇게 무효였음). 버전 업 시 키
  존재 재확인. 상세: `docs/incidents/2026-08-18-log-switch-archive-delay.md`.
- `schema.evolution=none` (jdbc-sink): 스키마 변경은 7절 승인 워크플로가 처리한다. sink가 임의로 타깃 DDL을 치면 안 됨.
- `iceberg.tables.auto-create-enabled=false`: changelog 테이블 스키마는 5절에 고정 —
  테이블은 backend가 명시 스키마로 생성하고 sink는 append만 한다.
- `iceberg.control.commit.interval-ms=60000`: 기본 5분은 토이 검증 피드백에 너무 길다.
- `transforms.pos`(`org.apache.iceberg.connect.transforms.KafkaMetadataTransform`, **2026-09-05
  추가** — 다중 소스·다중 타깃 ① changelog 중립 계약, architecture.md 5.1절): `field_name=_pos`,
  `nested=true`로 레코드 최상위에 `_pos {topic, partition, offset, timestamp}` struct를 부착.
  클래스명·설정 키(`field_name`/`nested`)는 커넥터 플러그인 jar(iceberg-kafka-connect-transforms)를
  `unzip -l`·`javap`로 직접 확인한 것(공식 문서에 설정 키가 명확히 나열돼 있지 않음).
- `iceberg.tables.route-field=_pos.topic` (**2026-09-05 변경, 종전 `source.table`**): 토픽 이름
  기준 라우팅으로 전환 — 동명 테이블이 다른 스키마에 있어도(=다른 토픽) 충돌하지 않는다.
  라우팅과 위치 컬럼 부착을 같은 SMT(`transforms.pos`)가 제공한다.
- `use.reduction.buffer=true` (jdbc-sink·recovery-sink, **2026-09-07 PG 소스 스모크에서 실측
  추가**): Debezium JDBC sink의 MERGE 기반 upsert(Oracle dialect)는 한 배치를 통째로 평가해
  MERGE를 세우는데, 같은 PK의 INSERT+UPDATE(또는 INSERT+INSERT)가 한 배치에 같이 들어오면
  중복 키로 `ORA-00001`이 난다. `use.reduction.buffer=true`는 배치 안에서 같은 키의 이벤트를
  하나로 합쳐(reduce) MERGE에 넘기므로 이 충돌을 없앤다(Debezium JDBC sink 문서의 MERGE
  dialect 주의사항). PK가 짧은 주기로 여러 번 바뀌는 트래픽(초기 스냅샷 직후 몰아치는 갱신
  등)에서 특히 중요 — 토이 볼륨에서도 실제로 발생을 확인했다.
- `iceberg.control.topic=control-iceberg-<prefix>` (iceberg-sink, **2026-09-07 PG 소스 스모크에서
  실측 추가**, 기본값은 고정 이름 `control-iceberg`): 코디네이터-태스크 간 커밋 프로토콜이
  쓰는 내부 토픽이다. 소스가 여러 개인데 이 토픽을 공유하면, 새로 배포되는 인스턴스의 태스크가
  **다른 소스의 옛 control 메시지 백로그까지 처음부터 재생**하느라 첫 커밋 응답이 수 분
  늦어진다(실측). `RegistrationService.deploySource`가 소스 topic.prefix로 이름을 나눠 배포한다
  — control 토픽도 데이터 토픽과 같은 원칙(소스별 격리, 4절)을 따른다.

## 미결 (마일스톤 2·3에서 실배선으로 확정)

1. ~~envelope → changelog 스키마 변환~~ **해소(2026-07-26)**: 5.1절을 envelope-as-is로 개정.
   changelog 테이블은 backend가 사전 생성(기본 골격 + `truncate(source.ts_ms, 1일)` 파티션),
   sink는 `evolve-schema-enabled=true`로 before/after를 첫 레코드에서 채운다.
   라우팅: **2026-09-05 개정** — 소스별 iceberg-sink 인스턴스(`dz-iceberg-<prefix>`) +
   `route-field=_pos.topic` + 테이블별 route-regex(토픽 이름 정확 일치,
   `^<prefix>\.<SCHEMA>\.<TABLE>$`). 종전 `route-field=source.table`의 동명 테이블 제약 해소.
   **주의: JdbcCatalog는 catalog_name으로 스코핑 — backend와 sink 모두 "iceberg" 이름 사용.**
   **2026-09-07 변경 (저장소 프로파일 MinIO/R2, TODO ③)**: `iceberg.catalog.*` 9개 키(catalog-impl·
   uri·jdbc.user/password·warehouse·io-impl·s3.*·client.region)를 템플릿에서 제거했다. backend의
   `RegistrationService.deploySource`가 `IcebergProperties.catalogProperties()`(프로파일별 카탈로그
   속성 맵 — minio: JDBC+S3FileIO, r2: REST+토큰)를 `iceberg.catalog.<key>` 접두로 extraConfig에
   병합해 배포 시점에 채운다. 템플릿을 프로파일별로 나누지 않는 이유: 카탈로그 접속 정보는
   설치 프로파일에 속하지 커넥터 정의에 속하지 않는다(3절) — 템플릿은 두 프로파일에서 동일.
2. ~~jdbc-sink 토픽→타깃 테이블 매핑~~ **확정(2026-07-25)**: RegexRouter로 토픽명에서
   `<prefix>.<schema>.` 접두를 제거해 테이블명만 남기고, apply는 **TARGET 연결 계정의
   기본 스키마**에 수행한다 (`collection.name.format=${topic}`). 스키마 한정자 문제 회피 —
   타깃 스키마 = TARGET 접속 계정 스키마라는 규약. 실 Oracle 배선에서 최종 검증 예정.
3. source의 `database.pdb.name`: 대상 Oracle이 CDB/PDB 구성일 때만 추가.
