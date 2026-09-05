# connectors — 커넥터 설정 템플릿

`{{var}}` 자리는 backend가 렌더링해 Connect REST(`POST /connectors`)로 배포한다.
설정 키는 전부 공식 문서에서 확인한 것만 사용 (Debezium 3.6 / Iceberg kafka-connect 1.11 기준, 2026-07-24 검증).

| 템플릿 | 커넥터 | 역할 |
|---|---|---|
| source.json.tmpl | Debezium Oracle source | SRC 캡처. `table.include.list` = 등록 테이블 목록 |
| jdbc-sink.json.tmpl | Debezium JDBC sink | 실 적재 (PK upsert 멱등 + delete) |
| iceberg-sink.json.tmpl | Apache Iceberg sink | changelog append 적재 (소스별 인스턴스 1개 — 4절) |
| recovery-sink.json.tmpl | Debezium JDBC sink | jdbc-sink와 동일 설정, 구독 토픽만 복구 토픽. 평시 정지 |

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

## 미결 (마일스톤 2·3에서 실배선으로 확정)

1. ~~envelope → changelog 스키마 변환~~ **해소(2026-07-26)**: 5.1절을 envelope-as-is로 개정.
   changelog 테이블은 backend가 사전 생성(기본 골격 + `truncate(source.ts_ms, 1일)` 파티션),
   sink는 `evolve-schema-enabled=true`로 before/after를 첫 레코드에서 채운다.
   라우팅: **2026-09-05 개정** — 소스별 iceberg-sink 인스턴스(`dz-iceberg-<prefix>`) +
   `route-field=_pos.topic` + 테이블별 route-regex(토픽 이름 정확 일치,
   `^<prefix>\.<SCHEMA>\.<TABLE>$`). 종전 `route-field=source.table`의 동명 테이블 제약 해소.
   **주의: JdbcCatalog는 catalog_name으로 스코핑 — backend와 sink 모두 "iceberg" 이름 사용.**
2. ~~jdbc-sink 토픽→타깃 테이블 매핑~~ **확정(2026-07-25)**: RegexRouter로 토픽명에서
   `<prefix>.<schema>.` 접두를 제거해 테이블명만 남기고, apply는 **TARGET 연결 계정의
   기본 스키마**에 수행한다 (`collection.name.format=${topic}`). 스키마 한정자 문제 회피 —
   타깃 스키마 = TARGET 접속 계정 스키마라는 규약. 실 Oracle 배선에서 최종 검증 예정.
3. source의 `database.pdb.name`: 대상 Oracle이 CDB/PDB 구성일 때만 추가.
