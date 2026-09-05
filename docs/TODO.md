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
    - Iceberg sink: `KafkaMetadataTransform`(`field_name=_pos`, `nested=true` → topic/partition/offset/timestamp,
      공식 문서 확인 2026-09-05)로 `_pos` 부착 + `route-field=_pos.topic` 라우팅, 소스별 인스턴스로 전환,
      복구 토픽 구독 제외
    - backend: changelog 사전 생성 스키마에 `_pos`, namespace `changelog_<prefix>`, 복구 진입점
      SCN → 시각(한 파티션 앞부터), 복구 drawer·changelog 현황의 SCN 표시를 시각·`_pos`로 교체
    - recovery-job: 재생 정렬 `_pos` 파티션별 offset 순, 왕복 테스트에서 `_pos` 제외 처리
    - rule-check.sh: recovery-job·backend 수렴/복구 코드의 `source.scn` 등 참조 차단 (코드 전환과 동시에)
    - 기존 등록 테이블 해제·재등록 절차 (operations.md에 기록)
    - 확인: 소스 토픽 파티션 수(브로커 기본값) — 1이면 `_pos.partition`은 항상 0, 컬럼은 유지
  - [ ] **② 두 번째 소스·타깃: PostgreSQL** — 캡처 층 분기 증명
    - 등록 키 (source_id, schema, table) 전환, connections의 db_type별 사전 점검 목록(8절)
    - Debezium PostgreSQL source 템플릿, JDBC sink PostgreSQL 타깃
  - [ ] **③ 저장소 프로파일: MinIO / R2** — R2 프로파일 = Cloudflare R2(10GB·egress 무료) +
        R2 Data Catalog(Iceberg REST)
    - `deploy/env.sh`·backend 설정을 프로파일화, R2 프로파일에서 MinIO·iceberg_catalog DB 미기동
    - 연결 화면에 읽기 전용 "changelog 저장소" 카드(프로파일·버킷·카탈로그·외부 접근 가능 여부·연결 테스트)
    - 검증: 카탈로그 전환 스모크 + recovery-job 왕복 테스트, 인터넷 너머 커밋 시간 실측
    - 프로파일 전환은 changelog 이전이 따르는 설치 작업 — 절차를 operations.md에
  - [ ] **④ DW 계열: Snowflake · Databricks** (설계 문서 v2:
        https://claude.ai/code/artifact/581e7a1e-b7ca-4e0b-b6d6-556d8464c3cc — 단, 증분 기준
        "SCN 워터마크"와 복구 "재발행 → 중복 append"는 2026-09-05 논의로 `_pos` 워터마크·되감기로 대체됨)
    - 사전 확인(미확인): R2 Data Catalog 상태·한도, Snowflake catalog integration 외부 REST 지원·인증,
      Databricks(serverless 포함) Iceberg REST federation, 외부 Iceberg 테이블 증분 읽기(stream) 지원
    - 남은 결정: MERGE 오브젝트 배포 주체(backend 렌더링·배포 vs SQL 제공 후 수동), DW용 읽기 전용
      카탈로그 토큰 분리, "1분"의 정의(MERGE 주기 vs end-to-end)
    - 순서: DW 읽기 배선 → MERGE 멱등 증명 → 복구 리허설 DW판(6.4) → backend 등록 분기(DW 자격만)·lag 통합
    - 범위 밖: fan-in, DW 스키마 전파, 초 단위 스트리밍 ingest (10절·6.5)
- [ ] 테이블별 incremental snapshot (Kafka signal) — 기동 중 테이블 추가 시 초기적재,
      테이블 단위 reload(Qlik per-table reload에 해당). architecture.md 10절 미결
- [ ] 컬럼 리네임의 적재 반영 방침 결정 — 스톡 sink 한계로 현재 저장만
      ("데이터 경로 자체 코드 금지" 규칙 개정 필요 여부 포함)
- [ ] changelog 파일 목록·row 미리보기 UI (SCN·op 컬럼 스팟체크)
- [ ] DLQ (현재 범위 외 선언 상태 — at-least-once 실패 격리가 필요해지면)

## 운영

- [ ] docker-compose 패키징 (베어메탈 안정화 후)
- [ ] UI 프로덕션 서빙 (vite build 산출물을 backend 정적 리소스 또는 nginx로)
