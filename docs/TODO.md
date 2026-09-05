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

- [ ] **DW 타깃(Snowflake·Databricks) 적재** (2026-09-02~05 설계 논의 — 방향 합의, 세부 결정 대기)
  - 설계 문서(합의 근거·다이어그램·개정안 전체): https://claude.ai/code/artifact/581e7a1e-b7ca-4e0b-b6d6-556d8464c3cc (v2)
  - **주 안 — changelog 겸용**: 기존 Iceberg changelog를 DW 랜딩으로 겸용.
    Snowflake/Databricks가 외부 Iceberg 카탈로그로 changelog를 직접 읽어
    **1분 주기 집합 MERGE**(PK별 최신 1건 선별)로 최종 테이블 수렴. 커넥터 추가 0개.
  - 합의된 것: 행 단위 upsert 배제(Debezium JDBC sink는 dialect도 없음) ·
    append+집합 MERGE · 신선도 1분 · 증분 기준 SCN 워터마크(ts_ms 파티션 프루닝 + scn 캐스팅) ·
    내부 MinIO 비노출 → changelog를 클라우드로 이전 · 파일 포맷은 parquet(Iceberg 유지, raw 파일 격하 안 함)
  - 선행 과제 2건: ① changelog 스토리지 이전 — Cloudflare R2 제안(10GB·egress 무료)
    ② 카탈로그 JDBC(PG) → Iceberg REST 전환(R2 Data Catalog) — DW가 JDBC 카탈로그에 못 붙음.
    sink·recovery-job·backend는 catalog 설정만 변경. HMS는 미사용·미도입.
  - 남은 결정: 기존 changelog 데이터 이전 여부 / MERGE 오브젝트 배포 주체(backend vs 수동) /
    보존 정책 통합 / DW DDL 전파(별도 설계)
  - 검증 순서: R2 Data Catalog·Snowflake catalog integration·Databricks federation 현황 확인(미확인)
    → 카탈로그 전환 스모크 + recovery-job 왕복 테스트 → DW 읽기 배선 → MERGE 멱등 증명
    → 복구 리허설 DW판 → backend·UI
  - 확정 시 architecture.md 개정 필요: "apply 단일 경로" → 타깃 계열별(OLTP upsert / DW MERGE),
    Iceberg 역할에 "DW 랜딩 겸용" 추가, 3·5절 카탈로그·스토리지 서술
- [ ] 테이블별 incremental snapshot (Kafka signal) — 기동 중 테이블 추가 시 초기적재,
      테이블 단위 reload(Qlik per-table reload에 해당). architecture.md 10절 미결
- [ ] 컬럼 리네임의 적재 반영 방침 결정 — 스톡 sink 한계로 현재 저장만
      ("데이터 경로 자체 코드 금지" 규칙 개정 필요 여부 포함)
- [ ] changelog 파일 목록·row 미리보기 UI (SCN·op 컬럼 스팟체크)
- [ ] DLQ (현재 범위 외 선언 상태 — at-least-once 실패 격리가 필요해지면)

## 운영

- [ ] docker-compose 패키징 (베어메탈 안정화 후)
- [ ] UI 프로덕션 서빙 (vite build 산출물을 backend 정적 리소스 또는 nginx로)
