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

- [ ] 테이블별 incremental snapshot (Kafka signal) — 기동 중 테이블 추가 시 초기적재,
      테이블 단위 reload(Qlik per-table reload에 해당). architecture.md 10절 미결
- [ ] 컬럼 리네임의 적재 반영 방침 결정 — 스톡 sink 한계로 현재 저장만
      ("데이터 경로 자체 코드 금지" 규칙 개정 필요 여부 포함)
- [ ] changelog 파일 목록·row 미리보기 UI (SCN·op 컬럼 스팟체크)
- [ ] DLQ (현재 범위 외 선언 상태 — at-least-once 실패 격리가 필요해지면)

## 운영

- [ ] docker-compose 패키징 (베어메탈 안정화 후)
- [ ] UI 프로덕션 서빙 (vite build 산출물을 backend 정적 리소스 또는 nginx로)
