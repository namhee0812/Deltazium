# 장애 기록: 디스크 풀로 Kafka 브로커 사망 — 나흘간 미검출

앞선 세 건(08-04·08-13·08-18)이 전부 소스(Oracle·LogMiner) 쪽이었다면, 이번엔 파이프라인
한가운데(Kafka)가 죽었다. 더 문제였던 건 죽음 자체보다 **나흘간 아무도 몰랐다**는 것 —
Connect·backend·UI가 전부 떠 있어 겉보기엔 시스템이 살아있는 것처럼 보였고, UI는 테이블
목록이 "0건"으로 보이는 오해를 유발하는 증상만 냈다.

## 타임라인 (KST)

| 시각 | 사건 | 근거 |
|---|---|---|
| 08-20 09:51:03 | 디스크 풀 — checkpoint 파일 쓰기 실패, `KafkaStorageException` → 브로커 사망 | `logs/server.log` 말미: `replication-offset-checkpoint.tmp (No space left on device)` |
| 08-20 ~ 08-24 | 미검출 나흘. 그 사이 디스크 공간은 확보됨(원인 미상 — 08-24 시점 492G 중 360G 여유) | `df -h` |
| 08-24 18:33경 | UI 테이블 모니터링 0건 인지 → 진단 시작 | 사용자 보고 |
| 08-24 18:37 | `dzadmin infra start`로 Kafka 재기동 | watchdog 이전 수동 조치 |
| 08-24 18:39:37 | Connect 워커 브로커 재합류, 리밸런스 지연 대기 진입 | `connect.log` "Catching up to assignment's config offset" |
| 08-24 18:44:40 | 지연 리밸런스(기본 `scheduled.rebalance.max.delay.ms` 5분) 후 커넥터 재배정 | `connect.log` "Joined group at generation 123" |
| 08-24 18:45경 | 전 커넥터·task RUNNING, heartbeat offset 전진 확인 — dz-source가 08-20 offset SCN부터 정상 재개 (archive log 보존돼 있었음) | `__debezium-heartbeat.dz` offset 8317→8318 |

## 원인 사슬 — 각각이 독립된 결함

1. **디스크 풀 → 브로커 즉사.** 단일 노드라 브로커 하나 = 전체. Kafka는 스토리지 예외에
   보수적으로 자살하는 설계라 복구는 재기동뿐이다. **무엇이 디스크를 채웠는지는 미규명** —
   발견 시점엔 이미 공간이 확보돼 있어 추적 불가. `logs/` 5.4 GB(당시 최대 후보),
   `connect.log` 단일 파일 39 MB(로테이션 미적용) 확인.
2. **감시 부재 → 나흘 미검출.** `dzadmin`은 조회 도구일 뿐 감시·재기동이 없다.
3. **UI가 목록과 지표를 한 API에 묶음 → 오진 유발.** 테이블 목록의 원천은 PG(정상)인데
   `TablesPanel`이 행을 `/api/metrics/tables`에서만 채웠고, 이 API는 PG 목록에 Kafka 지표를
   합치다 AdminClient 호출에서 통째로 실패 — "테이블이 없다"로 보였다.
4. **AdminClient 타임아웃 미설정 → API가 매달림.** 기본 60초 타임아웃으로 HTTP 응답조차
   늦어(체감 무응답), 명확한 에러 대신 침묵을 반환했다.
5. **재기동 후에도 5분 공백.** 단일 워커에 의미 없는 lost-assignment 재배정 지연(기본 5분)
   때문에 커넥터들이 UNASSIGNED로 머물렀다.

## 파생 개선 (08-24, feature/ops-hardening)

| 결함 | 조치 |
|---|---|
| 감시 부재 | `deploy/watchdog.sh` — `dzadmin status` 파싱, DOWN인 infra/backend 자동 재기동, `logs/watchdog.log` 기록. crontab 5분 주기 등록 |
| 미검출 | 전역 **경고 센터** — `GET /api/system/warnings`(디스크 사용률 ≥85%·Kafka 연결·커넥터/task 상태) + UI 헤더 경고 칩·팝오버. backend 자체가 죽으면 클라이언트가 "backend 연결 끊김"을 합성 표시 |
| 목록·지표 결합 | `TablesPanel` 행 원천을 `/api/registrations`로 분리 — 지표 실패 시에도 목록 유지, 지표 셀만 "—" |
| 타임아웃 | `KafkaMetricsService` AdminClient에 `default.api.timeout.ms=5000` / `request.timeout.ms=4000` |
| 리밸런스 공백 | `connect-distributed.properties`에 `scheduled.rebalance.max.delay.ms=30000` |
| 검증 중 발견한 맹점 | 커넥터 RUNNING인데 task 0개(소비 정지)인 상태를 경고 센터가 못 잡던 것 → `connector-no-tasks` WARN 추가 |

## 남은 것

- **디스크를 채운 원인 규명** — 재발 시 경고 센터의 disk-usage WARN(85%)이 죽기 전에 잡는다.
  잡히면 그 시점에 `du` 스냅샷을 뜰 것.
- `connect.log` 로테이션 미적용 — 로그 설정 정리 필요 (TODO).
- 이번엔 운이 좋았다: 나흘 정지에도 소스 Oracle의 archive log가 offset SCN 구간을 보존하고
  있어 무손실 재개됐다. 보존 기간이 짧은 환경이었다면 08-04 장애(archive 유실 → 복구 레인)의
  재연이 됐다 — 정지 기간이 archive 보존 기간에 근접하면 경고하는 지표는 아직 없다.
