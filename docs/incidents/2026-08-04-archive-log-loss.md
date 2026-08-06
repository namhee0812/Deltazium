# 장애 기록: archive log 소실로 인한 캡처 정지 (나흘 미감지)

실제 발생한 장애의 타임라인·원인·복구·파생 개선. 이 프로젝트에서 가장 많은 기능이
이 장애 하나에서 파생됐다.

## 타임라인

| 시각 | 사건 |
|---|---|
| 07-31 17:00 | Connect 워커 재기동 → dz-source task가 저장된 offset SCN(31,074,946,369)부터 재개 시도 → 해당 SCN을 담은 archive log가 이미 삭제됨 → `LogFileNotFoundException: re-snapshot is required`, task FAILED |
| 07-31 ~ 08-04 | **나흘간 미감지.** connector는 RUNNING이라 UI 전부 초록, 발행이 없으니 sink lag도 0 — "완벽한 평온"으로 보임 |
| 08-04 | 사용자가 "CDC가 안 되는데 UI는 정상"을 발견 → 진단 시작 |
| 08-04 | 재스냅샷으로 복구(수동 절차). 이후 고아 행 발견 → truncate 재구축 기능 구현 → 실전 실행(10.5만 행, 50초) → 정합 검증으로 종결 |

## 진단 경로 (재현 가능한 순서)

1. `GET :8083/connectors?expand=status` → **connector RUNNING / task FAILED** 불일치 발견
2. status의 task `trace` → `LogFileNotFoundException ... re-snapshot is required` (원인 확정)
3. connect 로그 grep → 최초 발생 시각 07-31 17:00 확정
4. 메트릭 API 교차 검증 → 발행 정지 상태에서 lag 0은 "정상"이 아니라 "무소식"이었음

## 근본 원인

- 공유 dev Oracle의 archive log 정리(RMAN)는 우리 사정과 무관하게 돈다.
- 캡처가 멈춘 사이 재개 지점 SCN이 온라인 리두 윈도를 벗어났고, 그 구간의 archive마저
  삭제 → LogMiner가 요구하는 "재개 SCN부터의 빈틈없는 로그 체인"이 성립 불가.
- 운영 수칙: **archive 보존 기간 > 커넥터 최대 허용 다운타임.**
- 갭 구간의 변경은 redo가 소멸했으므로 재마이닝 불가, changelog에도 없다(캡처가 죽어
  발행 자체가 없었으니). 유일한 복구는 재스냅샷 — 단, 갭 중 소스에서 DELETE된 행은
  스냅샷이 못 지우므로(현재 행만 upsert) 타깃에 고아로 남는다. 실측: 두 테이블에
  +1,225 / +1,012행. 완전 동일화는 truncate 후 재적재만이 보장.

## 왜 나흘간 안 보였나 — 이중 맹점

1. UI가 connector 상태만 보고 task 상태를 안 봄 (가장 흔한 장애 형태가 사각지대)
2. 헬스워처가 "전이"만 기록 — backend가 장애 이후에 기동되면 첫 관측은 기준선 처리돼
   이벤트가 안 남음

## 파생 개선 (전부 이 장애가 계기)

- task 포함 상태 판정(`effectiveState`) — 토폴로지·요약·행 배지 전부
- 헬스워처: 관측 시작 시점에 이미 FAILED여도 이벤트 기록
- 캡처 장애 배너 (감지 시각·원인 한 줄·전체 trace·[복구 시작])
- 행별 [재시도] (KIP-745 onlyFailed restart)
- 재스냅샷 워크플로: 단계형 상태 기계 + truncate 완전 재구축(승인·권한 홀드·count=0 게이트)
- Debezium notification 기반 스냅샷 진행 실측
- 로그 일 단위 정리(진단 중 시간별 수백 파일의 비용을 체감)

## 교훈

- **lag 0은 "따라잡았다"와 "아무것도 안 온다"를 구분하지 못한다.** 발행량과 함께 봐야
  한다 (대시보드의 발행 vs apply 차트가 이 교훈의 산물).
- 상태 API의 계층(connector/task)을 끝까지 내려가서 봐야 한다.
- 복구 수단의 계층: 재시도(retention 내) → S3 재발행(retention 초과) →
  재스냅샷(캡처 갭) → truncate 재구축(완전 동일화).
