# 장애 기록: 유휴 트래픽 중 offset SCN 유실 — 08-04 장애의 재발 (변종)

[2026-08-04 장애](2026-08-04-archive-log-loss.md)와 같은 종착점(`LogFileNotFoundException`,
offset SCN 31,087,514,551), 다른 경로. 그때의 파생 개선 7건은 전부 **감지·복구**였고
**예방이 빠져 있었다** — 이번에 그 구멍이 그대로 재현됐다.

## 08-04와 무엇이 다른가

| | 08-04 | 이번 |
|---|---|---|
| 트리거 | Connect 워커 재기동 (커넥터 **다운 중** SCN 유실) | 캡처 대상 무트래픽 (커넥터 **RUNNING인 채** offset 정체) |
| 당시 수칙의 커버 | "보존 기간 > 최대 다운타임" — 이 경우를 커버 | **커버 못 함** — 다운타임이 0이어도 발생 |

## 메커니즘 (결정론적 재현 조건)

Debezium은 offset을 **이벤트를 발행할 때만** 영속화한다. 캡처 대상 테이블에
트래픽이 없으면 마이닝 위치는 내부적으로 전진해도 offsets 토픽은 안 움직인다.
그동안 Oracle은 유휴여도 redo를 만들고(SMON·MMON·AWR), archive는 보존 정책대로
지워진다. 따라서:

```
유휴 기간 > archive 보존 기간  (heartbeat 없음)  →  100% 재현
```

예: 주말 이틀 무트래픽 + 보존 1일 → 월요일마다 task FAILED. 확률이 아니라 확정이다.

## 예방책 — heartbeat (이번 조치)

Debezium 공식 문서(3.6, Oracle)가 이 상황의 해법으로 명시한 설정:

- **non-CDB 모드**(우리: `database.dbname=orcl`, `database.pdb.name` 없음):
  `heartbeat.interval.ms`만으로 충분. 주기적으로 heartbeat 메시지를 발행하면서
  현재 읽기 SCN이 offset으로 flush된다.
- **CDB(PDB) 모드였다면**: `heartbeat.action.query`(heartbeat 테이블에 주기 DML)까지
  필수 — PDB 내부 변경을 인위적으로 만들어야 offset이 전진한다. 우리는 해당 없음.

`source.json.tmpl`에 `heartbeat.interval.ms=60000`(1분) 추가. 오버헤드는
`__debezium-heartbeat.{topic_prefix}` 토픽에 분당 1건. 보존 주기 대비 압도적으로
짧아 Connect 일시 장애로 몇 번 빠져도 안전하다.

- 템플릿은 **다음 배포부터** 적용된다. 현재 FAILED인 dz-source는 어차피 재스냅샷
  복구가 필요하고, 그때 재배포되면서 반영된다.

## 운영 수칙 (08-04 수칙의 완결판)

- **heartbeat는 '살아있는 동안' offset이 뒤처지는 것을 막고, archive 보존은
  '죽어 있는 동안'을 커버한다. 둘 다 있어야 완결된다.**
- 보존 기간 산정: 최대 계획 정지 시간 + 여유 (heartbeat가 못 지켜주는 커넥터
  다운 구간의 보험).

## 교훈

- 장애 후속은 감지·복구·**예방** 세 축을 다 채웠는지 확인해야 한다. 08-04 때
  감지(effectiveState, 헬스워처)와 복구(재스냅샷 워크플로)는 채웠지만 예방을
  비워둬서 같은 종착점에 다시 도착했다.
- 로그 기반 CDC 공통 제약: 재개 지점은 소스 로그 보존 윈도 안에 있어야 한다.
  체크포인트 전진 정책과 소스 보존 정책의 관계는 설계 항목이다 (DeltaStream NG
  trail 설계에도 동일하게 적용할 것).
