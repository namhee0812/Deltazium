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

## 추가 관찰 (2026-08-14): heartbeat가 못 막는 세 번째 경로 — 장기 트랜잭션 pin

heartbeat 적용 다음 날 dz-source가 같은 예외(`LogFileNotFoundException`, offset SCN
31,095,228,051)로 다시 FAILED. **heartbeat는 정상 작동 중이었다** —
`__debezium-heartbeat.dz` 토픽에 하루 만에 1,459건(60초 주기와 일치).

원인은 offset 값 자체의 정체다. 실패 약 1시간 전 경고가 정확히 말해준다:

```
09:17:13 WARN Offset SCN 31095199770 has not changed in 25 mining session
iterations. This may indicate long running transaction(s),
active transactions: [01001700e81b0200]
```

Debezium은 재시작 시 **가장 오래된 미커밋 트랜잭션의 시작 SCN부터** 다시 읽어야
하므로, 트랜잭션이 열려 있는 동안 offset은 그 시작 SCN에 고정(pin)된다. heartbeat는
offset을 자주 flush해줄 뿐, pin된 값을 전진시키지는 못한다. 공유 dev Oracle의 로그
정리가 그 SCN 구간을 지우면 동일 예외로 사망.

정리하면 이 장애 계열의 경로는 셋이고 방어책이 각각 다르다:

| 경로 | offset이 뒤처지는 이유 | 방어 |
|---|---|---|
| 유휴 트래픽 (08-13) | 발행이 없어 flush가 없음 | heartbeat ✅ 적용됨 |
| 커넥터 다운 (08-04) | 죽어 있는 동안 flush 불가 | archive 보존 > 최대 다운타임 |
| **장기 트랜잭션 (08-14)** | **미커밋 tx 시작 SCN에 pin** | tx 원인 제거, 또는 `log.mining.transaction.retention.ms`(정합 트레이드오프 있음 — 아래) |

`log.mining.transaction.retention.ms`는 지정 시간을 넘긴 미커밋 트랜잭션을 버리고
offset을 전진시킨다. **버린 트랜잭션이 나중에 커밋되면 그 변경은 유실된다** — 캡처
대상 테이블에 정당한 장기 트랜잭션이 있을 수 있는 환경에서는 함부로 켤 수 없고,
켠다면 재스냅샷으로 복구 가능한 개발 환경 전제여야 한다. 적용 여부 미결.

## 교훈

- 장애 후속은 감지·복구·**예방** 세 축을 다 채웠는지 확인해야 한다. 08-04 때
  감지(effectiveState, 헬스워처)와 복구(재스냅샷 워크플로)는 채웠지만 예방을
  비워둬서 같은 종착점에 다시 도착했다.
- 로그 기반 CDC 공통 제약: 재개 지점은 소스 로그 보존 윈도 안에 있어야 한다.
  체크포인트 전진 정책과 소스 보존 정책의 관계는 설계 항목이다 (DeltaStream NG
  trail 설계에도 동일하게 적용할 것).
