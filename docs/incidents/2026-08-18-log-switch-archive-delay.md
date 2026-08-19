# 장애 기록: 로그 스위치 중 아카이브 완료 대기 초과 — offset SCN이 있는데도 LogFileNotFoundException

[08-04](2026-08-04-archive-log-loss.md)·[08-13](2026-08-13-idle-scn-loss-recurrence.md)과
같은 예외(`LogFileNotFoundException: None of the log files contain offset SCN`), 그러나
**로그는 유실되지 않았다.** offset SCN을 담은 archive log가 멀쩡히 있는데도 task가 죽었다 —
Debezium이 로그를 찾는 창이 Oracle이 아카이빙을 끝내는 시간보다 짧았다.

## 타임라인 (2026-08-18, KST)

| 시각 | 사건 | 근거 |
|---|---|---|
| 18:49:35 | offset scn=31098244920 정상 진행 | connect-1.log:18545 |
| 18:49:42 | 로그 스위치 seq 1051→1052 | `v$archived_log` NEXT_TIME |
| 18:49:44 ~ 18:50:16 | `No logs available yet (attempt 1~6)` — 6회 재시도 | connect-1.log:18552-18560 |
| **18:50:33** | **seq 1051 아카이브 완료** (3.5 GB, 51초 소요) | `v$archived_log` COMPLETION_TIME |
| 18:50:48 | `LogFileNotFoundException` offset SCN 31098244969 → task FAILED | connect-1.log:18568 |

offset SCN 31098244969는 seq 1051(31097649579 ~ 31098244997) 안에 있고, 그 archive는
지금도 존재한다(`deleted='NO', status='A'`). 08-19 task 재시작만으로 같은 offset에서 재개됐다.

## 메커니즘

Debezium 3.6 `SqlUtils.allMinableLogsQuery`는 마이닝 대상 로그를 두 소스에서 모은다:

- online redo: `V$LOG WHERE L.STATUS = 'CURRENT'` — **CURRENT만**
- archived: `V$ARCHIVED_LOG WHERE STATUS='A' AND ARCHIVED='YES' AND NAME IS NOT NULL ...`

로그 스위치 직후 직전 시퀀스는 online에서 ACTIVE(아카이빙 중)가 되어 첫 조건에서 빠지고,
`V$ARCHIVED_LOG` 행은 아카이빙이 **끝나야** 생긴다. 그 사이엔 어느 소스에도 없다.
`LogFileCollector.isOpenThreadConsistent`가 "offset SCN을 담은 로그 없음"으로 판정하면
`log.mining.log.query.max.retries`(기본 5 → 6회 시도) 동안 지수 백오프(1s→최대 60s)로 재시도한다:
시도 시각 +0, 1, 3, 7, 15, 31s. **마지막 검사가 +31s**, 그 뒤 32초를 자고 예외를 던진다.

이 DB의 redo log는 그룹당 4 GB, 스위치 시 3.5 GB 아카이빙에 ~50초(1050: 49s, 1051: 51s).
마지막 검사(+31s)가 아카이브 완료보다 앞서면 죽는다. 08-17 22:02 스위치는 마이닝 루프가
스위치를 늦게 알아채 타이밍이 맞았고, 08-18은 17초 차이로 빗나갔다. 확률 장애다.

## 조치 (2026-08-19)

- `connectors/source.json.tmpl`에 **`internal.log.mining.log.query.max.retries=20`** 추가, 운영 중인
  dz-source에도 PUT 반영. 백오프가 60s에서 포화하므로 20회면 마지막 검사가 스위치 후
  약 14분 뒤 — 아카이빙 수 분 지연까지 흡수한다. 정상 시엔 첫 시도에서 통과하므로 비용 없음.
- task 재시작으로 복구. 재스냅샷 불필요(로그 유실 아님).

**키 이름 함정 (1차 조치 무효, 10:39 스위치에서 재발).** 소스의 Field 정의는
`Field.createInternal("log.mining.log.query.max.retries")`인데, `createInternal`은 실제 키에
`internal.` 접두를 붙인다 — `Field.name()`은 `internal.log.mining.log.query.max.retries`.
접두 없는 키를 넣으면 Connect도 Debezium 설정 dump도 20을 그대로 되비추지만(미지 키는 통과)
`OracleConnectorConfig`는 기본값 5를 쓴다. 설치 jar로 직접 확인:
`new OracleConnectorConfig(config with "log.mining...=20").getMaximumNumberOfLogQueryRetries()`
→ 5, `internal.log.mining...=20` → 20. 공식 문서에 없는 internal 설정이므로 버전 업 시
존재 여부를 다시 확인할 것 (`javap -c OracleConnectorConfig | grep max.retries`).

## 세 번째 경로

같은 예외의 원인이 이제 세 갈래다. 진단할 때 예외 메시지만 보고 "archive 유실"로 단정하지 말 것:

| 경로 | 로그 존재 | 복구 |
|---|---|---|
| 08-04 다운타임 중 archive 삭제 | 없음 | 재스냅샷 |
| 08-13 유휴로 offset 정체 후 archive 삭제 | 없음 | 재스냅샷 (예방: heartbeat) |
| **08-18 스위치 중 아카이빙 대기 초과** | **있음** | **task 재시작** (예방: max.retries) |

판별 쿼리 — offset SCN을 담은 archive가 있으면 세 번째 경로:
```sql
select sequence#, first_change#, next_change#, deleted, status, completion_time
  from v$archived_log where :offset_scn between first_change# and next_change#;
```
