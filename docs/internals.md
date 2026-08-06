# 구현 내부 노트

"UI에는 사용자 결정에 필요한 것만, 시스템 내부는 문서와 코드 주석에" 원칙의 수납처.
설계 기준은 [architecture.md](architecture.md), 운영 절차는 [operations.md](operations.md),
백로그는 [TODO.md](TODO.md). 각 항목의 상세는 해당 소스 파일 헤더의 수정 내역 참조.

## 상태 판정 — connector와 task는 별개다

Kafka Connect는 connector 상태와 task 상태가 분리돼 있고, **가장 흔한 장애 형태가
"connector RUNNING + task FAILED"** 다. 실제로 이 형태의 장애가 나흘간 UI에 초록으로
보인 사고가 있었다 ([incidents/2026-08-04](incidents/2026-08-04-archive-log-loss.md)).

- UI: 모든 상태 표시는 `ui/src/lib/connect.ts`의 `effectiveState()` — connector와
  전체 task 중 최악값. 원인 요약은 trace의 마지막 `Caused by` 줄(`causeLine()`).
- backend: `ConnectorHealthWatcher`(30초)가 상태 전이를 이벤트로 적재. **첫 관측이
  이미 FAILED여도 기록**한다 — 전이만 기록하면 backend 기동 전에 난 장애가 영영 안 보인다.

## 재스냅샷 상태 기계 (ResnapshotOrchestrator)

```
① 유입 차단(source stop — offset 보존) → ② sink 잔량 소진 대기
→ ③ [truncate 시] 실행 주체 승인 → 시스템 실행(권한 사전 점검) 또는 홀드(count=0 폴링)
→ ④ offset 삭제·snapshot.mode 재배포·재개 → ⑤ 초기 스냅샷 → ⑥ go-live
```

- **④ 전까지는 원복 가능**: offset을 안 건드렸으므로 취소/실패 시 source resume만으로
  기존 지점부터 스트리밍이 이어진다. ④ 이후는 스냅샷 완주가 안전한 길이라 취소를 막는다.
- **홀드가 길어져도 안전한 이유**: 어차피 ④에서 offset을 버리고 전체 스냅샷을 뜨므로
  redo/archive 보존 기간과 무관해진다. 단 홀드 기간의 changelog 공백은 감수.
- **순서가 핵심인 이유**: ①(유입 차단) 없이는 잔량이 무한하고, ②(소진) 없이 truncate하면
  파이프에 남은 옛 이벤트가 빈 타깃에 뒤늦게 apply돼 오염된다.
- truncate 권한: 스키마 소유 또는 DROP ANY TABLE (`TargetTableGate.canTruncate` —
  session_privs 조회). 권한 점검을 실행 **전에** 하므로 부분 truncate가 생기지 않는다.
- NO_DATA + truncate 조합은 거부 — 비우고 현재부터 하면 과거 데이터가 영영 없다.
- no_data의 경계: 재기동 시점에 진행 중이던 트랜잭션은 시작 SCN 이전 변경분이
  마이닝 범위 밖이라 온전히 반영되지 않을 수 있다 (UI 경고 문구에 반영).

## 스냅샷 진행 실측 — Debezium notification

- source 커넥터 설정: `notification.enabled.channels=sink`,
  `notification.sink.topic.name={prefix}-notifications`
- `SnapshotNotificationPoller`가 상시 소비: STARTED / IN_PROGRESS /
  TABLE_SCAN_COMPLETED(테이블·행수) / COMPLETED(= go-live) → 인메모리 상태(UI 폴링) +
  이벤트 적재
- **함정 (실제로 밟음)**: 워커가 JSON converter `schemas.enabled=true`라 notification도
  `{schema, payload}` 봉투에 싸여 온다 — payload 언랩 필수. 또 구독 후에 생성되는
  토픽의 첫 메시지를 놓치지 않으려면 `auto.offset.reset=earliest`.

## 모니터링 파이프라인

수집(`MetricsSampler`, 1분) → 저장(PG `metrics_samples`) → 롤업(`MetricsRollupService`,
매시) → 조회(`/api/metrics/dashboard`, SQL GROUP BY).

| 지표 | 수집 방식 | 롤업 집계 |
|---|---|---|
| PUBLISH / APPLY_JDBC / APPLY_ICEBERG | 토픽 end offset·(end−lag)의 분당 델타. 누적값이 줄면(재스냅샷 offset 리셋) 그 구간은 건너뜀 | 합 |
| LAG_JDBC / LAG_ICEBERG | consumer group lag 시점값 | **구간 최대** — 평균은 스파이크를 숨긴다 |
| RESOURCE | pid 파일(`~/deltazium-runtime/pids`) + 자기 자신의 `/proc/<pid>/stat·status` → CPU%·RSS | 평균 |

- 해상도 2단 롤업: MIN(48h 보존) → HOUR(60일) → DAY(1년). 완결된 구간만 대상이라
  실시간 그래프에 영향이 없고, NOT EXISTS 조건으로 멱등(재실행해도 중복 없음).
  삭제는 롤업 성공과 무관한 보존 기간 기준 — 롤업이 실패해도 원본이 48시간 남아 재시도된다.
- **DATE_TRUNC 방언**: PG는 문자열 인자(`'hour'`), H2는 키워드(`HOUR`).
  `MyBatisConfig`의 databaseIdProvider로 매퍼 XML에서 분기. 주의: databaseId가 지정된
  `<sql>` 조각만 있으면 provider 없는 컨텍스트(@MybatisTest 슬라이스)에서 **전체 매퍼
  파싱이 깨진다** — databaseId 없는 기본 변형을 반드시 함께 둘 것.
- MyBatis 별칭 함정: `javaType="long"`은 래퍼 Long, 원시형은 `"_long"`.
- CPU% 계산: utime+stime tick 델타 / (HZ=100 × 경과초). `/proc/<pid>/stat`은 comm
  필드에 공백·괄호가 올 수 있어 마지막 `)` 이후를 파싱한다 (`ProcReader`).
- 미수집: PostgreSQL (pid 파일을 pg_ctl이 pgdata에 따로 관리), 원격 Oracle.
- Prometheus/Grafana 도입은 [TODO.md](TODO.md) — 현 구조는 "의도적 경량 자체 수집".

## 대시보드 UI

- **토폴로지는 자체 SVG** (`TopologySvg.tsx`): 고정 8노드 정적 다이어그램이라
  React Flow가 과잉이었고, 뷰포트 상태 소실로 화면이 사라지는 이슈까지 있었다.
  레인 배치(실 적재/캡처 본류/changelog/복구)로 간선 교차 0.
- 차트(`LineChart.tsx`)는 SVG 직접 구현. 계열 색 `#2BA3C4`/`#8B6FE8`은 다크 서피스
  기준 색각이상(CVD) 분리·대비 검증을 통과시켜 고정한 팔레트 — 상태색(ok/warn/crit)과
  분리해서 쓴다. 범례 + 선 끝 직접 라벨(충돌 자동 회피) + 크로스헤어 툴팁.
- 주기 버튼은 기간 중심("최근 6시간/7일/90일") — 해상도·보존은 내부 사정이므로 숨긴다.

## DDL 치환의 한계

승인 시 타깃 이름 치환은 정규식 기반(한정/비한정·따옴표 조합, 단어 경계로 오치환 방지).
**문자열 리터럴 안의 테이블명은 구분하지 못한다** — 정확한 해법은 파서(ANTLR,
Debezium ddl-parser)이고 현재 범위 밖으로 선언.

## 개발 환경 특이사항

- vite dev 서버는 `usePolling` (vite.config.ts): 이 서버에서 inotify 감시가 변경을
  놓쳐 낡은 모듈을 서빙하는 일이 반복돼 폴링으로 전환. 외부 접속은 `--host`
  (dzadmin web start가 고정).
- Gradle 툴체인: `~/.gradle/gradle.properties`의
  `org.gradle.java.installations.paths=/home/nhchoi/java21` — 비표준 경로의 JDK 21을
  IntelliJ sync가 찾게 한다.
