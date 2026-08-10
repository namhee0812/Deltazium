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

## AI 진단 어시스턴트 로그 검색 (assist 패키지)

`GET /api/assist/logs` 하나. **UI용 API와 같은 컨트롤러에 얹지 않고 `io.deltazium.backend.assist`
패키지로 분리**했다. 이유는 둘이다. ① 응답 형태 요구가 다르다 — 화면은 집계·상태값을 원하지만
AI는 답변에 출처를 붙여야 하므로 근거 위치(파일 상대경로·줄번호·매칭 여부)를 줄 단위로 받아야 한다.
② **AI가 볼 수 있는 표면을 한 패키지에 모아 두면 감사와 읽기 전용 강제가 쉽다** — 이 패키지에는
GET만 두고 DB 쓰기·파일 쓰기를 넣지 않는다는 규칙을 패키지 단위로 지킬 수 있다.

제약과 근거:

| 제약 | 이유 |
|---|---|
| 대상은 `LogSource` enum(BACKEND/CONNECT/KAFKA/CONTROLLER)의 **고정 매핑**으로만 결정 | 사용자 입력이 파일명·경로에 닿는 통로를 없앤다. 파일명·경로 파라미터는 받지 않는다 |
| 그럼에도 최종 경로를 `toRealPath()`로 정규화해 로그 루트 하위인지 검증 | 심링크로 루트를 벗어나는 파일 차단. 벗어나면 조용히 건너뛴다 |
| 검색은 대소문자 무시 **literal 부분문자열** (정규식 금지) | 키워드는 외부 입력이다 — 정규식을 허용하면 ReDoS |
| `limit` 기본 100·서버 캡 500, `contextAfter` 최대 20 | 클라이언트가 뭘 보내든 응답·메모리 상한을 서버가 쥔다 |
| 줄 단위 스트리밍 + 고정 크기 링버퍼 | 파일을 통째로 올리지 않는다. connect.log가 이미 2MB대이고 더 커진다 |

`source`는 대소문자를 무시해 받고(호출자가 AI라 소문자가 흔하다), 매핑에 없는 값은 **허용 값
목록을 담아** 400 — AI가 그걸 보고 스스로 고치게 한다. `from > to`는 조용한 빈 결과 대신 400이다:
파라미터 실수가 "해당 기간 로그 없음"으로 위장되면 AI가 오답한다.

**로그 파일 레이아웃 처리** — 오늘은 루트 바로 아래 평면 파일 `{base}.log`, 지난 날짜는
`{yyyy-MM-dd}/{base}-{index}.log`. index 자리수가 섞인 건 순전히 이력이다 — 현재
backend/connect/kafka 로그 설정은 이미 동일하게 통일돼 있고(같은 파일명 패턴·100MB·일 단위
롤오버·14일 보존), `connect-00.log` 같은 파일은 전부 옛 설정 잔재다. index는 **정수로 파싱하되
용도는 "판별"이다** — `{base}-{숫자}.log` 형태인지 확인해 같은 glob에 걸리는 동명이인
(`connect-console.log`, `kafka-authorizer-*.log`)을 걸러내는 필터로만 쓴다.
날짜 디렉터리·파일이 없으면 에러가 아니라 건너뛴다.

**함정: index는 시각 순서와 무관하다 — 최신성 판단은 mtime으로 한다.**
log4j2 `DefaultRolloverStrategy`의 index는 1..max를 **회전 재사용하는 슬롯 번호**라 시각과
단조 대응하지 않는다. 한 바퀴 돌면 낮은 번호가 다시 최신이 된다. 실제 `logs/2026-08-04/`가 그렇다 —
`connect-13.log`가 14:59인데 그날 가장 최신인 파일은 **23:59의 `connect-10.log`**다(11·12·13은
max가 더 컸던 옛 설정 시절 잔재). index로 정렬하면 그날 최신을 통째로 놓친다.
그래서 같은 날짜 안의 파일 순서는 `Files.getLastModifiedTime()` 기준이고, mtime이 같을 때만
index로 tie-break 한다. 날짜 디렉터리 사이의 순서는 디렉터리명(날짜)으로 판단한다 — 그건 신뢰할 수 있다.
mtime을 읽지 못하는 파일(경합으로 삭제 등)은 예외를 던지지 않고 가장 오래된 것으로 취급해 뒤로 민다.

**정렬 정책: 최신 우선으로 고르고, 시간 오름차순으로 반환한다.**
진단은 거의 항상 "최근 에러"를 본다. 오름차순으로 훑다 cap에서 끊으면 *가장 오래된* 매칭 500개가
나와 용도와 정반대가 된다. 그래서 날짜 역순·같은 날은 mtime 역순으로 파일을 훑고, 파일 안에서는
스트리밍 오름차순 읽기를 유지하되 최근 cap개만 남기는 링버퍼로 오래된 매칭을 밀어낸다.
cap이 차면 더 오래된 파일은 열지 않는다 — 대부분의 질의는 최신 파일 하나만 읽고 끝난다.
고른 뒤에는 뒤집어 **시간 오름차순으로 반환**한다: 사람이 읽기 편하고, contextAfter로 딸려온
stack trace 줄 순서도 자연스럽다. `truncated`는 "조건에 맞는 매칭이 더 있었으나 잘렸다"는 뜻이며,
링버퍼에서 밀려났거나 더 오래된 파일을 열지 않은 경우 모두 true다.

로그 루트는 `deltazium.assist.log-dir`(기본 `${DZ_LOG_DIR:~/deltazium-runtime/logs}`).
복구 잡 출력 경로인 `deltazium.recovery.log-dir`과는 용도가 달라 재사용하지 않는다.

## 개발 환경 특이사항

- vite dev 서버는 `usePolling` (vite.config.ts): 이 서버에서 inotify 감시가 변경을
  놓쳐 낡은 모듈을 서빙하는 일이 반복돼 폴링으로 전환. 외부 접속은 `--host`
  (dzadmin web start가 고정).
- Gradle 툴체인: `~/.gradle/gradle.properties`의
  `org.gradle.java.installations.paths=/home/nhchoi/java21` — 비표준 경로의 JDK 21을
  IntelliJ sync가 찾게 한다.
