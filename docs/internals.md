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

## UI 디자인 토큰

2026-08-28 리디자인(좌측 rail + drawer 패턴) 기준. 토큰 출처는 dswebclient(DS-3025)의
표면 위계·반경·그림자 체계를 채용하고, 브랜드 색은 Deltazium 파랑(`#005FB8`/다크
`#0078D4`)을 유지했다. 폰트는 Inter Variable → IBM Plex Sans(본문)/IBM Plex Mono
(숫자·SCN·테이블명·타임스탬프)로 교체(`@fontsource/ibm-plex-sans`,
`@fontsource/ibm-plex-mono`, 400/500/600(/700)).

- **원시 토큰** (`ui/src/index.css`의 `:root`/`.dark`): `--surface`/`--surface-2`,
  `--ink`/`--ink-2`/`--ink-3`, `--line`/`--line-2`, `--brand`/`--brand-2`/`--brand-soft`,
  상태색 `--ok`/`--warn`/`--crit`/`--stop`(각 `-soft` 배경 짝), rail 전용
  `--rail`/`--rail-2`/`--rail-ink`/`--rail-ink-2`, `--shadow-card`(2단 그림자).
  값은 `Main.dc.html`/`Tables.dc.html`/`RecoveryDrawer.dc.html` 목업의 `light`/`darkT`
  객체가 정본이다.
- **기존 shadcn 변수와의 매핑**: `--background`/`--card`/`--popover`는 `--bg`/`--surface`에,
  `--muted-foreground`는 `--ink-2`(기존 컴포넌트가 조회 폭넓게 쓰는 보조 텍스트라 더
  옅은 `--ink-3`보다 가독성을 우선), `--border`/`--input`은 `--line`, `--ring`/`--primary`는
  `--brand`에 매핑 — 이 덕분에 shadcn CLI로 새로 추가되는 컴포넌트도 별도 손질 없이
  새 팔레트를 받는다. `--surface2`/`--chart-grid`/`--chart-dim`은 하이픈 없는 옛 이름을
  각각 `--surface-2`/`--line`/`--ink-3`로 앨리어싱한 하위호환 유지용이다.
- **radius**: `--radius: 12px`를 카드 기준으로 두고 `--radius-md`만 8px로 고정 override
  (컨트롤 반경) — 나머지(`sm`/`xl`/`2xl`…)는 기존 비례식(`calc(--radius * n)`)을 유지해
  파급 범위를 줄였다.
- **프리미티브** (`ui/src/components/ui/`): `StatusPill`(ok/warn/crit/stop/brand, 20px,
  점+텍스트), `FilterChip`(999px, 선택 시 brand 역전 + mono 카운트), `GhostButton`(28px,
  hover 시 brand 테두리+soft 배경), `Segmented`(기간 선택), `Card`/`CardHeader`/`CardTitle`
  (12px radius·1px border·`--shadow-card`, 헤더 12px/16px 패딩 + 하단 라인 — 상위
  shadcn의 `--card-spacing` gap 모델 대신 섹션별 고정 패딩으로 재작성).
- **레이아웃 패턴**: 좌측 rail(220px, `--rail` 배경) + 상단 바(56px) — `App.tsx`.
  상세는 비차단(마스크 없음) 우측 drawer로 뜬다 — 테이블 모니터링 440px
  (`TablesPanel.tsx`의 `TableDetailDrawer`), 복구 480px(`RecoveryPanel.tsx`의
  `RecoveryDrawer`). 둘 다 진한(rail) 헤더 + 하단 고정 액션 바 anatomy를 공유한다.

## DDL 치환의 한계

승인 시 타깃 이름 치환은 정규식 기반(한정/비한정·따옴표 조합, 단어 경계로 오치환 방지).
**문자열 리터럴 안의 테이블명은 구분하지 못한다** — 정확한 해법은 파서(ANTLR,
Debezium ddl-parser)이고 현재 범위 밖으로 선언.

## changelog `_pos` 위치 컬럼 도입 (다중 소스·다중 타깃 ①, 2026-09-05)

architecture.md 5.1·6.2절 확정 사항의 구현 판단 기록. 설계 자체(왜 `_pos`가 필요한가,
장기 트랜잭션에서 scn 정렬이 왜 틀리는가)는 architecture.md에 있으므로 여기는 "어떻게
구현했는가"만 남긴다.

**SMT 구조를 그대로 채택한 이유.** `_pos` struct(`topic`/`partition`/`offset`/`timestamp`)는
Iceberg Kafka Connect의 `KafkaMetadataTransform`(`field_name=_pos`, `nested=true`)이 부착하는
구조 그대로다. 커스텀 SMT를 만들지 않은 이유: ① 데이터 경로는 기성 커넥터·SMT만 쓴다는
절대 규칙, ② 이 SMT가 이미 topic/partition/offset/timestamp 네 필드를 구조체 하나로 묶어
주고, 그중 `topic`이 라우팅 키로도 재사용 가능해 "위치 부착"과 "라우팅"을 SMT 하나로
해결한다. 클래스명(`org.apache.iceberg.connect.transforms.KafkaMetadataTransform`)과 설정
키(`field_name`, `nested`)는 공식 문서에 나열돼 있지 않아 커넥터 플러그인 jar를
`unzip -l`·`javap -p -c`로 직접 확인했다 (문자열 상수 `field_name`/`nested`/`external_field`,
`Transformation<SinkRecord>` 구현 확인 — 표준 Kafka Connect SMT 설정 규약을 그대로 따른다).

**라우팅을 `source.table`에서 `_pos.topic`으로 바꾼 이유.** 종전 `route-field=source.table`은
테이블명만 보고 라우팅해 같은 이름의 테이블이 다른 스키마에 있으면 충돌했다(등록 시점에
`existsTableNameInOtherSchema`로 거부하던 이유). `_pos.topic`은 `<prefix>.<schema>.<table>`
전체를 담고 있어 스키마까지 구분되므로, 이 제약을 등록 검증에서 완전히 제거했다
(`RegistrationService.validateTable`에서 해당 체크·리포지토리 메서드·매퍼 쿼리를 삭제).
route-regex는 테이블명 정규식(`^T1$`) 대신 토픽 이름 정확 일치(`^\Q<prefix>.<schema>.<table>\E$`,
`Pattern.quote`로 이스케이프)로 바뀐다.

**namespace를 설정값에서 계산값으로 바꾼 이유.** `IcebergProperties.namespace`(고정 설정)를
없애고 `ChangelogTableService`가 `deltazium.topic-prefix`에서 `changelog_<prefix>`(소문자)를
계산하도록 옮겼다. 소스가 하나뿐인 지금은 결과가 같지만(`changelog_dz`), 다중 소스(②
마일스톤)에서 소스별 namespace가 정말 갈릴 때 설정 파일에 소스 수만큼 항목을 늘리지
않고도 자연히 분리되게 하기 위함 — topic.prefix가 이미 소스 식별자다(2.2절).

**복구 스캔의 "한 파티션 앞" lookback을 정밀 절단 없이 구현한 이유.** recovery-job은 진입
시각을 `truncate(ts_ms, 86400000)` 파티션 경계로 스냅한 뒤 한 파티션 앞(`lookbackBoundary`)부터
`Expressions.greaterThanOrEqual("source.ts_ms", scanFromTsMs)`로 스캔하고, 그 이후 나오는
모든 행을 재생 대상으로 삼는다 — 진입 시각 이후만 골라내는 정밀 필터를 추가로 두지 않았다.
이유: ① 정밀 절단은 "장기 트랜잭션에서 커밋 순서와 어긋난다"는 근본 문제를 근처에서 다시
만든다(어느 컬럼으로 절단해도 결국 소스 전용 위치나 근사치에 의존하게 됨). ② PK upsert
멱등이 전제라 여분의 재생은 안전하고(6.2절), 파티션 폭(1일)만큼의 과다 재생은 토이 볼륨
전제에서 비용이 크지 않다. 파티션 폭 상수(`PARTITION_WIDTH_MS`)는 backend의
`ChangelogTableService`와 recovery-job에 각각 정의돼 있다 — recovery-job은 backend에
의존하지 않는 플레인 Java 모듈이라는 절대 규칙 때문에 상수를 공유할 수 없어 중복
정의했다(두 값이 어긋나면 파티션 프루닝이 깨지므로 상수 옆에 상호 참조 주석을 남겼다).

**재조립에서 `_pos`를 제외하는 방식.** `ConnectJsonAssembler`의 `structSchema`/`structPayload`가
struct를 순회할 때 필드명이 `_pos`면 건너뛴다 — 원본 Debezium envelope에는 없던,
파이프라인이 나중에 붙인 필드라서다. 이 필터는 이름 기준이라 재귀적으로 적용되지만, `_pos`가
envelope의 다른 위치(before/after/source 내부)에 나타날 일이 없어 문제가 없다.

**rule-check.sh의 소스 전용 위치 필드 차단.** `source.scn`·`txId`·`commit_scn`·`lsn`을
`getField(...)`/`get(...)`/`path(...)`로 "읽는" 호출 형태만 grep으로 잡는다 — envelope
스키마 선언(`NestedField.optional(id, "scn", ...)`, 패스스루 보존용, 5.1절 설계 불변식 2가
명시적으로 허용)과 실제 값 참조를 텍스트만으로 구분하기 위한 최소한의 장치다. 값 참조와
스키마 선언은 코드상 호출 형태가 다르므로(`.getField("scn")` vs `NestedField.optional(N,
"scn", ...)`) 이 구분이 grep 수준에서도 안정적으로 성립한다. 검사 범위는
`recovery-job/src/main` 전체와 `backend`의 `recovery`·`iceberg` 패키지 — DDL 워크플로
(`backend/src/main/java/io/deltazium/backend/ddl`)는 스키마 변경 이벤트의 SCN을 사용자에게
참고 표시하는 별개 용도(7절)라 검사 범위 밖이다. UI 참고 텍스트용으로 changelog·복구
경로 안에서 소스 위치를 뽑아야 할 일이 생기면, 그 줄에
`// rule-check-allow: source-position-reference` 주석과 사유를 남기고 예외로 둔다 — 2026-09-05
시점에는 그런 코드가 없어 화이트리스트가 비어 있다.

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

## AI 진단 어시스턴트 overview (assist 패키지)

`GET /api/assist/overview` (파라미터 없음). "왜 CDC가 멈췄어?" 같은 **대상 미지정 질문의
진입점**이다 — AI가 이 한 번의 호출로 어디가 이상한지 특정한 뒤, 로그 검색 등 기존 도구로
파고든다. 새 데이터 소스는 없다: ConnectClient·KafkaMetricsService·TableEventService·
DdlEventService 재사용만 (읽기 전용 원칙은 로그 검색과 동일).

**정상은 개수만, 비정상만 상세를.** 응답 크기가 곧 토큰 비용이다. 그래서 정상 커넥터는
running 카운트, 정상 테이블은 total 카운트로만 잡히고, FAILED·PAUSED·lag 초과·승인 대기만
상세가 실린다. 같은 이유로 상세 목록엔 상한이 있다 — failed/paused/unassigned/lagging/
pendingApproval 각 20건, recentErrors items 10건. 목록이 잘려도 전체 규모는 count 필드
(failedCount·laggingCount·pendingCount 등)로 항상 보인다. recentErrors의 detail(스택 등
대용량)과 trace 전문도 싣지 않는다 — traceHead는 앞 5줄, ddlSummary는 앞 120자이고, 깊이는
로그 검색으로 판다.

**"멈췄다"의 유형 구분이 응답 구조에 있다.** 원인과 조치가 전부 다르기 때문이다:
커넥터 FAILED와 task만 FAILED는 둘 다 `failed` 목록이되 `connectorState`로 구분되고(후자는
커넥터가 RUNNING), 사람이 세운 PAUSED는 이름만 실린다(고장이 아니다),
failed/paused/unassigned 어디에도 안 걸리는 상태(STOPPED·RESTARTING·미지의 상태 문자열
전부)는 `other`에 {name, state} 원문 그대로 실린다, 전부 RUNNING인데
적재가 안 되는 유형은 `tables.lagging`(임계: `deltazium.assist.lag-threshold`, 기본 1000 —
**offset 건수지 시간이 아니다**), DDL 승인 대기로 선 것은 `ddl.pendingApproval`
(state=DETECTED만 — 이것도 고장이 아니라 사람의 결정 대기)로 나타난다. AI가 유형을
텍스트에서 추론하는 게 아니라 구조에서 읽게 한다.

**섹션별 장애 격리.** Kafka Connect가 죽어 있는 상황이야말로 이 API가 필요한 순간이므로,
한 소스의 실패가 전체를 500으로 만들면 안 된다. 섹션별 독립 try-catch — 실패한 소스는
`sources`(connect/kafka/db)에 UNREACHABLE로 표기하고 해당 섹션만 null. "Connect 자체가
안 떠 있다"가 그 자체로 AI의 판단 근거가 된다. recentErrors와 ddl은 둘 다 메타데이터
PG가 소스라 어느 한쪽 실패면 db=UNREACHABLE이다.

STOPPED·RESTARTING·미지 상태를 별도 분류하지 않고 `other` 버킷(상한 20건, otherCount는
항상 전체 수)에 상태 문자열 원문으로 담기로 결정(2026-08-11) — 아무 목록에도 안 실리면
total≠running인데 이유가 안 보이는 응답이 되고, 멈춤 유형 구분이 이 API의 존재 이유다.
미지 상태도 원문 그대로 노출하면 AI가 알아서 해석한다.

## AI 진단 어시스턴트 Claude 연동 (chat 패키지)

`POST /api/chat` (body `{"question":"..."}`, 응답 `text/event-stream`). assist 패키지의
GetOverview·SearchLogs를 Claude API tool use로 묶어 대화형 진단을 만든다. **backend가
도구를 실행한다** — Claude가 우리 API를 직접 호출하는 게 아니라, backend가 Claude API를
호출하고(질문 전달) Claude가 tool_use로 돌려준 요청을 backend가 같은 프로세스의
OverviewService·LogSearchService로 직접 실행해(HTTP 왕복 없음) 결과를 다시 Claude에
넘기는 왕복을 반복한다. Java SDK의 `BetaToolRunner`가 이 왕복을 자동화한다
(`client.beta().messages().toolRunner(params)`).

**도구는 읽기 전용 2종뿐 — 조치 실행 도구는 의도적으로 없다.** GetOverviewTool·
SearchLogsTool 모두 assist 패키지 서비스를 호출만 한다. 시스템 프롬프트가 "조치는
제안만"이라고 지시하는 것과 별개로, 애초에 실행 수단 자체가 없어야 프롬프트 우회에도
안전하다 — 프롬프트 지시는 우회될 수 있어도 없는 도구는 호출할 수 없다.

**근거 강제.** 시스템 프롬프트가 모든 주장에 "어느 도구의 어떤 필드/로그 파일·줄 번호"
근거를 요구한다. CDC 파이프라인 진단은 오탐의 비용이 크다(불필요한 재시작·복구 트리거로
이어질 수 있음) — 모델이 그럴듯하게 추측하는 것보다 "모른다"고 말하는 편이 안전하다.

**도구 클래스가 Spring 빈에 접근하는 방법 — 정적 브릿지(결정 필요로 남김).**
`BetaToolRunner.addTool(Class)`는 Jackson이 도구 클래스를 매 호출마다 기본 생성자로
새로 만든다(anthropic-java-core:2.53.0 확인 — `RunnableTool.FromClass.run()`이 매번
새 인스턴스를 역직렬화). 그래서 OverviewService·LogSearchService를 생성자로 주입할 수
없다 — `ToolBeans`(정적 필드)로 우회했다. 두 서비스가 상태 없는 읽기 전용 싱글턴이라
요청 간 공유가 안전하다고 보고 택한 방식이지만, 더 나은 대안이 있는지는 확인이
필요하다(`ToolBeans.java` 주석 참고).

**파라미터 없는 도구의 스키마 제약.** GetOverview는 원래 파라미터가 없지만,
anthropic-java-core:2.53.0의 로컬 스키마 검증이 `properties`가 빈 도구를 거부한다
(addTool의 `JsonSchemaLocalValidation` 인자와 무관하게 `RunnableTool.FromClass`가 항상
YES로 재검증). 그래서 사용하지 않는 nullable 필드 하나(`unused`)를 스키마에 남겨
검증을 통과시킨다 — Claude에게는 "값을 채우지 말 것"으로 설명한다.

**필드명은 camelCase 그대로.** 도구 클래스의 Java 필드명이 스키마 프로퍼티명·Claude가
보내는 JSON 키에 그대로 쓰인다(snake_case로 자동 변환되지 않음 — 실측 확인). optional
필드는 `org.springframework.lang.Nullable`(심플 이름이 "Nullable"이면 어느 패키지든
인식된다)로 표시하면 스키마 타입에 `null`이 허용되지만, strict 스키마 특성상
`required` 배열에서는 빠지지 않는다 — Claude는 항상 키를 보내되 값으로 `null`을 쓸 수
있다.

**키 미설정 시 SSE로 안내만 하고 종료.** `ANTHROPIC_API_KEY`는 `deploy/env.sh`가
`~/deltazium-runtime/conf/secrets.env`를 있으면 로드한다(git 추적 안 됨). 클라이언트는
지연 초기화하며, 요청이 들어왔을 때만 환경변수를 확인한다 — 키가 없으면 클라이언트를
만들지 않고 `{"type":"error", ...}` 이벤트 후 `{"type":"done"}`으로 스트림을 정리한다.

**비용 구조와 prompt caching (2026-08-13).** tool use는 왕복마다 시스템 프롬프트·도구
정의·누적 히스토리를 재전송한다 — 캐시 없이 opus로 도구 왕복 14회짜리 진단 1건에
약 $1이 나왔다. top-level cache 브레이크포인트(`cacheControl`, 마지막 블록 자동 배치)를
걸면 왕복 N의 캐시 쓰기를 왕복 N+1이 그대로 읽어(0.1배 단가) 반복분이 사실상 사라진다.
실측: sonnet + 캐시로 동일 질문이 신규입력 6 / 캐시읽기 4,239 / 캐시쓰기 14,857 토큰
≈ **$0.06**. 왕복별 usage는 backend.log에 `chat usage` INFO로 남는다(cache_read=0이면
캐시 미적중 — 회귀 신호).

**모델은 `deltazium.chat.model`로 설정** (기본 claude-sonnet-5). opus 대비 단가 절반
이하에 도구 호출 횟수도 크게 적었다(2회 vs 14회 — 빠르고 싸지만 탐색 깊이는 얕다).
server-side fallback은 Opus/Fable 계열 전용이라 모델명 prefix로 조건부 적용한다.

**UI는 탭이 아니라 플로팅 위젯 (2026-08-14 전환).** 진단 질문은 다른 화면을 보다가
생긴다 — 문제 화면을 눈에 둔 채 묻고 답변의 지시를 바로 따라가야 하므로, 전용 탭 대신
우하단 FAB + 슬라이드업 패널(420↔720px)로 했다. 위젯은 탭 조건부 밖에 항상 마운트라
탭 전환에도 대화가 유지되고, 스트리밍 중 닫아도 백그라운드에서 완료된다.
EventSource가 POST를 못 하므로 SSE는 fetch 스트리밍으로 직접 파싱한다(`ui/src/lib/sse.ts`).

## 개발 환경 특이사항

- vite dev 서버는 `usePolling` (vite.config.ts): 이 서버에서 inotify 감시가 변경을
  놓쳐 낡은 모듈을 서빙하는 일이 반복돼 폴링으로 전환. 외부 접속은 `--host`
  (dzadmin web start가 고정).
- Gradle 툴체인: `~/.gradle/gradle.properties`의
  `org.gradle.java.installations.paths=/home/nhchoi/java21` — 비표준 경로의 JDK 21을
  IntelliJ sync가 찾게 한다.
