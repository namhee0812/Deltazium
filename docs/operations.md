# 운영 가이드

콘솔(UI) 사용자 관점의 기능·운영 절차. 시스템 내부 구조는 [internals.md](internals.md),
설계 근거는 [architecture.md](architecture.md).

## 기동·정지

```bash
./deploy/dzadmin all start      # infra(pg→minio→kafka→connect) → backend(8090) → web(5173)
./deploy/dzadmin status         # 전체 상태 요약
./deploy/dzadmin backend restart   # 코드 반영 재기동 (별칭 engine; web/ui도 동일)
./deploy/dzadmin all stop       # 역순 정지 (데이터 보존)
```

- Kafka Connect가 뜨면 등록된 커넥터는 마지막 offset부터 자동 재개된다 — 커넥터 별도 기동 없음.
- backend(제어면)가 죽어도 CDC 데이터 흐름(캡처→apply→changelog)은 계속 돈다.
  UI·등록·복구·DDL 승인만 멈춘다.
- 로그 위치·로테이션: [deploy/README.md](../deploy/README.md) — 일 단위,
  지난 날짜는 `logs/yyyy-MM-dd/` 디렉터리.

## 장애 자동 감지 (watchdog)

`deploy/watchdog.sh`가 `dzadmin status` 출력을 파싱해 infra(pg/minio/kafka/connect)·backend
중 DOWN인 컴포넌트를 dzadmin의 해당 컴포넌트 `start`로 자동 재기동한다.
web(vite)은 개발 도구이므로 감시 대상에서 제외한다.

- 재기동 판단은 5분 주기 실행을 전제로 한다(crontab). 이 리포는 crontab 등록까지는
  하지 않으므로 필요하면 직접 등록한다:
  ```
  */5 * * * * /home/nhchoi/deltazium/deploy/watchdog.sh
  ```
- 로그: `~/deltazium-runtime/logs/watchdog.log` — **정상일 때는 아무것도 기록하지 않는다**
  (5분 주기로 계속 도는 로그가 오염되지 않도록). DOWN 감지·재기동 시도·결과만 남는다.
- 동시 실행은 `flock`으로 막는다(`/tmp/dz-watchdog.lock`) — 재기동이 다음 주기보다
  오래 걸리는 경우 중복 실행 방지.
- infra는 컴포넌트별로 이미 떠 있으면 건너뛰는 멱등 스크립트라, 일부만 죽어 있어도
  `dzadmin infra start` 한 번으로 죽은 것만 재기동된다.
- 26-08-20 디스크 풀로 Kafka가 죽고 나흘간 미검출됐던 장애의 재발 방지 목적이다. 다만
  watchdog은 "죽은 프로세스를 다시 띄우는 것"까지만 한다 — 디스크가 다시 가득 차면
  재기동도 계속 실패한다. 근본 원인(디스크 사용률)은 아래 경고 센터로 사전에 본다.

## AI 진단 어시스턴트 키 설정

`~/deltazium-runtime/conf/secrets.env` 파일을 만들고 `export ANTHROPIC_API_KEY=sk-ant-...`
한 줄을 넣는다(git 추적 안 됨). `deploy/env.sh`가 있으면 자동으로 로드하므로 backend
재기동(`./deploy/dzadmin backend restart`)만 하면 된다.

## 화면 안내

콘솔은 좌측 rail 내비게이션(220px) + 상단 바(56px, 페이지 제목·부제 + 등록/AI 진단/테마/
경고 센터) + 본문 구조다. DDL 이력 항목에는 승인 대기 건수 배지가, rail 하단에는 엔진 연결
상태 pill이 뜬다.

| 탭 | 용도 |
|---|---|
| 대시보드 | KPI 카드(커넥터·처리량·최대 lag·미승인 DDL) + 토폴로지 + "주의 필요" 목록(lag 초과·DDL 대기, 클릭 시 해당 탭 이동) + 처리량·lag 시계열(테이블·기간 선택) + 자원 + 최근 이벤트 |
| 테이블 모니터링 | 검색 + 상태 필터 칩(전체/정상/경고/정지), 행 클릭 시 우측 440px 상세 drawer(비차단 — 그리드 위에 겹쳐 뜨고 뒤 화면도 계속 조작 가능)에서 상태·15분 lag 추이·최근 이벤트 확인 + 정지/재개/재시도/삭제·재스냅샷 실행 |
| DDL 이력 | 소스 DDL 수집 타임라인 — 승인(타깃 실행)/거부(해당 테이블 apply 정지), 비전파성 DDL 자동 무시 |
| 이벤트 | 테이블별 운영 이벤트 이력 (장애 전이 trace, 스냅샷 진행, 복구 단계 등 전부 여기 남는다) |
| 복구 | changelog(S3) 현황·실행 이력은 페이지에 상시 표시, [복구 실행] 버튼으로 우측 480px drawer를 열어 대상 테이블·SCN 범위·go-live 여부를 지정해 트리거. 정합 검증(행수·체크섬)은 drawer 3단계 카드에서 수동 실행 |
| DB 연결 | 연결 카드 그리드(카드 = 연결 하나, 헤더 좌측 액센트가 마지막 테스트 결과). 카드 인라인 [테스트], 사용 중인 테이블 수(/api/registrations join) 표시, "+ 연결 추가" 점선 카드로 등록 |

### AI 진단 위젯 (상단 바 아이콘 → 우측 drawer)

어느 탭에서든 상단 바 우측 말풍선 아이콘("AI 진단") → 우측 고정 drawer(440px, 폭 확장
토글로 720px, 마스크 없음·비차단 — 뒤 화면도 계속 조작 가능). "왜 CDC가 멈췄어?" /
"dz-source 왜 죽었어?" 같은 질문을 하면 AI가 상태 조회·로그 검색 도구를 스스로 호출해
근거(로그 파일·줄 번호)와 함께 원인을 짚는다. 사용한 도구는 답변 위에 표시된다.

- **AI는 조회만 한다** — 재시작·재스냅샷 등 조치는 제안까지고, 실행 버튼은 각 화면에서
  사람이 누른다.
- 탭을 옮겨도 대화가 유지되고, 답변 스트리밍 중 닫아도 백그라운드에서 계속 진행된다.
- 근거로 제시된 로그 위치는 `grep -n`으로 직접 검증할 수 있다.
- API 키 설정은 위(`secrets.env`) 참고. 질문당 비용 감각은 sonnet 기준 $0.1 안팎
  (backend.log의 `chat usage` INFO로 왕복별 토큰 확인 가능).

### 경고 센터 (헤더 우측 칩)

평시엔 아무것도 표시되지 않는다. 아래 3종 경고 중 하나라도 감지되면 헤더 우측에
칩(예: "⚠ 2")이 뜨고, **클릭하면** 팝오버로 목록(제목·상세·발생 시각)을 보여준다.
30초 주기로 `GET /api/system/warnings`를 폴링한다.

| 경고 | 판정 기준 | 심각도 |
|---|---|---|
| 디스크 사용률 | `deltazium.runtime-dir`(기본 `~/deltazium-runtime`) 파일시스템 사용률이 `deltazium.disk-warn-pct`(기본 85%) 이상 | WARN |
| Kafka 연결 불가 | AdminClient `describeCluster` 실패(타임아웃 포함) | CRITICAL |
| 커넥터 상태 | 커넥터/태스크가 FAILED면 커넥터별 CRITICAL, RUNNING이 아닌 그 외 상태(UNASSIGNED·PAUSED 등)면 WARN, Connect REST 자체 접근 불가면 개별 대신 CRITICAL 1건 | WARN 또는 CRITICAL |

- 칩 색상은 목록 중 최고 심각도 기준 — CRITICAL이 하나라도 있으면 destructive 색,
  없으면(WARN만) warning 색.
- 임계값은 `backend/src/main/resources/application.yml`의 `deltazium.runtime-dir` /
  `deltazium.disk-warn-pct`에서 조정한다.
- **API 호출 자체가 실패하면(backend가 완전히 다운)** 칩을 CRITICAL로 띄우고
  "backend 연결 끊김" 경고를 클라이언트에서 합성해 보여준다 — 배너를 숨기지 않고
  오히려 가장 눈에 띄게 뜨는 것이 이 기능의 핵심이다(26-08-20 "겉보기엔 정상, 실은
  나흘째 다운" 장애 재발 방지).
- 테이블 모니터링 탭의 지표 조회 실패 배너·캡처 장애 배너와는 역할이 다르다 — 경고
  센터는 "시스템 전체가 정상인가", 화면 배너는 "이 화면(테이블)의 데이터가 최신인가".

## CDC 등록

[＋ CDC 등록] 위저드: 소스/타깃 연결 선택 → 패턴 조회(`SCHEMA.*` 가능) → 테이블 선택
(PK 없는 테이블은 선택 불가) → 컬럼 매핑(체크박스 = 적재 컬럼 선택, `${COL}` 매핑) →
사전 점검 → 스냅샷 모드(초기적재 포함 / 현재 시점부터) → 배포.

사전 점검이 배포를 게이트한다:
- 테이블 supplemental logging: 적용 DDL을 보여주고 **승인 시에만** 실행 (권한 없으면 에러 노출)
- 캡처 권한 8종: 누락 시 DBA용 GRANT 스크립트 안내, 통과 전 배포 차단

## 운영 시나리오

### 테이블 정지/재개
행의 [정지]는 **그 테이블의 타깃 apply만** 멈춘다 (회색 "정지됨"). 캡처와 changelog 기록은
계속되므로 데이터 유실이 없고, [재개]하면 밀린 만큼(JDBC LAG) 따라잡는다.
단, Kafka retention(24h)을 넘기면 밀린 이벤트가 Kafka에서 사라진다 → 그때는 S3 복구로.

### 테이블 apply 장애 (행 빨간 "장애" 배지)
타깃 쪽 원인(컬럼 크기 부족, 제약 위반 등)으로 그 테이블 sink만 죽은 상태.
다른 테이블·캡처·changelog는 계속 돈다.
1. 배지에 마우스 → 원인 한 줄. 상세는 이벤트 탭(trace 포함)
2. 타깃에서 원인 해결 (예: ALTER TABLE로 컬럼 확장)
3. 행의 [재시도] → 실패 지점부터 재소비 (at-least-once 재시도, 멱등 upsert라 안전)
4. retention(24h) 안에 해결 못 했으면 → 복구 화면에서 시각 지정 재발행 (S3 복구)

### 캡처 장애 (테이블 모니터링 상단 빨간 배너)
전 테이블 수집이 멈춘 상태 — 배너에 감지 시각·원인·전체 trace가 뜬다.

**`LogFileNotFoundException`이면 복구 시작 전에 offset SCN을 담은 archive가 남아 있는지
먼저 확인한다** (`v$archived_log where :scn between first_change# and next_change#`).
남아 있으면 로그 유실이 아니라 로그 스위치 중 아카이빙 대기 초과
([08-18 기록](incidents/2026-08-18-log-switch-archive-delay.md))이므로 재스냅샷 없이
task 재시작(`POST /connectors/dz-source/tasks/0/restart`)만으로 같은 offset에서 재개된다.

[복구 시작] → 방식 선택:
- **초기 스냅샷부터** (권장): 유실 없이 전 테이블 재구축
- **현재 시점부터**: 장애 구간 유실을 수용하고 스트리밍만 재개
- **타깃을 비우고 재적재** 체크: 소스에서 삭제된 행(고아)까지 정리하는 완전 재구축.
  스냅샷 완료까지 타깃 조회가 비어 보이므로 타깃을 읽는 작업이 없을 때만.

진행은 단계 팝업으로 표시되며(닫아도 계속), TRUNCATE 실행 주체를 물어본다 —
[시스템이 실행](권한 자동 점검) 또는 [직접/DBA가 실행](비워지면 자동 진행되는 홀드).
완료(go-live) 후 복구 화면에서 **정합 검증**으로 일치를 확인한다.

### 테이블 목록과 지표 조회 실패
테이블 모니터링의 행 목록은 항상 `/api/registrations`(PostgreSQL 조회) 기준이라 Kafka가
죽어도 그대로 보인다. 이벤트 수·lag 등 지표(`/api/metrics/tables`, Kafka AdminClient
조회)만 별도로 갱신되며, 이 조회가 실패하면 지표 셀은 "—"로 바뀌고 상단에
"지표 조회 실패(Kafka 연결 확인): ..." 배너가 뜬다 — 테이블이 0건으로 보이는
오검출(26-08-20 장애의 증상 중 하나)을 막기 위한 분리다.

### 평시 재스냅샷
테이블 모니터링 헤더의 [재스냅샷] — 장애가 아니어도 쓰는 운영 액션:
- 타깃 표류(누가 타깃에 직접 DML), 정합 검증 불일치의 소스 기준 복구
- Qlik Replicate의 Reload, AWS DMS의 Reload table에 해당

### S3(changelog) 복구
타깃이 훼손됐거나 retention을 넘긴 경우. 복구 화면에서 테이블·시작 시각 지정 →
재발행 → apply 완료 감지 → (자동 재개 체크 시) go-live.
시각은 보수적으로(더 과거로) 잡아도 안전하다 — recovery-job이 그 시각의 ts_ms 파티션
한 칸 앞부터 전부 재생하고, apply가 멱등 upsert라 중복이 무해.
완료 후 정합 검증(행 수 + 체크섬)으로 닫는다.

주의: **캡처가 멈췄던 구간은 S3 복구 대상이 아니다** — 그 구간은 changelog에도 없다.
그 경우는 재스냅샷이 정답 (위 캡처 장애 절차).

### 기존 등록 테이블의 해제·재등록 (changelog 스키마·라우팅이 바뀐 경우)

changelog 테이블 스키마(컬럼 구성)나 네임스페이스·라우팅 방식이 바뀌는 배포가 있다
(예: 2026-09-05 `_pos` 위치 컬럼 도입 — namespace가 `changelog`에서 `changelog_<prefix>`로,
route-field가 `source.table`에서 `_pos.topic`으로 전환). 이런 배포 이후 **이미 등록돼 있던
테이블은 새 iceberg-sink 설정으로 자동 이전되지 않는다** — 옛 changelog 테이블은 새 sink
인스턴스의 라우팅 대상이 아니라서 더 이상 새 레코드가 쌓이지 않는다. 옛 changelog는 삭제되지
않고 그대로 남지만(복구 원본 보존 원칙), 새 스키마로 이어받으려면 재등록이 필요하다.

절차:
1. 테이블 모니터링에서 해당 테이블 [삭제] — **changelog 보존(기본값)을 유지**한다. 옛
   changelog까지 지우는 옵션은 데이터가 되돌릴 수 없이 사라지므로, 정말 필요한 경우에만
   사용자가 직접 확인하고 체크한다.
2. CDC 등록 위저드에서 같은 테이블을 다시 등록한다 (스냅샷 모드는 보통 INITIAL — 현재
   상태 전체를 새 changelog에 처음부터 다시 쌓는다).
3. changelog 현황에서 새 이름(`changelog_<prefix>.<schema>_<table>`)의 테이블이 생기고
   `_pos`가 채워지는지 확인한다.
4. 옛 changelog 테이블(`changelog.<schema>_<table>`)은 그대로 남는다 — 더 이상 갱신되지
   않는 과거 이력으로 취급하고, 카탈로그에서 수동으로 정리할지는 별도로 판단한다(자동
   삭제하지 않음 — MinIO 오브젝트·Iceberg 테이블 삭제는 실행 전 사용자 확인 사항).

재등록 사이 구간은 캡처가 잠시 끊기지만, 재등록이 스냅샷부터 다시 시작하므로 그 구간의
변경분은 스냅샷 시점 데이터에 포함돼 유실 없이 복원된다(평시 재스냅샷과 같은 원리).

### DDL 변경
소스 DDL은 자동으로 수집돼 DDL 이력 탭에 쌓인다.
- 승인 → 타깃 이름(스키마·테이블)이 다르면 치환해서 타깃에 실행 후 재개
- 거부 → 해당 테이블 sink만 정지 (다른 테이블 무영향). 이후 타깃을 수동 정리하고 재개
- supplemental logging·GRANT 등 비전파성 DDL은 자동 무시(IGNORED) 처리

## 문제 추적 경로

1. **이벤트 탭** — 언제부터 무슨 일이 있었나 (장애 전이·복구 단계·스냅샷 진행 전부 기록)
2. **장애 배지/배너의 원인·trace** — Connect가 가진 스택트레이스를 UI가 바로 보여준다
3. **로그 파일** — 위 둘로 부족할 때: `~/deltazium-runtime/logs/`
   (connect.log = Debezium 포함 커넥터 상세, backend.log = 제어면)
