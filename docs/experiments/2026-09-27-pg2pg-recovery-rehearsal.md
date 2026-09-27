# changelog 복구 재발행 리허설 — PostgreSQL 소스 → PostgreSQL 타깃 (2026-09-27)

- 대상: Deltazium main(94e2415) — backend 8090, Connect 8083(Debezium 3.6.0.Final, Iceberg sink
  1.12.0-SNAPSHOT), recovery-job installDist 산출물(2026-09-08 빌드, 이후 소스 변경 없음)
- 소스: PostgreSQL 5433 `source` DB `cdc_tmp.orders`(등록 31, DDL 정책 AUTO) ·
  `cdc_tmp.items`(등록 32, MANUAL — `tag`·`tag2` 컬럼 매핑 비활성), topic_prefix `nhtest_src`
- 타깃: PostgreSQL 5181 `target` DB `cdc_tmp` 스키마(별도 인스턴스)
- changelog: `changelog_nhtest_src.cdc_tmp_orders` / `cdc_tmp_items` (Iceberg/MinIO, 1일 파티션)
- 기준: architecture.md 6.1(재발행·동일 sink 설정)·6.2(`_pos` 순서·시각 진입점·멱등)·6.4(리허설
  시나리오), 절차: operations.md "S3(changelog) 복구". 코드·설정·커넥터 등록은 리허설 중 수정하지
  않았다. 시각은 KST, epoch는 ms.

## 결론

- **items(타임스탬프 컬럼 없음)는 6.4 시나리오 ①~⑤를 통과했다.** 타깃 훼손(삭제 4·변조 1) →
  시각 지정 복구 → recovery-sink apply → 행 수·체크섬 일치, 정렬 덤프 diff 0. 비활성 컬럼
  필터(`field.include.list`) 상속, `_pos` 순서 재생, 같은 복구 재실행 멱등, go-live 후 라이브
  경로 복귀까지 관찰 포인트 전부 확인.
- **orders(`updated_at timestamptz`)는 recovery-sink apply가 실패했다(결함 R1).** recovery-job이
  재조립한 envelope에서 `updated_at`의 Debezium 논리 타입명(`io.debezium.time.ZonedTimestamp`)이
  사라져 plain `string`이 되고, JDBC sink가 varchar로 바인딩해 PostgreSQL이
  `column "updated_at" is of type timestamp with time zone but expression is of type character
  varying`(SQLSTATE 42804)으로 거부 → task FAILED, 복구 토픽 122건 미적용. **PG 타깃에서
  timestamptz 컬럼이 있는 테이블은 현재 changelog 복구가 불가능하다.** 재트리거로는 복구되지
  않는다(결함 R2). 타깃은 수동 SQL로 소스 값에 맞춰 정리했다.
- 부수 관찰: 실패한 실행의 상태가 `DONE`에 30분 머문다(R3), 실행 이력이 backend 메모리에만
  있다(정보), 시각 진입점의 1일 파티션 프루닝 때문에 "T0부터" 복구는 T0 이전 파티션의 행을
  되돌리지 못한다(설계 그대로 — 운영 절차의 "보수적으로 더 과거" 지침이 필수임을 실측).

## 사전 상태 (1단계, 20:16)

| 항목 | 값 |
|---|---|
| verify 31(orders) | 49/49, checksum 120660560034 양쪽 일치 |
| verify 32(items) | 82/82, checksum 189032495112 양쪽 일치 |
| 정렬 덤프 diff | orders 0, items 0 (items는 활성 컬럼 `id,order_id,sku,qty`만) |
| changelog | orders 97건(09-23: 96, 09-27: 1), items 162건(09-23: 160, 09-27: 2) — Kafka 원본 토픽 end offset 97/162와 동일 |
| 복구 sink | `dz-recovery-sink-nhtest_src-*` 미존재, `GET /api/recovery` 빈 목록 |
| 커넥터 | source·iceberg·jdbc-sink×2 전부 RUNNING. items sink에 `field.include.list=id,order_id,sku,qty`, orders sink는 필터 없음 |

## 단계별 결과

### 2단계 — 라이브 DML로 이력 생성 (T0 = 1790507822960, 20:17:02.96)

orders: INSERT 5(5001~5005) → 5001을 3회 순차 UPDATE(NEW→PAID→SHIPPED→DELIVERED, amount
10→11→12→13) → 기존 1·2 UPDATE(status RH_UPD, note) → DELETE 5005·3.
items: INSERT 4(6001~6004, `tag`·`tag2` 채움) → 6001을 2회 UPDATE(qty 1→2→3) → 기존 1 UPDATE
(qty 999) → DELETE 6004·2.

- Kafka 원본 토픽 end offset 97→111(+14 = 이벤트 12 + tombstone 2), 162→173(+11 = 9 + 2).
- 타깃 반영 약 18초 후 확인: 52/84, verify 양쪽 일치(orders 123723013613, items 189641156076).
- changelog는 iceberg-sink 커밋 주기(`iceberg.control.commit.interval-ms=60000`) 뒤 20:18:09에
  109/171로 반영(오늘 파티션 13/11). tombstone은 changelog에 남지 않았다(+12/+9).

### 3단계 — 타깃 훼손 (20:17:54, 소스 무변경)

| 테이블 | 훼손 |
|---|---|
| orders | DELETE 5001·5002(오늘 변경 행), DELETE 10·11(스냅샷 이후 미변경 행), UPDATE 5003(amount 0, status HACKED), UPDATE 20(status HACKED), 소스에서 지운 3을 고아로 INSERT |
| items | DELETE 6001·6002(오늘 변경), DELETE 30·31(미변경), UPDATE 6003(qty -1) |

verify: orders 52/49 checksum 불일치, items 84/80 checksum 불일치 — 훼손 감지.

### 4단계 — 복구 실행

`POST /api/recovery {"registeredTableId":N,"fromTimeMs":...,"autoResume":true}` 세 차례.

| 실행 | 대상 | fromTimeMs | scan-from(ts_ms 파티션 한 칸 앞) | 재발행 | 결과 |
|---|---|---|---|---|---|
| A (run 1·2, 20:18:35) | orders·items | T0 1790507822960 | 1790380800000 (09-26 00:00Z) | 13 / 11건 | items: DONE→APPLIED→LIVE 15초. **orders: recovery-sink task FAILED(R1)**, 상태 DONE 고착 |
| B (run 3·4, 20:20:38) | orders·items | 1790089200000 (09-23 00:00 KST) | 1789948800000 (09-21 00:00Z) | 109 / 171건(전량) | items: LIVE 15초, 전량 복원. orders: 재트리거해도 task FAILED 유지(R2) |
| C (run 5, 20:21:49) | items | 1790089200000 | 동일 | 171건 | LIVE 4초, 결과 B와 동일(멱등) |

관찰된 동작:
- 트리거 시 backend가 `dz-recovery-sink-nhtest_src-cdc_tmp_{orders,items}`를 템플릿
  `connectors/recovery-sink.json.tmpl`로 배포(PUT config + resume) → recovery-job 프로세스 기동 →
  `RECOVERY_RESULT` 파싱 → consumer group lag 0 확인 → recovery-sink pause → (autoResume) 라이브
  jdbc-sink resume. 이벤트 탭에 RECOVERY_STARTED(WARN) → RECOVERY_DONE(재발행 완료) →
  RECOVERY_DONE(apply 완료 확인·정지) → RESUMED(go-live) 순으로 기록됨(이벤트 221~237).
- **items 복구 sink 설정에 `field.include.list=id,order_id,sku,qty`가 그대로 들어갔다**
  (`RecoveryService.deployRecoverySink` → `RegistrationService.fieldIncludeConfig`). 복구 토픽의
  items envelope에는 `tag`·`tag2` 값이 들어 있는데(`{'qty': 3, 'tag': 't1-final', 'tag2': 'tt1'}`)
  타깃 items 테이블에는 두 컬럼이 없다 — 필터가 없었으면 orders처럼 실패했을 경로. 관찰 (a) 통과.
- 실행 A의 복구 토픽 재발행 순서(orders 13건): 2001(c, 오늘 파티션의 기존 1건) → 5001~5005(c) →
  5001 u PAID → u SHIPPED → u DELIVERED → 1 u → 2 u → 3 d → 5005 d. items 11건: 3001·3002(c) →
  6001~6004(c) → 6001 u qty 2 → u qty 3 → 1 u → 2 d → 6004 d. 소스 `lsn` 오름차순과 같은 순서
  이고 recovery-job은 `_pos`(partition, offset)로만 정렬했다(로그·코드 `REPLAY_ORDER`). 관찰 (b) 통과.
- 복구 토픽 `dz-recovery.nhtest_src.cdc_tmp_*`는 첫 트리거에 자동 생성됐고 broker 기본 retention
  24h(`log.retention.hours=24`)를 따른다. 실행마다 append(items end offset 11→182→353)되고
  recovery-sink consumer group offset이 pause를 거쳐 유지되므로 재실행은 새 구간만 소비한다
  (`CURRENT-OFFSET 353 / LAG 0`).
- recovery-job 로그: `~/deltazium-runtime/logs/recovery-cdc_tmp_{orders,items}-<runId>.log`.
  `재발행 대상 N건 (from-ts-ms=…, scan-from-ts-ms=…)` → `RECOVERY_RESULT published=N skipped=0`.
- 트리거가 라이브 jdbc-sink를 정지하지 않는다 — 복구 중에도 RUNNING. 이번 시나리오(소스 무변경)
  에서는 충돌이 없었다.

### 5단계 — 정합 검증

- 실행 A 직후(items): 84/82 불일치. 6001·6002 복원, 6003 qty 3으로 회복, 2·6004 부재 — 그러나
  스냅샷 이후 미변경이던 30·31은 되돌아오지 않았다. 스캔 범위(09-26 00:00Z 이후)에 그 행의
  이벤트가 없기 때문 — 6.2의 "그 이후 전부 재생" 그대로. operations.md의 "시각은 보수적으로
  더 과거로" 지침이 실제로 필요한 이유가 이것이다(훼손 행이 언제 마지막으로 변경됐는지 모르면
  스냅샷 이전 시각을 줘야 한다).
- 실행 B 직후(items): verify 84/84, checksum 189641156076 양쪽 일치. 정렬 덤프 diff 0. 30·31 복원,
  6001 qty 3(마지막 UPDATE 값), 2·6004 없음.
- 실행 C 직후(items): verify 일치, 덤프 diff 0, 실행 B 덤프와 `cmp` 동일. 관찰 (c) 통과.
- orders: 실행 A·B 후에도 3단계 훼손 그대로(52/49). 행 diff — 고아 3 잔존, 10·11·5001·5002 부재,
  20·5003 HACKED. 복구 토픽에는 A 13건 + B 109건 = 122건이 쌓였으나 recovery-sink consumer group은
  committed offset 없음(전량 미적용).

### 6단계 — 복구 후 라이브 계속 (20:22:52)

소스 INSERT orders 5100 · items 6100 → 약 30초 뒤 확인 시점에 타깃 도착, 라이브 jdbc-sink
consumer group lag 0. items verify 85/85 일치. items recovery-sink는 PAUSED 유지(평시 정지),
orders recovery-sink는 RUNNING/task FAILED로 남음.

### 마무리 — orders 타깃 수동 정리 (20:23:54)

R1로 복구가 불가능하므로 타깃 `cdc_tmp.orders`의 3·10·11·20·5001·5002·5003을 DELETE 후 소스
값으로 INSERT(허용된 우회: 수동 SQL). 이후 verify 31: 53/53 checksum 126082545095 일치, 덤프
diff 0.

## 관찰 포인트 판정

| | 항목 | 판정 | 근거 |
|---|---|---|---|
| (a) | 복구 sink가 `field.include.list`(tag·tag2 제외)를 물려받는가 | 통과 | `GET /connectors/dz-recovery-sink-nhtest_src-cdc_tmp_items` config, envelope에 tag 값이 있어도 apply 성공 |
| (b) | `_pos` 순서 재생으로 UPDATE 순서 보존 | 통과 | 복구 토픽 순서(5001 PAID→SHIPPED→DELIVERED, 6001 qty 2→3), 최종 타깃 값 = 마지막 UPDATE |
| (c) | 같은 복구 재실행 멱등 | 통과(items) | 실행 B·C 타깃 덤프 동일, verify 일치. orders는 R1로 판정 불가 |
| (d) | 소요 시간 | — | 재발행(recovery-job 기동 포함) 13/11건 4.7초, 109/171건 4.5초; 트리거→LIVE items 15초(A)·15초(B)·4초(C). B와 C는 connect.log상 같은 순서(config updated → connector 재시작 → resume → pause)였고 재발행 건수도 같은데 소요가 다르다 — 원인 미확인 |
| 추가 | 시각 진입점 파티션 프루닝 | 설계대로 | T0 → 09-26 00:00Z부터 스캔, 09-23 파티션 미포함 → 미변경 행 미복원 |

## 결함

### R1 — 재조립 envelope에서 Debezium 논리 타입명 소실 → PG timestamptz 컬럼 apply 실패
- 증상: `dz-recovery-sink-nhtest_src-cdc_tmp_orders` task FAILED. connect.log 11:18:40Z:
  `Batch entry 0 INSERT INTO cdc_tmp.orders (...updated_at...) VALUES (... ('2026-09-27T11:17:02.971965Z') ...)
  ON CONFLICT (id) DO UPDATE ... ERROR: column "updated_at" is of type timestamp with time zone but
  expression is of type character varying` (SQLSTATE 42804).
- 원인: 라이브 토픽의 `after.updated_at` 스키마는 `{"type":"string","name":"io.debezium.time.ZonedTimestamp",
  "version":1}`이고 JDBC sink가 이 논리 타입명으로 timestamptz 바인딩을 고른다. Iceberg changelog는
  이 컬럼을 `string`으로 저장하고(iceberg-sink의 Connect→Iceberg 변환에 ZonedTimestamp 대응 타입 없음),
  이 컬럼을 `string`으로 저장하고(재조립 스키마가 `string`으로 나온 것으로 확인 — Iceberg에 논리
  타입명을 실을 자리가 없다), `ConnectJsonAssembler.fieldSchema`는 Iceberg `STRING`을 plain
  `{"type":"string"}`으로 되돌린다.
  파일 헤더에 "원본 Debezium 논리 타입명(io.debezium.time.*)까지는 복원하지 않는다 — apply 동등성이
  기준"이라 적혀 있는데, PG 타깃 timestamptz에서는 그 전제(apply 동등성)가 성립하지 않는다.
  `EnvelopeRoundTripTest`는 payload 동등성만 보고 스키마 `name`은 보지 않는다.
- 영향: PostgreSQL 타깃에서 `timestamptz`(Debezium `ZonedTimestamp`) 컬럼을 가진 테이블은 changelog
  복구 불가. `timestamp`(without tz)·`date`는 Debezium이 int64/int32 논리 타입으로 내보내고 Iceberg
  timestamp/date로 저장되므로 assembler가 Connect `Timestamp`/`Date`로 복원한다 — 이번 리허설에서는
  미검증. Oracle 타깃(`TIMESTAMP WITH TIME ZONE`)에서 같은 증상이 나는지도 미확인.
- 재현: orders처럼 timestamptz 컬럼이 있는 PG 테이블에 `POST /api/recovery` 1회 → recovery-sink
  status에서 task trace 확인. 복구 토픽 첫 레코드 스키마:
  `kafka-console-consumer.sh --topic dz-recovery.nhtest_src.cdc_tmp_orders --partition 0 --offset 0 --max-messages 1`
  → `after` 필드의 `updated_at`에 `name` 없음.
- 부수 차이(실패 원인은 아님, 기록만): 재조립 스키마의 `id`가 `optional:true`(원본 false), `amount`
  precision 38(원본 12), envelope `name`이 `recovery.Envelope`, 최상위 필드 순서 상이.

### R2 — 실패한 recovery-sink는 재트리거로 회복되지 않는다
실행 B의 orders 트리거는 같은 config로 PUT → Connect가 "connector-only config update"로 커넥터만
재시작하고 task는 FAILED 그대로(`RUNNING ['FAILED']` 유지), resume은 RUNNING 커넥터에 no-op.
복구 토픽에는 109건이 추가로 쌓였다. R1이 고쳐지더라도 실패 task를 되살리는 경로(task restart)가
backend에 없다 — 운영자가 Connect REST로 `POST /connectors/<name>/tasks/0/restart`를 직접 호출해야
한다. 이번엔 호출하지 않았다(R1 때문에 다시 실패할 뿐이므로).

### R3 — apply 실패 시 실행 상태가 `DONE`에 30분 고착, 이벤트는 커넥터 모니터에만
`awaitApplyThenPauseSink`는 lag 0을 5초 간격으로 30분 기다린다. recovery-sink task가 FAILED여도
lag는 줄지 않으므로 실행 1·3은 `DONE`(published=13/109)으로 남아 있고 복구 화면 실행 이력만으로는
실패를 알 수 없다. 실패 사실은 커넥터 상태 모니터가 남긴 `CONNECTOR_FAILED` 이벤트(225,
"관측 시작 시점에 이미 FAILED")로만 드러났다. 30분 만료 시 이벤트 238(20:48:43, RECOVERY_DONE/WARN
"apply가 30분 내 완료되지 않아 recovery-sink를 정지하지 않음 — lag 확인 필요 (자동 재개도 보류됨 —
수동 재개 필요)")이 남았고, 실행 1의 상태는 그 뒤에도 `DONE`이다 — 실행 이력에는 종결 상태가
기록되지 않는다. 이벤트 종류가 성공 시와 같은 `RECOVERY_DONE`이라 종류 필터로는 구분되지 않는다.

### 정보 (결함 아님)
- `RecoveryService.runs`는 `ConcurrentHashMap` — 실행 이력이 backend 재기동 시 사라진다(리허설 시작
  시 빈 목록이었던 이유).
- 트리거가 라이브 jdbc-sink를 정지하지 않는다. 복구와 라이브가 같은 타깃 행을 동시에 쓰는 경우의
  순서 보장은 이번 시나리오(소스 무변경) 범위 밖.
- tombstone(delete 뒤 null value)은 Kafka 원본 토픽에는 있고 changelog에는 없다 — 재발행에도 없다.
  recovery-sink의 `delete.enabled=true`는 `op=d` envelope로 삭제하므로 apply에는 영향 없음.

## 소요 시간 (KST)

| 구간 | 시각 | 경과 |
|---|---|---|
| 2단계 DML → 타깃 반영 확인 | 20:17:02 → 20:17:21 | 18초(확인 시점 기준, 실제 반영은 더 빠를 수 있음) |
| 2단계 DML → changelog 커밋 | 20:17:02 → 20:18:09 | 67초(commit interval 60초) |
| 실행 A 트리거 → items LIVE | 20:18:35.7 → 20:18:50.5 | 15초 |
| 실행 B 트리거 → items LIVE | 20:20:38.7 → 20:20:53.2 | 15초 |
| 실행 C 트리거 → items LIVE | 20:21:49.5 → 20:21:53.2 | 4초 |
| recovery-job 프로세스(기동~RECOVERY_RESULT) | — | 4~5초(13건도 171건도 비슷 — JVM·카탈로그 기동이 지배) |
| 6단계 라이브 INSERT → 타깃 확인 | 20:22:52 → 20:23:25 | 33초(확인 시점 기준) |

## 남긴 상태

- 소스 `cdc_tmp.orders` 53행(5001~5004·5100 추가, 3·5005 삭제, 1·2 변경), `cdc_tmp.items` 85행
  (6001~6003·6100 추가, 2·6004 삭제, 1 변경). 타깃은 두 테이블 모두 소스와 일치(orders는 수동 정리).
- 커넥터: `dz-recovery-sink-nhtest_src-cdc_tmp_items` PAUSED(평시 정지, 정상),
  **`dz-recovery-sink-nhtest_src-cdc_tmp_orders` RUNNING/task FAILED** — 삭제·재시작하지 않았다.
  R1 수정 후 task restart 시 복구 토픽 122건이 타깃에 upsert된다(멱등이라 타깃은 그대로일 것이나
  실측 전).
- 복구 토픽 `dz-recovery.nhtest_src.cdc_tmp_orders`(122건, 미소비) · `cdc_tmp_items`(353건, 전량 소비),
  retention 24h.
- backend 메모리의 복구 실행 1~5(재기동 시 소실). 실행 1의 30분 만료 WARN은 20:48:43에 발생
  (이벤트 238), 실행 3의 것은 20:50:43경 예상 — 문서 작성 시점에는 미확인
  (`GET /api/events?schema=cdc_tmp&table=orders`).
- 이 리허설 중 다른 등록·커넥터·소스·타깃 객체는 조회만 했다.

## 재확인 명령

```
# 정합 검증
curl -s -X POST localhost:8090/api/recovery/verify -H 'Content-Type: application/json' -d '{"registeredTableId":31}'
curl -s -X POST localhost:8090/api/recovery/verify -H 'Content-Type: application/json' -d '{"registeredTableId":32}'
# 복구 실행 이력·이벤트
curl -s localhost:8090/api/recovery
curl -s "localhost:8090/api/events?schema=cdc_tmp&limit=40"
# recovery-sink 상태와 R1 trace
curl -s localhost:8083/connectors/dz-recovery-sink-nhtest_src-cdc_tmp_orders/status
curl -s localhost:8083/connectors/dz-recovery-sink-nhtest_src-cdc_tmp_items
# 복구 토픽·consumer group
~/deltazium-runtime/kafka_2.13-4.3.1/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic dz-recovery.nhtest_src.cdc_tmp_orders
~/deltazium-runtime/kafka_2.13-4.3.1/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group connect-dz-recovery-sink-nhtest_src-cdc_tmp_items
# recovery-job 로그
ls ~/deltazium-runtime/logs/recovery-cdc_tmp_*-*.log
# 행 단위 diff (items는 활성 컬럼만)
PGPASSWORD=nhtest psql -h 127.0.0.1 -U nhtest -d source -p 5433 -Atc "select id,order_id,sku,qty from cdc_tmp.items order by id" > /tmp/s.txt
PGPASSWORD=nhtest psql -h 192.168.4.123 -U nhtest -d target -p 5181 -Atc "select id,order_id,sku,qty from cdc_tmp.items order by id" > /tmp/t.txt
diff /tmp/s.txt /tmp/t.txt
```
