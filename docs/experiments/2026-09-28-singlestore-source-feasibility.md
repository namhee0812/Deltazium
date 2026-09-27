# 2026-09-28 — SingleStore CDC 소스 편입 가능성 검증

## 목적

SingleStore를 Deltazium 파이프라인의 **소스**로 편입할 수 있는지 판정한다. 타깃 지원은 별도
문서(`2026-09-28-singlestore-target.md`)에서 이미 검증됐고, 그때 소스는 범위 밖으로 남겼다.
이 문서는 구현이 아니라 가부 판단이 목적이다.

## 환경

- SingleStore 서버 **9.0.44** — `ghcr.io/singlestore-labs/singlestoredb-dev` 컨테이너
  `singlestoredb-dev`(podman, 호스트 3307). `enable_observe_queries=ON`(실험 전 설정).
  `snapshots_to_keep=2`, `snapshot_trigger_size=2147483648`(둘 다 기본값).
- Kafka Connect 워커 4.3.1, 기존 플러그인은 Debezium 3.6.0.Final 계열.
- 실험용 DB `exp_s2_src`, 테이블 `t_basic(id INT PK, name VARCHAR(50), amt DECIMAL(10,2),
  upd_ts DATETIME)`. 커넥터 이름 `exp-s2-source`, `topic.prefix=exps2`.
- converter는 우리 표준과 동일하게 JSON(`schemas.enable=true`).

## 커넥터 입수·설치

- **GitHub 배포 zip 없음.** `https://github.com/singlestore-labs/singlestore-debezium-connector`
  는 HTTP 404(리포 비공개 또는 삭제). API로 releases 조회도 404. Maven Central에만 존재하고,
  `assembly` 프로필이 `activeByDefault=false`라 Maven에도 connector-distribution 아카이브가
  올라가 있지 않다(0.1.9 디렉터리에 jar/sources/javadoc/pom만).
- 따라서 Gradle로 `com.singlestore:singlestore-debezium-connector:0.1.9`의 런타임 의존성을
  해석해 jar 13개를 모아 플러그인 디렉터리에 배치했다:
  `~/deltazium-runtime/connect-plugins/singlestore-debezium-connector-0.1.9` (**5.3 MB**).
  구성: 커넥터 jar, `debezium-core`/`debezium-api` **2.5.1.Final**, `singlestore-jdbc-client`
  1.2.6, `jts-core`/`jts-io-common` 1.20.0, jackson 2.13.5 4종, json-simple, slf4j-api.
- 기존 플러그인 디렉터리는 건드리지 않았다. Connect 워커를 1회 재기동해 플러그인을 인식시켰고
  (`connect.pid` kill 후 `deploy/start-infra.sh` — pg/minio/kafka는 멱등 스킵),
  재기동 후 기존 커넥터 15개는 전부 RUNNING으로 복귀했다.
- 워커가 보고한 플러그인: `com.singlestore.debezium.SingleStoreConnector` **0.1.9**.
  Debezium 2.5.1 기반 커넥터가 Connect 4.3.1 워커에서 로드·구동됐다(플러그인 classloader 격리).

## 판정 표

| # | 항목 | 판정 | 근거 |
|---|---|---|---|
| 1 | 기본 캡처 | **가능** | INSERT/UPDATE/DELETE가 모두 Kafka로 흘렀다. 토픽 이름은 `<topic.prefix>.<db>.<table>` = `exps2.exp_s2_src.t_basic`. DELETE 뒤에 tombstone(값 null)도 발행된다 |
| 2 | envelope 구조 | **부분** — 형태는 표준, **before가 항상 null** | 최상위 필드는 `before/after/source/op/ts_ms/transaction`, 스키마명 `...Envelope`. 그러나 **UPDATE의 before가 null이고, DELETE는 before·after가 모두 null**이다 |
| 3 | 초기 스냅샷 | **불가** — op='r'이 나오지 않는다 | `snapshot.mode`는 `initial`/`initial_only`/`when_needed`/`no_data` 4종 존재. `initial`로 등록했으나 스냅샷 단계가 **0건**을 내보냈고, 등록 시점 기존 2행은 스트리밍 경로에서 **op='c'**, `source.snapshot="false"`로 흘러왔다 |
| 4 | notification 채널 | **가능** | `notification.enabled.channels=sink`가 동작. `exps2-notifications`에 `io.debezium.connector.common.Notification` 스키마로 `STARTED`/`COMPLETED`(aggregate_type `Initial Snapshot`) 2건 발행 |
| 5 | DDL 감지 | **불가** — schema change topic 없음. 지문 비교는 원리상 성립하나 전제가 깨진다 | schema change topic이 생성되지 않는다(`include.schema.changes=true`를 줘도 토픽은 `exps2.exp_s2_src.t_basic`과 `exps2-notifications` 둘뿐). value.schema는 컬럼 구성을 담으므로 지문 비교 자체는 가능하지만, **DDL 직후 커넥터가 조용히 멈춰** 새 스키마 레코드가 나오지 않는다(아래 6) |
| 6 | 운영 제약 | 아래 별도 절 | |

## 실제 레코드 샘플

INSERT (op='c'). 등록 시점 기존 행도 이 형태로 왔다 — op='r'이 아니다:

```json
{"op":"c",
 "before":null,
 "after":{"id":1,"name":"alpha","amt":"BBo=","upd_ts":1790526263000},
 "source":{"version":"0.1.9","connector":"singlestore","name":"exps2",
           "ts_ms":1790526362875,"snapshot":"false","db":"exp_s2_src","sequence":null,
           "table":"t_basic",
           "txId":"0300000000000000010000000000000010ffff...0000",
           "partitionId":1,
           "offsets":[null,"000000000000000a000000000000000700000000000070b100000000"]},
 "ts_ms":1790526362879,"transaction":null}
```

UPDATE (op='u') — **before가 null**:

```json
{"op":"u",
 "before":null,
 "after":{"id":2,"name":"beta-updated","amt":"Jw8=","upd_ts":1790526263000},
 "source":{"...":"...","partitionId":0,
           "offsets":["0000000000000009000000000000000b000000000000b06b00000000",
                      "000000000000000a000000000000000b000000000000b06400000000"]},
 "ts_ms":1790526373994,"transaction":null}
```

DELETE (op='d') — **before·after 둘 다 null**. 값은 Kafka 메시지 키에만 있다:

```json
키:  {"schema":{...,"name":"exps2.exp_s2_src.t_basic.Key"},"payload":{"id":1}}
값:  {"op":"d","before":null,"after":null,
      "source":{"...":"...","db":"exp_s2_src","table":"t_basic","partitionId":1,
                "offsets":["...b06b00000000","...c05600000000"]},
      "ts_ms":1790526373994,"transaction":null}
그 다음 레코드: 같은 키 + 값 null (tombstone)
```

notification:

```json
{"payload":{"id":"416de375-05e2-4ca1-bf1e-b27c02cd6fd3","type":"STARTED",
            "aggregate_type":"Initial Snapshot",
            "additional_data":{"connector_name":"exps2"},"timestamp":1790526361595}}
{"payload":{"id":"fd12ca9c-a64c-4046-b265-6244a0e6977c","type":"COMPLETED",
            "aggregate_type":"Initial Snapshot",
            "additional_data":{"connector_name":"exps2"},"timestamp":1790526362863}}
```

`source` 블록 필드: `version/connector/name/ts_ms/snapshot/db/sequence/table/txId/partitionId/offsets`.
SCN·LSN에 대응하는 위치는 `offsets` — **SingleStore 파티션 수만큼의 hex 문자열 배열**이다
(스칼라가 아니다).

## 코드로 확인한 동작 원리

소스 jar(`singlestore-debezium-connector-0.1.9-sources.jar`)를 받아 확인했다.

**before가 null인 이유는 설계가 아니라 미구현이다.**
`SingleStoreStreamingChangeEventSource`는 OBSERVE 결과셋에서 `after`만 만들고 emitter의
`before` 인자에 리터럴 `null`을 넘긴다:

```java
Object[] after = ObserveResultSetUtils.rowToArray(rs, columnPositions,
    connectorConfig.populateInternalId());

dispatcher.dispatchDataChangeEvent(partition, table,
    new SingleStoreChangeRecordEmitter(
        partition, offsetContext, clock, operation,
        null,        // ← before
        after, internalId, connectorConfig, schema.tableFor(table)));
```

`SingleStoreChangeRecordEmitter.emitDeleteRecord()`는 `getOldColumnValues()`(= 이 null)로
oldValue를 만들므로 DELETE의 before가 비고, after도 애초에 emit 대상이 아니라 **값이 전혀 없는
DELETE envelope**이 나온다. 키만 `InternalIdUtils.generateKey()`로 채워진다(PK가 있으면 PK,
없으면 `internalId`).

**DDL 처리는 TODO로 비어 있다.** 같은 파일의 `catch (SQLException e)` 바로 위에
`// TODO: handle schema change event` 주석이 있다.

**stale offset 조건**은 커넥터가 SQLSTATE `HY000`, 에러코드 `2851`,
메시지 `"The requested Offset is too stale. Please re-start the OBSERVE query from the latest
snapshot."`을 잡아 `StaleOffsetException`으로 바꾸며, 로그에 이렇게 안내한다(원문):

> Offset the connector is trying to resume from is considered stale. Therefore, the connector
> cannot resume streaming. You can use either of the following options to recover from the
> failure: (1) Delete the failed connector, and create a new connector with the same
> configuration but with a different connector name. (2) Pause the connector and then remove
> offsets, or change the offset topic. To help prevent failures related to stale offsets, you
> can increase the value of the following engine variables in SingleStore:
> `snapshots_to_keep`, `snapshot_trigger_size`.

즉 **stale이 나면 자동 복구 경로가 없고 재스냅샷(새 커넥터)뿐**이다. 그런데 이 커넥터의
스냅샷은 3번 항목대로 op='r' 재현이 안 되므로, 재스냅샷이 곧 전량 op='c' 재발행이 된다.

부수 관찰: 스냅샷 완료 로그의 offset context가 `SqlServerOffsetContext [...]`로 찍힌다 —
SQL Server 커넥터에서 복사한 코드의 `toString()`이 남아 있다.

## 운영 제약 (6번 항목 상세)

| 제약 | 내용 |
|---|---|
| 커넥터 1개 = **테이블 1개** | 설정 키가 `table.include.list`가 아니라 **`database.table`**(단수, "The name of the table from which the connector should capture changes"). 스냅샷 코드에도 `assert (snapshotContext.capturedTables.size() == 1)`이 있다. 테이블 N개면 커넥터 N개 |
| 전역 설정 | `enable_observe_queries=ON`이 필요(엔진 변수) |
| stale offset | 트랜잭션 로그 보존(`snapshots_to_keep`, `snapshot_trigger_size`)을 넘기면 OBSERVE가 2851로 끊기고, 커넥터 재생성/offset 삭제 외에 복구 수단이 없다 |
| 재시작 offset 이어받기 | **작동한다.** DDL로 멈춘 뒤 밀려 있던 3건을 커넥터 restart 후 따라잡았다(토픽 offset 6→9). 로그: `A previous offset indicating a completed snapshot has been found. Only schema will be snapshotted.` |
| 레코드 순서 | `source.offsets`는 SingleStore 파티션별 배열이고 파티션 간 전역 순서는 없다. 실측에서도 DB 실행 순서(id=4 INSERT → id=3 UPDATE)와 토픽 도착 순서(id=3 UPDATE → id=4 INSERT)가 달랐다. 단 같은 PK는 같은 SingleStore 파티션·같은 Kafka 키라서 **PK별 순서는 보존**된다 — architecture.md 6.2가 요구하는 것은 PK별 순서뿐이므로 이 항목 자체는 계약 위반이 아니다 |
| 부가 설정 | `populate.internal.id`(after에 internalId 추가), `offsets`(snapshot.mode=no_data일 때 시작 offset 직접 지정), `geography.handling.mode`, `database.ssl.*` |
| 커넥터 요구 버전 | pom의 Confluent Hub 메타데이터에 `SingleStore 8.7.16+` |
| Debezium 버전 | 커넥터가 **Debezium 2.5.1.Final**에 고정. 우리 다른 커넥터는 3.6.0.Final |

## 치명적 결함: DDL 후 조용한 정지

`ALTER TABLE t_basic ADD COLUMN memo VARCHAR(100)` 실행 후 INSERT·UPDATE를 걸었으나
**토픽 offset이 6에서 움직이지 않았다**(30초 관찰). 그 사이:

- 커넥터·태스크 상태는 계속 `RUNNING`, `trace` 비어 있음
- connect.log에 에러 없음

즉 **에러도 FAILED 상태도 없이 CDC만 멈춘다.** 감시로 잡을 신호가 없다. 커넥터를 restart하면
밀린 변경을 저장된 offset에서 따라잡고, 이후 레코드의 value.schema에는 새 컬럼 `memo`가 반영된다
(따라잡은 UPDATE의 after는 `"memo": null`). 회복은 되지만 **수동 개입이 전제**다.

## 치명적 결함: DELETE가 복구 경로에서 소실된다

우리 changelog 스키마(architecture.md 5.1)의 컬럼은 `op/before/after/source/ts_ms/_pos`로,
**Kafka 메시지 키를 저장하지 않는다.** 키는 changelog에 들어가지 않고, 재발행 시 값에서 되만든다.

`recovery-job`의 재조립은 키를 `after`에서, `after`가 null이면 `before`에서 만든다
(`recovery-job/src/main/java/io/deltazium/recovery/envelope/ConnectJsonAssembler.java`):

```java
Object after = row.getField("after");
Record image = after != null ? (Record) after : (Record) row.getField("before");
if (image == null || rowType == null) {
    return null;
```

그리고 키가 null인 행은 `RecoveryJob`에서 **skip된다**:

```java
ObjectNode key = assembler.key(table.schema(), row, keyColumns);
if (key == null) {
    skipped++;
    continue;
}
```

SingleStore의 DELETE는 before·after가 모두 null이므로 changelog 행에 키 재료가 남지 않는다.
결과적으로 **복구 재발행에서 모든 DELETE가 건너뛰어지고**, 타깃에는 삭제가 반영되지 않아
삭제된 행이 되살아난다. 라이브 경로(JDBC sink)는 Kafka 키를 직접 쓰므로 삭제가 반영되지만,
**라이브와 복구의 결과가 달라진다** — architecture.md 6.1의 "계열 안에서 라이브와 복구는 같은
경로" 원칙과 5.1 설계 불변식 1(changelog 한 행에서 envelope을 손실 없이 재조립)에 정면으로 걸린다.

참고로 SingleStore 엔진 자체는 8.9부터 OBSERVE의 DELETE 레코드에 internalId 대신 PK를 채우므로,
값이 없는 것은 엔진이 아니라 커넥터가 `before`를 버리기 때문이다.

## 최종 판정

**현 시점(커넥터 0.1.9)에서 SingleStore를 소스로 편입하는 것은 진행 가치가 없다.**

근거를 무게 순으로:

1. **DELETE가 복구 경로에서 소실된다.** before·after가 모두 null이라 changelog에서 키를 복원할
   수 없고 recovery-job이 해당 행을 skip한다. 이 프로젝트의 존재 이유가 복구 재발행(5절·6절)인데
   그 지점이 깨진다. 우회하려면 changelog 스키마에 키 컬럼을 추가해야 하는데, 5절 스키마는
   "임의 변경 금지"로 고정돼 있고 소스 하나 때문에 바꿀 성질이 아니다.
2. **DDL 후 조용히 멈춘다.** 에러도 FAILED도 없이 CDC만 정지한다. 7절 DDL 승인 워크플로는
   schema change topic이나 최소한 "변경이 계속 흐른다"를 전제하는데 둘 다 성립하지 않는다.
3. **초기 스냅샷이 op='r'로 오지 않는다.** 등록 시점 기존 행이 op='c'로 들어온다. 멱등 upsert라
   타깃 적재는 되지만, changelog에는 실제 INSERT와 스냅샷 read가 구분 없이 쌓인다.
4. **커넥터 1개 = 테이블 1개.** 소스 테이블 N개면 커넥터 N개 — 우리 backend의 등록 모델
   (소스 커넥터 1개에 `table.include.list`로 테이블 추가)과 구조가 다르다.
5. **공급 경로가 불안정하다.** GitHub 리포가 404라 소스·이슈·릴리스 노트를 볼 수 없고,
   배포 아카이브도 없어 의존성을 직접 모아야 한다. 공식 Debezium 프로젝트가 아니고
   Debezium 2.5.1에 고정돼 있다.

1·2번은 커넥터 수정 없이는 우회 불가다. before 채우기와 DDL 처리는 둘 다 커넥터
(`SingleStoreStreamingChangeEventSource`)에서 해결할 문제인데, 자체 커넥터 작성은 이 리포의
절대 규칙이 금지한다.

**재검토 조건**: 커넥터가 (a) UPDATE·DELETE에 before를 채우고, (b) DDL을 처리하게 되면
(GitHub 리포 공개 재개 또는 상위 버전 릴리스) 다시 볼 값이 있다. 그전까지 SingleStore는
**타깃 전용**으로 둔다.

## 정리한 자원 / 남긴 자원

정리함:

- Connect 커넥터 `exp-s2-source` — stop → `DELETE /connectors/exp-s2-source/offsets` →
  `DELETE /connectors/exp-s2-source`. 삭제 후 커넥터 목록은 기존 15개, 전부 RUNNING.
- Kafka 토픽 `exps2.exp_s2_src.t_basic`, `exps2-notifications` — 삭제 완료.

남김:

- 플러그인 디렉터리 `~/deltazium-runtime/connect-plugins/singlestore-debezium-connector-0.1.9`
  (**5.3 MB**, jar 13개). 판정이 부정이므로 **삭제 대상**이다. 지운 뒤에는 Connect 워커
  재기동이 필요하다(플러그인 목록은 기동 시 스캔).
- SingleStore DB `exp_s2_src`, 테이블 `t_basic`(id 2~5, ALTER로 `memo` 컬럼 추가된 상태).
  타깃 검증에 쓰는 `cdc_tgt` DB와는 별개라 그대로 둬도 무해하다.
- `enable_observe_queries=ON` — 실험 전부터 켜져 있던 설정. 되돌리지 않았다.
