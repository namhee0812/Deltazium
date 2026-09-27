package io.deltazium.backend.recovery;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import io.deltazium.backend.connect.ConnectClient;
import io.deltazium.backend.connect.ConnectorDeployService;
import io.deltazium.backend.connect.ConnectorNames;
import io.deltazium.backend.ddl.CaptureTopicSchemaReader;
import io.deltazium.backend.ddl.SchemaFingerprint;
import io.deltazium.backend.dictionary.DictionaryRouter;
import io.deltazium.backend.dictionary.TableColumn;
import io.deltazium.backend.events.TableEventService;
import io.deltazium.backend.iceberg.ChangelogTableService;
import io.deltazium.backend.iceberg.IcebergProperties;
import io.deltazium.backend.metrics.KafkaMetricsService;
import io.deltazium.backend.registration.ColumnMapping;
import io.deltazium.backend.registration.RegisteredColumnRepository;
import io.deltazium.backend.registration.RegisteredTable;
import io.deltazium.backend.registration.RegisteredTableRepository;
import io.deltazium.backend.registry.DbConnection;
import io.deltazium.backend.registry.DbConnectionService;
import io.deltazium.backend.registry.DbType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * 파일명 : RecoveryService.java
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : 복구 트리거 (architecture.md 6절):
 * ① recovery-sink 배포(대상 테이블용, live jdbc-sink와 동일 apply 설정)
 * ② recovery-job 프로세스 기동(Iceberg scan → 재발행) — apply는 하지 않는 잡
 * ③ 상태 추적 + SRC/TGT 정합 검증(행수·체크섬, 6.4절 ⑤)
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 09. 05.       | 최남희  | 복구 진입점을 SCN에서 시각(epoch millis)으로 전환 — recovery-job은
 * |                          | ts_ms 파티션 한 칸 앞부터 스캔해 `_pos` 순서로 재생한다 (5.2·6.2절)
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ②: recovery-sink 이름·복구 토픽·changelog
 * |                          | 테이블명에 소스 topicPrefix를 반영(ConnectorNames), 딕셔너리를
 * |                          | DictionaryRouter로 교체 — 타깃 컬럼 조회가 dbType별로 정확해짐
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ③ 저장소 프로파일(MinIO/R2): buildCommand의
 * |                          | catalog-uri 등 고정 인자를 `IcebergProperties.catalogProperties()`
 * |                          | 기반 `catalog.&lt;key&gt;=&lt;value&gt;` 반복 인자로 전환 — recovery-job이
 * |                          | 어느 프로파일이든 같은 방식으로 카탈로그를 연다(3절 단일 진원지).
 * |                          | 비밀값이 프로세스 인자(ps 노출)로 전달되는 현행 방식은 유지
 * |                          | (docs/internals.md 기록)
 * --------------------------------------------------
 * 26. 09. 23.       | 최남희  | 결함 수정: 정합 검증 체크섬을 ChecksumSql(DbType별 생성기)로
 * |                          | 분리 — Oracle은 컬럼별 해시 후 행 해시(+ 필요 시 묶음)로
 * |                          | VARCHAR2 4000 한도(ORA-01489, 206컬럼 테이블에서 실측)를
 * |                          | 피하고, PostgreSQL은 md5 기반 SQL을 추가. 소스·타깃 DbType이
 * |                          | 다른 이종 조합은 체크섬 비교가 성립하지 않아 행 수만 비교.
 * |                          | (보완) 체크섬 컬럼명을 DbType 폴딩으로 — sourceColumn()의 대문자
 * |                          | 가정 때문에 PG↔PG에서 "ID" does not exist로 실패하던 것(라이브 실측)
 * --------------------------------------------------
 * 26. 09. 27.       | 최남희  | PG→PG 복구 리허설 결함 R1·R2·R3 수정
 * |                          | (docs/experiments/2026-09-27-pg2pg-recovery-rehearsal.md):
 * |                          | R1) 복구 트리거 시 캡처 토픽 마지막 레코드(폴백: schema_fields_json)
 * |                          | 에서 컬럼 논리 타입을 읽어 힌트 파일로 recovery-job에 넘긴다
 * |                          | (writeFieldSchemaHint·FieldSchemaHint) — Iceberg changelog는
 * |                          | Debezium 논리 타입명을 보존하지 못해 재조립만으로 복원 불가하다.
 * |                          | R2) 복구 sink 배포 후 재개를 ConnectClient.resumeAfterDdl로 —
 * |                          | FAILED task는 restartFailed, PAUSED는 resume(종전엔 무조건 resume
 * |                          | 만 호출해 FAILED task가 재트리거로도 회복되지 않았다).
 * |                          | R3) apply 대기 중 recovery-sink FAILED를 감지하면 즉시 run 상태를
 * |                          | FAILED(cause=causeLine)로 남기고 WARN 이벤트 기록 — 종전엔 30분
 * |                          | 타임아웃까지 실행 상태가 DONE에 고착돼 실패를 알 수 없었다.
 * --------------------------------------------------
 */
@Service
public class RecoveryService {

    /**
     * @param fromTimeMs 복구 진입 시각 (epoch millis)
     * @param cause      status가 FAILED일 때만 채워지는 원인 한 줄(recovery-sink의 causeLine,
     *                   결함 R3) — 그 외 상태에서는 null
     */
    public record RecoveryRun(long id, String table, long fromTimeMs, String status,
                              long published, long skipped, String logPath,
                              LocalDateTime startedAt, boolean autoResume, String cause) {
    }

    /**
     * @param sourceChecksum checksumSupported가 false면 null(이종 DB 조합 — 6.4절 ⑤, 체크섬 비교 불가)
     * @param targetChecksum checksumSupported가 false면 null(위와 동일)
     * @param note           checksumSupported가 false일 때만 채워지는 사유 한 줄
     */
    public record VerifyResult(long sourceCount, long targetCount,
                               Long sourceChecksum, Long targetChecksum, boolean match,
                               boolean checksumSupported, String note) {
    }

    private static final Pattern RESULT_LINE =
            Pattern.compile("RECOVERY_RESULT published=(\\d+) skipped=(\\d+)");

    private final RegisteredTableRepository registrations;
    private final RegisteredColumnRepository columns;
    private final DbConnectionService connections;
    private final DictionaryRouter dictionaryRouter;
    private final ConnectorDeployService deploy;
    private final ConnectClient connect;
    private final ChangelogTableService changelog;
    private final IcebergProperties iceberg;
    private final TableEventService events;
    private final KafkaMetricsService metrics;
    private final String kafkaBootstrap;
    private final String launcher;
    private final String logDir;

    private final Map<Long, RecoveryRun> runs = new ConcurrentHashMap<>();
    private final AtomicLong runSeq = new AtomicLong();

    public RecoveryService(RegisteredTableRepository registrations,
                           RegisteredColumnRepository columns,
                           DbConnectionService connections,
                           DictionaryRouter dictionaryRouter,
                           ConnectorDeployService deploy,
                           ConnectClient connect,
                           ChangelogTableService changelog,
                           IcebergProperties iceberg,
                           TableEventService events,
                           KafkaMetricsService metrics,
                           @Value("${deltazium.kafka.bootstrap}") String kafkaBootstrap,
                           @Value("${deltazium.recovery.launcher}") String launcher,
                           @Value("${deltazium.recovery.log-dir}") String logDir) {
        this.registrations = registrations;
        this.columns = columns;
        this.connections = connections;
        this.dictionaryRouter = dictionaryRouter;
        this.deploy = deploy;
        this.connect = connect;
        this.changelog = changelog;
        this.iceberg = iceberg;
        this.events = events;
        this.metrics = metrics;
        this.kafkaBootstrap = kafkaBootstrap;
        this.launcher = launcher;
        this.logDir = logDir;
    }

    public List<RecoveryRun> list() {
        return runs.values().stream()
                .sorted((a, b) -> Long.compare(b.id(), a.id())).toList();
    }

    public RecoveryRun trigger(long registeredTableId, long fromTimeMs) {
        return trigger(registeredTableId, fromTimeMs, false);
    }

    /**
     * @param fromTimeMs 복구 진입 시각 (epoch millis) — recovery-job이 ts_ms 파티션 한 칸
     *                   앞부터 스캔해 `_pos` 순서로 재생한다(5.2·6.2절). 정밀 절단은 하지 않는다 —
     *                   경계 중복은 PK upsert 멱등으로 흡수.
     * @param autoResume true면 복구 apply 완료 감지 후 해당 테이블 jdbc-sink를 자동 재개 (go-live)
     */
    public RecoveryRun trigger(long registeredTableId, long fromTimeMs, boolean autoResume) {
        RegisteredTable table = find(registeredTableId);
        DbConnection source = connections.get(table.sourceConnectionId());
        DbConnection target = connections.get(table.targetConnectionId());
        String prefix = source.topicPrefix();

        // key 컬럼: 타깃 테이블 PK (apply 대상 기준 — 소스가 죽어 있어도 복구는 가능해야 한다)
        List<String> keyColumns = dictionaryRouter.forConnection(target)
                .listColumns(target, table.targetSchema(), table.targetTable()).stream()
                .filter(TableColumn::pk).map(TableColumn::name).toList();
        if (keyColumns.isEmpty()) {
            throw new IllegalArgumentException(
                    "타깃 테이블에 PK가 없다 — 복구 apply 불가: " + table.targetQualified());
        }

        String recoveryTopic = ConnectorNames.recoveryTopic(prefix, table.suffix());
        deployRecoverySink(table, target, prefix, recoveryTopic);

        long id = runSeq.incrementAndGet();
        String logPath = logDir + "/recovery-" + table.suffix() + "-" + id + ".log";
        String hintPath = logDir + "/recovery-" + table.suffix() + "-" + id + "-field-schema.json";
        String fieldSchemaFile = writeFieldSchemaHint(table, prefix, hintPath);
        List<String> command = buildCommand(table, prefix, fromTimeMs, keyColumns, recoveryTopic, logPath,
                fieldSchemaFile);

        RecoveryRun run = new RecoveryRun(id, table.qualified(), fromTimeMs, "RUNNING",
                0, 0, logPath, LocalDateTime.now(), autoResume, null);
        runs.put(id, run);
        events.record(table.schemaName(), table.tableName(), "RECOVERY_STARTED", "WARN",
                Instant.ofEpochMilli(fromTimeMs) + "부터 재발행 시작" + (autoResume ? " (완료 후 자동 재개)" : ""), null);
        launchProcess(id, command, logPath, table, prefix, autoResume);
        return run;
    }

    List<String> buildCommand(RegisteredTable table, String topicPrefix, long fromTimeMs,
                              List<String> keyColumns, String recoveryTopic, String logPath) {
        return buildCommand(table, topicPrefix, fromTimeMs, keyColumns, recoveryTopic, logPath, null);
    }

    /** @param fieldSchemaFile 결함 R1 — 컬럼 논리 타입 힌트 파일 경로(writeFieldSchemaHint가
     *                         만든다). 힌트를 하나도 못 구했으면 null(인자 자체를 안 넘김 —
     *                         recovery-job은 힌트 없이 기존 Iceberg 타입 기반 동작으로 진행). */
    List<String> buildCommand(RegisteredTable table, String topicPrefix, long fromTimeMs,
                              List<String> keyColumns, String recoveryTopic, String logPath,
                              String fieldSchemaFile) {
        List<String> cmd = new ArrayList<>();
        cmd.add(launcher);
        iceberg.catalogProperties().forEach((k, v) -> cmd.add("catalog." + k + "=" + v));
        cmd.add("table=" + changelog.changelogTableName(topicPrefix, table.schemaName(), table.tableName()));
        cmd.add("from-ts-ms=" + fromTimeMs);
        cmd.add("key-columns=" + String.join(",", keyColumns));
        cmd.add("bootstrap=" + kafkaBootstrap);
        cmd.add("topic=" + recoveryTopic);
        if (fieldSchemaFile != null) {
            cmd.add("field-schema-file=" + fieldSchemaFile);
        }
        return cmd;
    }

    /**
     * 결함 R1(docs/experiments/2026-09-27-pg2pg-recovery-rehearsal.md) — changelog(Iceberg)는
     * Debezium 전용 논리 타입명(io.debezium.time.* 등)을 보존하지 못해(docs/experiments/
     * 2026-07-24-iceberg-sink-schema.md) recovery-job의 재조립만으로는 복원할 수 없다.
     * changelog는 소스 중립(5.1절 불변식 2)이라 애초에 그 이름을 담을 자리가 없으므로,
     * changelog 대신 **트리거 시점**의 캡처 토픽에서 별도로 얻는다 — SchemaFingerprintPoller가
     * DDL 지문 비교에 쓰는 것과 같은 읽기(CaptureTopicSchemaReader, assign 기반·트래픽 재구독
     * 없음)를 재사용한다. 캡처 토픽에서 못 얻으면(토픽 비었음 등) 등록 시점 스냅샷
     * (registered_tables.schema_fields_json)으로 폴백하고, 그것도 없으면 힌트 없이 진행하며
     * WARN 이벤트를 남긴다(조용히 묻히지 않게).
     * @return 힌트 파일 경로, 힌트를 하나도 못 구했으면 null
     */
    private String writeFieldSchemaHint(RegisteredTable table, String prefix, String hintPath) {
        List<SchemaFingerprint.FieldDesc> fields = readLiveFieldSchema(prefix, table);
        if (fields.isEmpty()) {
            fields = SchemaFingerprint.fromJson(table.schemaFieldsJson());
        }
        if (fields.isEmpty()) {
            events.record(table.schemaName(), table.tableName(), "RECOVERY_STARTED", "WARN",
                    "논리 타입 힌트 없음 — 타깃 타입 불일치 가능(예: PostgreSQL timestamptz)", null);
            return null;
        }
        try {
            Files.writeString(Path.of(hintPath), FieldSchemaHint.toJson(fields));
            return hintPath;
        } catch (IOException e) {
            events.record(table.schemaName(), table.tableName(), "RECOVERY_STARTED", "WARN",
                    "논리 타입 힌트 파일 기록 실패 — 힌트 없이 진행: " + e.getMessage(), null);
            return null;
        }
    }

    /** 캡처 토픽(&lt;prefix&gt;.&lt;schema&gt;.&lt;table&gt;) 마지막 레코드의 after struct
     * 필드 목록 — SchemaFingerprintPoller와 같은 1회성 assign 소비(consumer group 없음). 실패
     * 시(토픽 없음·타임아웃 등) 예외를 삼키고 빈 목록으로 — 힌트는 있으면 좋은 보조 정보일 뿐
     * 복구 자체를 막을 이유가 아니다. */
    private List<SchemaFingerprint.FieldDesc> readLiveFieldSchema(String prefix, RegisteredTable table) {
        String topic = ConnectorNames.captureTopic(prefix, table.schemaName(), table.tableName());
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // group.id 없음 — 복구 트리거 1회에 쓰고 버리는 단발 consumer(SchemaFingerprintPoller의
        // 상주 consumer와 달리 여기서 만들고 닫는다)
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(props)) {
            JsonNode value = CaptureTopicSchemaReader.lastEnvelopeValue(c, topic).orElse(null);
            return value == null ? List.of() : SchemaFingerprint.afterFields(value);
        } catch (Exception e) {
            return List.of();
        }
    }

    private void deployRecoverySink(RegisteredTable table, DbConnection target, String topicPrefix, String topic) {
        Map<String, String> vars = new HashMap<>();
        String connectorName = ConnectorNames.recoverySink(topicPrefix, table.suffix());
        vars.put("connector_name", connectorName);
        vars.put("recovery_topics", topic);
        vars.put("target_jdbc_url", target.jdbcUrl());
        vars.put("target_user", target.username());
        vars.put("target_password", target.password());
        vars.put("collection_name", table.targetQualified());
        // live jdbc-sink와 동일한 컬럼 필터 — apply 시맨틱 단일 경로 유지
        List<ColumnMapping> mappings = columns.findByTable(table.id());
        deploy.deploy("recovery-sink", vars,
                io.deltazium.backend.registration.RegistrationService.fieldIncludeConfig(mappings));
        // 이전 복구에서 pause된 상태로 남아 있을 수 있다 — 항상 깨워서 시작.
        // 결함 R2: 이전 실행이 apply 실패로 task FAILED인 채 남아 있을 수도 있다 — 같은 config를
        // PUT해도 Connect는 "connector-only config update"로 커넥터만 재시작하고 task는 FAILED
        // 그대로다(RUNNING 커넥터에 resume은 no-op). resumeAfterDdl이 상태를 보고 FAILED면
        // restartFailed(KIP-745), PAUSED면 resume을 골라 재트리거로도 회복되게 한다.
        connect.resumeAfterDdl(connectorName);
    }

    private void launchProcess(long id, List<String> command, String logPath, RegisteredTable table,
                               String topicPrefix, boolean autoResume) {
        Thread watcher = new Thread(() -> {
            try {
                ProcessBuilder pb = new ProcessBuilder(command);
                pb.redirectErrorStream(true);
                pb.redirectOutput(new java.io.File(logPath));
                Process process = pb.start();
                int exit = process.waitFor();
                long published = 0;
                long skipped = 0;
                for (String line : java.nio.file.Files.readAllLines(java.nio.file.Path.of(logPath))) {
                    Matcher m = RESULT_LINE.matcher(line);
                    if (m.find()) {
                        published = Long.parseLong(m.group(1));
                        skipped = Long.parseLong(m.group(2));
                    }
                }
                boolean ok = exit == 0;
                update(id, ok ? "DONE" : "FAILED", published, skipped);
                events.record(table.schemaName(), table.tableName(),
                        ok ? "RECOVERY_DONE" : "RECOVERY_FAILED", ok ? "INFO" : "ERROR",
                        ok ? "재발행 완료 — " + published + "건 (건너뜀 " + skipped + ")"
                           : "재발행 실패 — 로그: " + logPath, null);
                if (ok) {
                    awaitApplyThenPauseSink(id, table, topicPrefix, published, autoResume);
                }
            } catch (Exception e) {
                update(id, "FAILED", 0, 0);
                events.record(table.schemaName(), table.tableName(), "RECOVERY_FAILED", "ERROR",
                        "재발행 프로세스 실패: " + e.getMessage(), null);
            }
        }, "recovery-watch-" + id);
        watcher.setDaemon(true);
        watcher.start();
    }

    /**
     * recovery-sink의 apply 완료(lag 소진)를 기다렸다가 정지 — "평시 정지" 원칙(4절).
     * 발행 0건이면 바로 정지. 30분 내 소진 안 되면 정지하지 않고 경고만 남긴다.
     * autoResume이면 완료 후 해당 테이블 jdbc-sink를 재개해 go-live까지 마친다.
     *
     * <p>결함 R3: 대기 중 recovery-sink 커넥터/태스크가 FAILED로 전이하면 lag는 더 줄지
     * 않는데도(멈춰 있으므로) 종전엔 30분 타임아웃까지 실행 상태가 "DONE"에 그대로 남아
     * apply 실패를 실행 이력만으로는 알 수 없었다(실측: 리허설 orders). 5초 주기 대기마다
     * FAILED 여부를 함께 확인해 감지 즉시 run을 FAILED로 남기고(cause=causeLine), DONE·
     * go-live로 넘어가지 않는다.
     */
    private void awaitApplyThenPauseSink(long id, RegisteredTable table, String topicPrefix, long published,
                                         boolean autoResume) {
        String connector = ConnectorNames.recoverySink(topicPrefix, table.suffix());
        String group = ConnectorNames.consumerGroup(connector);
        String topic = ConnectorNames.recoveryTopic(topicPrefix, table.suffix());
        try {
            if (published > 0) {
                long deadline = System.currentTimeMillis() + 30 * 60_000L;
                String cause = failureCause(connector);
                while (cause == null && System.currentTimeMillis() < deadline) {
                    if (metrics.groupLag(group, topic) == 0) {
                        break;
                    }
                    Thread.sleep(5_000);
                    cause = failureCause(connector);
                }
                if (cause != null) {
                    update(id, "FAILED", published, 0, cause);
                    events.record(table.schemaName(), table.tableName(), "RECOVERY_DONE", "WARN",
                            "recovery-sink apply 실패 — " + cause, null);
                    return;
                }
                if (metrics.groupLag(group, topic) > 0) {
                    events.record(table.schemaName(), table.tableName(), "RECOVERY_DONE", "WARN",
                            "apply가 30분 내 완료되지 않아 recovery-sink를 정지하지 않음 — lag 확인 필요"
                            + (autoResume ? " (자동 재개도 보류됨 — 수동 재개 필요)" : ""), null);
                    return;
                }
            }
            deploy.pauseConnector(connector);
            update(id, "APPLIED", published, 0);
            events.info(table.schemaName(), table.tableName(), "RECOVERY_DONE",
                    "apply 완료 확인 — " + connector + " 정지 (평시 정지 원칙)");
            if (autoResume) {
                deploy.resumeConnector(ConnectorNames.jdbcSink(topicPrefix, table.suffix()));
                update(id, "LIVE", published, 0);
                events.info(table.schemaName(), table.tableName(), "RESUMED",
                        "복구 완료 후 자동 재개 (go-live) — 경계 중복은 PK upsert 멱등으로 흡수");
            }
        } catch (Exception e) {
            events.record(table.schemaName(), table.tableName(), "RECOVERY_DONE", "WARN",
                    "recovery-sink 정지/재개 실패: " + e.getMessage(), null);
        }
    }

    /**
     * connector/task 상태에서 FAILED 원인 한 줄 — ui/src/lib/connect.ts의 effectiveState·
     * causeLine과 같은 판정을 백엔드에서 재구현(결함 R3, 실행 이력에 원인을 남기기 위해 필요).
     * UI·SystemWarningService·ConnectorHealthWatcher·ConnectClient.resumeAfterDdl에 이미 각자
     * 목적으로 중복 정의된 effectiveState 판정의 다섯 번째 자리다(docs/internals.md "DDL 반영
     * 정책" 절 — 목적이 다 달라 공유 유틸리티로 뽑지 않는 이 프로젝트의 기존 방침을 따른다).
     * @return FAILED가 아니면 null
     */
    private String failureCause(String connectorName) {
        JsonNode status = connect.status(connectorName);
        boolean connectorFailed = "FAILED".equals(status.path("connector").path("state").asText());
        boolean taskFailed = false;
        String trace = null;
        for (JsonNode task : status.path("tasks")) {
            if ("FAILED".equals(task.path("state").asText())) {
                taskFailed = true;
                String t = task.path("trace").asText(null);
                if (t != null) {
                    trace = t;
                }
            }
        }
        if (!connectorFailed && !taskFailed) {
            return null;
        }
        if (trace == null) {
            return "상태 FAILED (trace 없음)";
        }
        String lastCause = null;
        for (String line : trace.split("\n")) {
            if (line.startsWith("Caused by: ")) {
                lastCause = line.substring("Caused by: ".length());
            }
        }
        return (lastCause != null ? lastCause : trace.split("\n")[0]).strip();
    }

    private void update(long id, String status, long published, long skipped) {
        update(id, status, published, skipped, null);
    }

    private void update(long id, String status, long published, long skipped, String cause) {
        runs.computeIfPresent(id, (k, r) -> new RecoveryRun(
                r.id(), r.table(), r.fromTimeMs(), status, published, skipped,
                r.logPath(), r.startedAt(), r.autoResume(), cause));
    }

    /**
     * 6.4절 ⑤ 정합 검증 — 행 수 + 체크섬(활성·동일명 컬럼 기준).
     * 체크섬은 소스·타깃 DbType이 같은 Oracle↔Oracle·PostgreSQL↔PostgreSQL 조합에서만
     * 비교 가능하다(ChecksumSql — Oracle은 ORA_HASH, PostgreSQL은 md5 기반이라 값 자체가
     * 다른 알고리즘이라 이종 조합은 비교가 성립하지 않는다, 2026-09-23 결정,
     * docs/internals.md "정합 검증 체크섬" 절). 이종 DB 조합(Oracle↔PostgreSQL)은 행 수만
     * 비교하고 checksumSupported=false로 사유를 note에 남긴다.
     */
    public VerifyResult verify(long registeredTableId) {
        RegisteredTable table = find(registeredTableId);
        DbConnection source = connections.get(table.sourceConnectionId());
        DbConnection target = connections.get(table.targetConnectionId());
        DbType sourceType = DbType.find(source.dbType())
                .orElseThrow(() -> new IllegalArgumentException("알 수 없는 소스 DB 종류: " + source.dbType()));
        DbType targetType = DbType.find(target.dbType())
                .orElseThrow(() -> new IllegalArgumentException("알 수 없는 타깃 DB 종류: " + target.dbType()));
        boolean checksumSupported = sourceType == targetType
                && (sourceType == DbType.ORACLE || sourceType == DbType.POSTGRESQL);

        List<String> cols = List.of();
        if (checksumSupported) {
            cols = columns.findByTable(table.id()).stream()
                    .filter(m -> m.enabled() && m.isIdentity())
                    .map(m -> m.sourceColumn().orElseThrow())
                    .collect(Collectors.toList());
            if (cols.isEmpty()) {
                // 매핑 메타데이터가 없는 구버전 등록 — 소스 딕셔너리 전 컬럼
                cols = dictionaryRouter.forConnection(source)
                        .listColumns(source, table.schemaName(), table.tableName())
                        .stream().map(TableColumn::name).collect(Collectors.toList());
            }
            // ColumnMapping.sourceColumn()은 Oracle 가정으로 대문자를 돌려준다 — 체크섬 SQL은 따옴표
            // 식별자라 실제 카탈로그 대소문자여야 하므로 DbType 폴딩(Oracle 대문자·PG 소문자, 8절)으로
            // 맞춘다. 체크섬은 동종 조합에서만 계산하니 소스·타깃에 같은 폴딩을 써도 된다.
            cols = cols.stream().map(sourceType::foldIdentifier).collect(Collectors.toList());
        }

        CountChecksum src = countAndChecksum(source, sourceType, table.schemaName(), table.tableName(),
                cols, checksumSupported);
        CountChecksum tgt = countAndChecksum(target, targetType, table.targetSchema(), table.targetTable(),
                cols, checksumSupported);
        boolean match = src.count() == tgt.count()
                && (!checksumSupported || java.util.Objects.equals(src.checksum(), tgt.checksum()));
        String note = checksumSupported ? null : "이종 DB는 행수만 비교(체크섬 미지원)";
        return new VerifyResult(src.count(), tgt.count(), src.checksum(), tgt.checksum(),
                match, checksumSupported, note);
    }

    private record CountChecksum(long count, Long checksum) {
    }

    private CountChecksum countAndChecksum(DbConnection conn, DbType type, String schema, String table,
                                           List<String> cols, boolean checksumSupported) {
        String sql = !checksumSupported ? ChecksumSql.rowCountOnly(type, schema, table)
                : type == DbType.POSTGRESQL ? ChecksumSql.forPostgres(schema, table, cols)
                : ChecksumSql.forOracle(schema, table, cols);
        Properties props = new Properties();
        props.setProperty("user", conn.username());
        props.setProperty("password", conn.password());
        if (type == DbType.POSTGRESQL) {
            props.setProperty("loginTimeout", "5");
            props.setProperty("connectTimeout", "5");
        } else {
            props.setProperty("oracle.net.CONNECT_TIMEOUT", "5000");
        }
        try (Connection db = DriverManager.getConnection(conn.jdbcUrl(), props);
             Statement st = db.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            long count = rs.getLong(1);
            Long checksum = checksumSupported ? rs.getLong(2) : null;
            return new CountChecksum(count, checksum);
        } catch (SQLException e) {
            throw new IllegalStateException("정합 검증 쿼리 실패(" + schema + "." + table + "): "
                    + (e.getMessage() == null ? e.toString() : e.getMessage().strip()));
        }
    }

    private RegisteredTable find(long registeredTableId) {
        return registrations.findAll().stream()
                .filter(t -> t.id() == registeredTableId).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("등록 테이블 없음: id=" + registeredTableId));
    }
}
