package io.deltazium.backend.recovery;

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

import io.deltazium.backend.connect.ConnectorDeployService;
import io.deltazium.backend.connect.ConnectorNames;
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
 * |                          | 다른 이종 조합은 체크섬 비교가 성립하지 않아 행 수만 비교
 * --------------------------------------------------
 */
@Service
public class RecoveryService {

    /** @param fromTimeMs 복구 진입 시각 (epoch millis) */
    public record RecoveryRun(long id, String table, long fromTimeMs, String status,
                              long published, long skipped, String logPath,
                              LocalDateTime startedAt, boolean autoResume) {
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
        List<String> command = buildCommand(table, prefix, fromTimeMs, keyColumns, recoveryTopic, logPath);

        RecoveryRun run = new RecoveryRun(id, table.qualified(), fromTimeMs, "RUNNING",
                0, 0, logPath, LocalDateTime.now(), autoResume);
        runs.put(id, run);
        events.record(table.schemaName(), table.tableName(), "RECOVERY_STARTED", "WARN",
                Instant.ofEpochMilli(fromTimeMs) + "부터 재발행 시작" + (autoResume ? " (완료 후 자동 재개)" : ""), null);
        launchProcess(id, command, logPath, table, prefix, autoResume);
        return run;
    }

    List<String> buildCommand(RegisteredTable table, String topicPrefix, long fromTimeMs,
                              List<String> keyColumns, String recoveryTopic, String logPath) {
        List<String> cmd = new ArrayList<>();
        cmd.add(launcher);
        iceberg.catalogProperties().forEach((k, v) -> cmd.add("catalog." + k + "=" + v));
        cmd.add("table=" + changelog.changelogTableName(topicPrefix, table.schemaName(), table.tableName()));
        cmd.add("from-ts-ms=" + fromTimeMs);
        cmd.add("key-columns=" + String.join(",", keyColumns));
        cmd.add("bootstrap=" + kafkaBootstrap);
        cmd.add("topic=" + recoveryTopic);
        return cmd;
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
        // 이전 복구에서 pause된 상태로 남아 있을 수 있다 — 항상 깨워서 시작
        deploy.resumeConnector(connectorName);
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
     */
    private void awaitApplyThenPauseSink(long id, RegisteredTable table, String topicPrefix, long published,
                                         boolean autoResume) {
        String connector = ConnectorNames.recoverySink(topicPrefix, table.suffix());
        String group = ConnectorNames.consumerGroup(connector);
        String topic = ConnectorNames.recoveryTopic(topicPrefix, table.suffix());
        try {
            if (published > 0) {
                long deadline = System.currentTimeMillis() + 30 * 60_000L;
                while (System.currentTimeMillis() < deadline) {
                    if (metrics.groupLag(group, topic) == 0) {
                        break;
                    }
                    Thread.sleep(5_000);
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

    private void update(long id, String status, long published, long skipped) {
        runs.computeIfPresent(id, (k, r) -> new RecoveryRun(
                r.id(), r.table(), r.fromTimeMs(), status, published, skipped,
                r.logPath(), r.startedAt(), r.autoResume()));
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
