package io.deltazium.backend.recovery;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
import io.deltazium.backend.dictionary.OracleDictionaryService;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * 복구 트리거 (architecture.md 6절):
 * ① recovery-sink 배포(대상 테이블용, live jdbc-sink와 동일 apply 설정)
 * ② recovery-job 프로세스 기동(Iceberg scan → 재발행) — apply는 하지 않는 잡
 * ③ 상태 추적 + SRC/TGT 정합 검증(행수·체크섬, 6.4절 ⑤)
 */
@Service
public class RecoveryService {

    public record RecoveryRun(long id, String table, long fromScn, String status,
                              long published, long skipped, String logPath,
                              LocalDateTime startedAt) {
    }

    public record VerifyResult(long sourceCount, long targetCount,
                               long sourceChecksum, long targetChecksum, boolean match) {
    }

    private static final Pattern RESULT_LINE =
            Pattern.compile("RECOVERY_RESULT published=(\\d+) skipped=(\\d+)");

    private final RegisteredTableRepository registrations;
    private final RegisteredColumnRepository columns;
    private final DbConnectionService connections;
    private final OracleDictionaryService dictionary;
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
                           OracleDictionaryService dictionary,
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
        this.dictionary = dictionary;
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

    public RecoveryRun trigger(long registeredTableId, long fromScn) {
        RegisteredTable table = find(registeredTableId);
        DbConnection target = connections.get(table.targetConnectionId());

        // key 컬럼: 타깃 테이블 PK (apply 대상 기준 — 소스가 죽어 있어도 복구는 가능해야 한다)
        List<String> keyColumns = dictionary
                .listColumns(target, table.targetSchema(), table.targetTable()).stream()
                .filter(TableColumn::pk).map(TableColumn::name).toList();
        if (keyColumns.isEmpty()) {
            throw new IllegalArgumentException(
                    "타깃 테이블에 PK가 없다 — 복구 apply 불가: " + table.targetQualified());
        }

        String recoveryTopic = "dz-recovery." + table.suffix();
        deployRecoverySink(table, target, recoveryTopic);

        long id = runSeq.incrementAndGet();
        String logPath = logDir + "/recovery-" + table.suffix() + "-" + id + ".log";
        List<String> command = buildCommand(table, fromScn, keyColumns, recoveryTopic, logPath);

        RecoveryRun run = new RecoveryRun(id, table.qualified(), fromScn, "RUNNING",
                0, 0, logPath, LocalDateTime.now());
        runs.put(id, run);
        events.record(table.schemaName(), table.tableName(), "RECOVERY_STARTED", "WARN",
                "SCN " + fromScn + "부터 재발행 시작", null);
        launchProcess(id, command, logPath, table);
        return run;
    }

    List<String> buildCommand(RegisteredTable table, long fromScn,
                              List<String> keyColumns, String recoveryTopic, String logPath) {
        List<String> cmd = new ArrayList<>();
        cmd.add(launcher);
        cmd.add("catalog-uri=" + iceberg.catalogUri());
        cmd.add("catalog-user=" + iceberg.catalogUser());
        cmd.add("catalog-password=" + iceberg.catalogPassword());
        cmd.add("warehouse=" + iceberg.warehouse());
        cmd.add("s3-endpoint=" + iceberg.s3Endpoint());
        cmd.add("s3-access-key=" + iceberg.s3AccessKey());
        cmd.add("s3-secret-key=" + iceberg.s3SecretKey());
        cmd.add("table=" + changelog.changelogTableName(table.schemaName(), table.tableName()));
        cmd.add("from-scn=" + fromScn);
        cmd.add("key-columns=" + String.join(",", keyColumns));
        cmd.add("bootstrap=" + kafkaBootstrap);
        cmd.add("topic=" + recoveryTopic);
        return cmd;
    }

    private void deployRecoverySink(RegisteredTable table, DbConnection target, String topic) {
        Map<String, String> vars = new HashMap<>();
        vars.put("connector_name", "dz-recovery-sink-" + table.suffix());
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
        deploy.resumeConnector("dz-recovery-sink-" + table.suffix());
    }

    private void launchProcess(long id, List<String> command, String logPath, RegisteredTable table) {
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
                    awaitApplyThenPauseSink(id, table, published);
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
     */
    private void awaitApplyThenPauseSink(long id, RegisteredTable table, long published) {
        String group = "connect-dz-recovery-sink-" + table.suffix();
        String topic = "dz-recovery." + table.suffix();
        String connector = "dz-recovery-sink-" + table.suffix();
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
                            "apply가 30분 내 완료되지 않아 recovery-sink를 정지하지 않음 — lag 확인 필요", null);
                    return;
                }
            }
            deploy.pauseConnector(connector);
            update(id, "APPLIED", published, 0);
            events.info(table.schemaName(), table.tableName(), "RECOVERY_DONE",
                    "apply 완료 확인 — " + connector + " 정지 (평시 정지 원칙)");
        } catch (Exception e) {
            events.record(table.schemaName(), table.tableName(), "RECOVERY_DONE", "WARN",
                    "recovery-sink 정지 실패: " + e.getMessage(), null);
        }
    }

    private void update(long id, String status, long published, long skipped) {
        runs.computeIfPresent(id, (k, r) -> new RecoveryRun(
                r.id(), r.table(), r.fromScn(), status, published, skipped,
                r.logPath(), r.startedAt()));
    }

    /** 6.4절 ⑤ 정합 검증 — 행 수 + ORA_HASH 체크섬 (활성·동일명 컬럼 기준). */
    public VerifyResult verify(long registeredTableId) {
        RegisteredTable table = find(registeredTableId);
        DbConnection source = connections.get(table.sourceConnectionId());
        DbConnection target = connections.get(table.targetConnectionId());

        List<String> cols = columns.findByTable(table.id()).stream()
                .filter(m -> m.enabled() && m.isIdentity())
                .map(m -> m.sourceColumn().orElseThrow())
                .collect(Collectors.toList());
        if (cols.isEmpty()) {
            // 매핑 메타데이터가 없는 구버전 등록 — 소스 딕셔너리 전 컬럼
            cols = dictionary.listColumns(source, table.schemaName(), table.tableName())
                    .stream().map(TableColumn::name).collect(Collectors.toList());
        }

        long[] src = countAndChecksum(source, table.qualified(), cols);
        long[] tgt = countAndChecksum(target, table.targetQualified(), cols);
        return new VerifyResult(src[0], tgt[0], src[1], tgt[1],
                src[0] == tgt[0] && src[1] == tgt[1]);
    }

    static String checksumSql(String qualifiedTable, List<String> cols) {
        String concat = cols.stream()
                .map(c -> "NVL(TO_CHAR(\"" + c + "\"), '~null~')")
                .collect(Collectors.joining(" || '|' || "));
        return "SELECT COUNT(*), NVL(SUM(ORA_HASH(" + concat + ")), 0) FROM " + qualifiedTable;
    }

    private long[] countAndChecksum(DbConnection conn, String qualifiedTable, List<String> cols) {
        Properties props = new Properties();
        props.setProperty("user", conn.username());
        props.setProperty("password", conn.password());
        props.setProperty("oracle.net.CONNECT_TIMEOUT", "5000");
        try (Connection db = DriverManager.getConnection(conn.jdbcUrl(), props);
             Statement st = db.createStatement();
             ResultSet rs = st.executeQuery(checksumSql(qualifiedTable, cols))) {
            rs.next();
            return new long[] {rs.getLong(1), rs.getLong(2)};
        } catch (SQLException e) {
            throw new IllegalStateException("정합 검증 쿼리 실패(" + qualifiedTable + "): "
                    + (e.getMessage() == null ? e.toString() : e.getMessage().strip()));
        }
    }

    private RegisteredTable find(long registeredTableId) {
        return registrations.findAll().stream()
                .filter(t -> t.id() == registeredTableId).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("등록 테이블 없음: id=" + registeredTableId));
    }
}
