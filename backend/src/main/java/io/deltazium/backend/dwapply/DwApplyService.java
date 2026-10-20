package io.deltazium.backend.dwapply;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.deltazium.backend.connect.ConnectorNames;
import io.deltazium.backend.dictionary.DictionaryRouter;
import io.deltazium.backend.dictionary.TableColumn;
import io.deltazium.backend.registration.ColumnMapping;
import io.deltazium.backend.registration.RegisteredColumnRepository;
import io.deltazium.backend.registration.RegisteredTable;
import io.deltazium.backend.registration.RegisteredTableRepository;
import io.deltazium.backend.registry.DatabricksSupport;
import io.deltazium.backend.registry.DbConnection;
import io.deltazium.backend.registry.DbConnectionService;
import io.deltazium.backend.registry.DbType;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

/**
 * 파일명 : DwApplyService.java
 * 작성일자 : 26. 10. 19.
 * 작성자 : 최남희
 * 설명 : DW apply 워커(dw-apply 모듈) 프로세스 관리 — DW 타깃(DbType.dwTarget) 커넥션당 1 프로세스
 * (architecture.md 6.5절). reconcile(targetId)가 그 타깃의 등록 테이블·컬럼 매핑으로 설정 JSON을
 * 쓰고 워커를 (재)기동한다 — 테이블이 0개면 종료만. 토큰은 설정 파일·argv가 아니라 프로세스
 * 환경변수 DZ_DATABRICKS_TOKEN으로만 넘긴다(DatabricksSupport.token: 연결의 password, 없으면
 * backend 환경변수). 감시: 비정상 종료 시 30초 후 재기동(연속 실패 시 지수 백오프, 최대 5분);
 * 종료 코드 2(설정 오류·토큰 없음·드라이버 로드 실패)는 재기동해도 소용없으므로 자동 재기동하지
 * 않고 오류로 남긴다(reconcile·수동 restart로만 재시도). 상태는 메모리 + 워커 statusFile.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@Service
public class DwApplyService {

    private static final Logger log = LoggerFactory.getLogger(DwApplyService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** 워커가 이 시간 이상 살아 있었으면 정상 가동으로 보고 재기동 백오프를 초기화한다. */
    private static final long STABLE_MS = 120_000;
    private static final long BACKOFF_INIT_MS = 30_000;
    private static final long BACKOFF_MAX_MS = 300_000;

    /** GET /api/dw-apply 응답 한 건. error는 기동 실패·비정상 종료 사유(없으면 null). */
    public record WorkerInfo(long targetId, String targetName, boolean running, Long pid, String startedAt,
                             int restarts, String error, JsonNode status) {
    }

    /** 설정 JSON 한 테이블 분. columns는 [소스 컬럼, 타깃 컬럼] 쌍. */
    public record TableConfig(String topic, String schema, String table, List<String> keyColumns,
                              List<String[]> columns) {
    }

    private static final class Worker {
        final long targetId;
        Process process;
        long startedAtMs;
        int restarts;
        boolean desired;
        long nextStartAtMs;
        long backoffMs = BACKOFF_INIT_MS;
        String error;

        Worker(long targetId) {
            this.targetId = targetId;
        }
    }

    private final RegisteredTableRepository tables;
    private final RegisteredColumnRepository columns;
    private final DbConnectionService connections;
    private final DictionaryRouter dictionaryRouter;
    private final ProcessStarter starter;
    private final String launcher;
    private final Path logDir;
    private final Path runDir;
    private final long batchMs;
    private final int parallelism;
    private final String kafkaBootstrap;

    private final Map<Long, Worker> workers = new LinkedHashMap<>();
    private ScheduledExecutorService supervisor;

    public DwApplyService(RegisteredTableRepository tables,
                          RegisteredColumnRepository columns,
                          DbConnectionService connections,
                          DictionaryRouter dictionaryRouter,
                          ProcessStarter starter,
                          @Value("${deltazium.dw-apply.launcher:../dw-apply/build/install/dw-apply/bin/dw-apply}") String launcher,
                          @Value("${deltazium.dw-apply.log-dir:${user.home}/deltazium-runtime/logs}") String logDir,
                          @Value("${deltazium.dw-apply.run-dir:${user.home}/deltazium-runtime/run}") String runDir,
                          @Value("${deltazium.dw-apply.batch-ms:10000}") long batchMs,
                          @Value("${deltazium.dw-apply.parallelism:4}") int parallelism,
                          @Value("${deltazium.kafka.bootstrap:localhost:9092}") String kafkaBootstrap) {
        this.tables = tables;
        this.columns = columns;
        this.connections = connections;
        this.dictionaryRouter = dictionaryRouter;
        this.starter = starter;
        this.launcher = launcher;
        this.logDir = Path.of(logDir);
        this.runDir = Path.of(runDir);
        this.batchMs = batchMs;
        this.parallelism = parallelism;
        this.kafkaBootstrap = kafkaBootstrap;
    }

    // ── 수명주기 ─────────────────────────────────────────────

    /** backend 기동 시 DW 타깃 전부 reconcile + 감시 스레드 시작. 한 타깃의 실패가 다른 타깃을 막지 않는다. */
    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        for (DbConnection c : connections.list()) {
            if (isDw(c)) {
                try {
                    reconcile(c.id());
                } catch (Exception e) {
                    log.error("DW apply 워커 복원 실패 targetId={}: {}", c.id(), e.toString());
                }
            }
        }
        supervisor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "dw-apply-supervisor");
            t.setDaemon(true);
            return t;
        });
        supervisor.scheduleWithFixedDelay(() -> {
            try {
                superviseOnce(System.currentTimeMillis());
            } catch (Exception e) {
                log.warn("DW apply 감시 오류: {}", e.toString());
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void shutdown() {
        if (supervisor != null) {
            supervisor.shutdownNow();
        }
        synchronized (this) {
            for (Worker w : workers.values()) {
                w.desired = false;
                destroy(w);
            }
        }
    }

    // ── reconcile ────────────────────────────────────────────

    /**
     * 타깃의 등록 테이블 기준으로 설정을 다시 쓰고 워커를 재기동한다(테이블 0개면 종료만).
     * 렌더링 실패(소스 딕셔너리 조회 불가 등)는 예외로 올린다 — 호출한 등록/해제 흐름이 실패로
     * 드러나게 한다. 프로세스 기동 실패는 상태 error에 남기고 감시가 재시도한다.
     */
    public synchronized void reconcile(long targetId) {
        Worker w = workers.computeIfAbsent(targetId, Worker::new);
        w.desired = false;
        destroy(w);
        DbConnection target;
        try {
            target = connections.get(targetId);
        } catch (DbConnectionService.NotFoundException e) {
            workers.remove(targetId);
            return;
        }
        List<RegisteredTable> mine = tables.findAll().stream()
                .filter(t -> t.targetConnectionId() == targetId).toList();
        if (mine.isEmpty()) {
            workers.remove(targetId);
            deleteQuietly(configPath(targetId));
            deleteQuietly(statusPath(targetId));
            return;
        }
        List<TableConfig> configs = collect(mine);
        try {
            Files.createDirectories(runDir);
            writeAtomically(configPath(targetId),
                    renderConfig(target, configs, kafkaBootstrap, batchMs, parallelism, statusPath(targetId)));
        } catch (IOException e) {
            throw new IllegalStateException("DW apply 설정 파일 기록 실패: " + e.getMessage(), e);
        }
        w.desired = true;
        w.error = null;
        w.backoffMs = BACKOFF_INIT_MS;
        w.nextStartAtMs = 0;
        startNow(w, target);
    }

    /** 수동 재시작 — 설정은 그대로 다시 렌더링한다(reconcile과 동일). */
    public void restart(long targetId) {
        reconcile(targetId);
    }

    private List<TableConfig> collect(List<RegisteredTable> mine) {
        List<TableConfig> out = new ArrayList<>();
        for (RegisteredTable t : mine) {
            DbConnection source = connections.get(t.sourceConnectionId());
            Set<String> pkSource = new HashSet<>();
            for (TableColumn c : dictionaryRouter.forConnection(source)
                    .listColumns(source, t.schemaName(), t.tableName())) {
                if (c.pk()) {
                    pkSource.add(c.name().toLowerCase(Locale.ROOT));
                }
            }
            List<String[]> cols = new ArrayList<>();
            List<String> keys = new ArrayList<>();
            for (ColumnMapping m : columns.findByTable(t.id())) {
                if (!m.enabled() || m.sourceColumnRaw().isEmpty()) {
                    continue;
                }
                String src = m.sourceColumnRaw().get();
                cols.add(new String[] {src, m.targetColumn()});
                if (pkSource.contains(src.toLowerCase(Locale.ROOT))) {
                    keys.add(m.targetColumn());
                }
            }
            if (keys.isEmpty()) {
                throw new IllegalStateException("PK 컬럼을 컬럼 매핑에서 찾지 못함: " + t.qualified());
            }
            String prefix = source.topicPrefix();
            out.add(new TableConfig(ConnectorNames.captureTopic(prefix, t.schemaName(), t.tableName()),
                    t.targetSchema(), t.targetTable(), keys, cols));
        }
        return out;
    }

    /** 워커 설정 JSON 렌더링 — 토큰은 넣지 않는다(환경변수로만 전달). */
    public static String renderConfig(DbConnection target, List<TableConfig> tableConfigs, String bootstrap,
                                      long batchMs, int parallelism, Path statusFile) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("targetId", target.id());
        n.put("bootstrap", bootstrap);
        n.put("groupId", ConnectorNames.dwApplyGroup(target.id()));
        n.put("host", target.host());
        n.put("httpPath", target.httpPath());
        n.put("catalog", target.databaseName());
        n.put("volume", DatabricksSupport.volumePath(target));
        n.put("batchMs", batchMs);
        n.put("batchMaxBytes", 64L * 1024 * 1024);
        n.put("parallelism", parallelism);
        n.put("statusFile", statusFile.toString());
        ArrayNode arr = n.putArray("tables");
        for (TableConfig t : tableConfigs) {
            ObjectNode o = arr.addObject();
            o.putArray("topics").add(t.topic());
            o.put("schema", t.schema());
            o.put("table", t.table());
            ArrayNode keys = o.putArray("keyColumns");
            t.keyColumns().forEach(keys::add);
            ArrayNode cols = o.putArray("columns");
            for (String[] c : t.columns()) {
                cols.addObject().put("source", c[0]).put("target", c[1]);
            }
        }
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(n);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    // ── 프로세스 ─────────────────────────────────────────────

    private void startNow(Worker w, DbConnection target) {
        String token = DatabricksSupport.token(target);
        if (token == null) {
            w.error = "Databricks 토큰 없음 — 연결의 Access token 또는 backend 환경변수 "
                    + DatabricksSupport.TOKEN_ENV + " 필요";
            w.desired = false;
            log.warn("DW apply 워커 기동 불가 targetId={}: {}", w.targetId, w.error);
            return;
        }
        Path launcherPath = Path.of(launcher).toAbsolutePath().normalize();
        try {
            w.process = starter.start(
                    List.of(launcherPath.toString(), "config=" + configPath(w.targetId)),
                    Map.of(DatabricksSupport.TOKEN_ENV, token),
                    logDir.resolve("dw-apply-" + w.targetId + ".log"));
            w.startedAtMs = System.currentTimeMillis();
            log.info("DW apply 워커 기동 targetId={} pid={}", w.targetId, w.process.pid());
        } catch (IOException e) {
            w.process = null;
            w.error = "워커 기동 실패: " + e.getMessage();
            log.warn("DW apply 워커 기동 실패 targetId={}: {}", w.targetId, e.toString());
        }
    }

    /** 종료: destroy → 10초 대기 → destroyForcibly. */
    private void destroy(Worker w) {
        Process p = w.process;
        w.process = null;
        if (p == null || !p.isAlive()) {
            return;
        }
        p.destroy();
        try {
            if (!p.waitFor(10, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                p.waitFor(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            p.destroyForcibly();
        }
    }

    /**
     * 감시 1회 — 죽은 워커를 재기동한다. 종료 코드 2는 자동 재기동하지 않는다.
     * 연속 실패는 30초부터 두 배씩 최대 5분. 테스트가 시각을 주입해 직접 호출한다.
     */
    synchronized void superviseOnce(long nowMs) {
        for (Worker w : workers.values()) {
            if (!w.desired) {
                continue;
            }
            if (w.process != null && !w.process.isAlive()) {
                int exit = w.process.exitValue();
                long lived = nowMs - w.startedAtMs;
                w.process = null;
                if (exit == 2) {
                    w.desired = false;
                    w.error = "워커가 설정 오류로 종료됨(exit 2) — 로그 확인 후 재시작 필요: dw-apply-" + w.targetId + ".log";
                    log.error("DW apply 워커 exit 2 targetId={} — 자동 재기동 안 함", w.targetId);
                    continue;
                }
                if (lived >= STABLE_MS) {
                    w.backoffMs = BACKOFF_INIT_MS;
                }
                w.nextStartAtMs = nowMs + w.backoffMs;
                w.backoffMs = Math.min(w.backoffMs * 2, BACKOFF_MAX_MS); // 연속 실패면 다음엔 두 배
                w.error = "워커 비정상 종료(exit " + exit + ") — " + (w.nextStartAtMs - nowMs) / 1000 + "초 후 재기동";
                log.warn("DW apply 워커 종료 targetId={} exit={} — {}ms 후 재기동", w.targetId, exit, w.nextStartAtMs - nowMs);
            }
            if (w.process == null && w.desired && nowMs >= w.nextStartAtMs && w.nextStartAtMs > 0) {
                try {
                    DbConnection target = connections.get(w.targetId);
                    w.restarts++;
                    startNow(w, target);
                } catch (Exception e) {
                    w.error = "재기동 실패: " + e.getMessage();
                }
            }
        }
    }

    // ── 조회 ────────────────────────────────────────────────

    /** DW 타깃 연결 전체의 워커 상태 — 워커가 없는 타깃은 running=false. */
    public synchronized List<WorkerInfo> list() {
        List<WorkerInfo> out = new ArrayList<>();
        for (DbConnection c : connections.list()) {
            if (!isDw(c)) {
                continue;
            }
            Worker w = workers.get(c.id());
            boolean running = w != null && w.process != null && w.process.isAlive();
            out.add(new WorkerInfo(c.id(), c.name(), running, running ? w.process.pid() : null,
                    running ? Instant.ofEpochMilli(w.startedAtMs).toString() : null,
                    w == null ? 0 : w.restarts, w == null ? null : w.error, readStatus(c.id())));
        }
        return out;
    }

    private JsonNode readStatus(long targetId) {
        Path p = statusPath(targetId);
        try {
            return Files.exists(p) ? MAPPER.readTree(Files.readString(p)) : null;
        } catch (IOException e) {
            return null; // 쓰는 도중이거나 손상 — 다음 조회에서 읽힌다
        }
    }

    // ── 유틸 ────────────────────────────────────────────────

    private static boolean isDw(DbConnection c) {
        return DbType.find(c.dbType()).map(DbType::dwTarget).orElse(false);
    }

    private Path configPath(long targetId) {
        return runDir.resolve("dw-apply-" + targetId + ".json");
    }

    private Path statusPath(long targetId) {
        return runDir.resolve("dw-apply-" + targetId + ".status.json");
    }

    private static void writeAtomically(Path file, String content) throws IOException {
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        Files.writeString(tmp, content);
        Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private static void deleteQuietly(Path p) {
        try {
            Files.deleteIfExists(p);
        } catch (IOException ignore) {
            // 남아도 무해
        }
    }
}
