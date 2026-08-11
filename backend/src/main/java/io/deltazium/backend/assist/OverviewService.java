package io.deltazium.backend.assist;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import io.deltazium.backend.connect.ConnectClient;
import io.deltazium.backend.ddl.DdlEvent;
import io.deltazium.backend.ddl.DdlEventService;
import io.deltazium.backend.events.TableEvent;
import io.deltazium.backend.events.TableEventService;
import io.deltazium.backend.metrics.KafkaMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 파일명 : OverviewService.java
 * 작성일자 : 26. 08. 11.
 * 작성자 : 최남희
 * 설명 : AI 진단 어시스턴트의 진입점 — "왜 CDC가 멈췄어?" 같은 대상 미지정 질문에
 * 한 번의 호출로 어디가 이상한지 특정하게 한다(읽기 전용, 기존 서비스 재사용만).
 *
 * <p>
 * 설계 원칙:
 * ① 정상은 개수만, 비정상만 상세를 — 응답 크기가 곧 토큰 비용이다.
 * ② 섹션별 독립 try-catch — Kafka Connect가 죽어 있는 상황이야말로 이 API가 필요한
 * 순간이므로, 한 소스의 실패가 전체 응답을 500으로 만들면 안 된다. 실패한 소스는
 * sources에 UNREACHABLE로 표기하고 해당 섹션만 null로 둔다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 11.       | 최남희  | 최초 생성
 * 26. 08. 11.       | 최남희  | other 버킷 추가 — STOPPED·RESTARTING·미지 상태를 원문 그대로 노출
 * --------------------------------------------------
 */
@Service
public class OverviewService {

    /** 상세 목록(failed/paused/unassigned/lagging/pendingApproval) 공통 상한. */
    public static final int MAX_LIST = 20;
    /** FAILED task trace에서 싣는 앞줄 수 — 예외 타입·메시지는 앞줄에 있다. */
    public static final int TRACE_HEAD_LINES = 5;
    /** DDL 요약 길이 (ddlText 앞 N자). */
    public static final int DDL_SUMMARY_CHARS = 120;
    /** recentErrors 조회 구간 — 최근 이벤트 N건 안에서 ERROR를 센다. */
    public static final int ERROR_WINDOW = 200;
    /** recentErrors 응답에 싣는 최대 건수. */
    public static final int ERROR_ITEMS = 10;

    static final String OK = "OK";
    static final String UNREACHABLE = "UNREACHABLE";

    private static final Logger log = LoggerFactory.getLogger(OverviewService.class);

    private final ConnectClient connect;
    private final KafkaMetricsService metrics;
    private final TableEventService events;
    private final DdlEventService ddl;
    private final long lagThreshold;

    public OverviewService(ConnectClient connect, KafkaMetricsService metrics,
                           TableEventService events, DdlEventService ddl,
                           AssistProperties properties) {
        this.connect = connect;
        this.metrics = metrics;
        this.events = events;
        this.ddl = ddl;
        this.lagThreshold = properties.lagThreshold();
    }

    public OverviewResult overview() {
        String connectStatus = OK;
        String kafkaStatus = OK;
        String dbStatus = OK;

        OverviewResult.ConnectorsSection connectors = null;
        try {
            connectors = connectorsSection();
        } catch (Exception e) {
            log.warn("overview: Connect 조회 실패 — {}", e.getMessage());
            connectStatus = UNREACHABLE;
        }

        OverviewResult.TablesSection tables = null;
        try {
            tables = tablesSection();
        } catch (Exception e) {
            log.warn("overview: Kafka 지표 조회 실패 — {}", e.getMessage());
            kafkaStatus = UNREACHABLE;
        }

        // recentErrors와 ddl은 둘 다 메타데이터 DB(PostgreSQL)가 소스 —
        // 어느 한쪽이라도 실패하면 db=UNREACHABLE (성공한 쪽 섹션은 그대로 싣는다)
        OverviewResult.RecentErrorsSection recentErrors = null;
        try {
            recentErrors = recentErrorsSection();
        } catch (Exception e) {
            log.warn("overview: 이벤트 조회 실패 — {}", e.getMessage());
            dbStatus = UNREACHABLE;
        }

        OverviewResult.DdlSection ddlSection = null;
        try {
            ddlSection = ddlSection();
        } catch (Exception e) {
            log.warn("overview: DDL 이벤트 조회 실패 — {}", e.getMessage());
            dbStatus = UNREACHABLE;
        }

        return new OverviewResult(connectors, tables, recentErrors, ddlSection,
                new OverviewResult.Sources(connectStatus, kafkaStatus, dbStatus));
    }

    /**
     * "멈췄다"의 유형 분리가 이 섹션의 존재 이유다:
     * 커넥터 FAILED와 task만 FAILED는 둘 다 failed 목록에 들어가되 connectorState로 구분되고,
     * PAUSED는 사람이 세운 것이므로 이름만 싣는다 (고장이 아니다).
     */
    private OverviewResult.ConnectorsSection connectorsSection() {
        JsonNode all = connect.listConnectors();
        if (all == null || !all.isObject()) {
            throw new IllegalStateException("Connect 응답이 객체가 아니다: " + all);
        }
        int total = 0;
        int running = 0;
        List<OverviewResult.FailedConnector> failed = new ArrayList<>();
        List<String> paused = new ArrayList<>();
        List<String> unassigned = new ArrayList<>();
        List<OverviewResult.OtherConnector> other = new ArrayList<>();

        for (Iterator<String> names = all.fieldNames(); names.hasNext(); ) {
            String name = names.next();
            total++;
            JsonNode status = all.get(name).path("status");
            String state = status.path("connector").path("state").asText("");

            List<OverviewResult.FailedTask> failedTasks = new ArrayList<>();
            for (JsonNode task : status.path("tasks")) {
                if ("FAILED".equals(task.path("state").asText())) {
                    String trace = task.hasNonNull("trace") ? task.get("trace").asText() : null;
                    failedTasks.add(new OverviewResult.FailedTask(
                            task.path("id").asInt(), traceHead(trace)));
                }
            }

            if ("FAILED".equals(state) || !failedTasks.isEmpty()) {
                failed.add(new OverviewResult.FailedConnector(name, state, failedTasks));
            } else if ("PAUSED".equals(state)) {
                paused.add(name);
            } else if ("UNASSIGNED".equals(state)) {
                unassigned.add(name);
            } else if ("RUNNING".equals(state)) {
                running++;   // 건강한 커넥터 — 개수만
            } else {
                // STOPPED·RESTARTING·미지의 상태 전부 — 상태 원문 그대로 노출한다.
                // 이 버킷이 없으면 total≠running인데 이유가 안 보이는 응답이 된다
                other.add(new OverviewResult.OtherConnector(name, state));
            }
        }

        return new OverviewResult.ConnectorsSection(
                total, running,
                failed.size(), cap(failed),
                paused.size(), cap(paused),
                unassigned.size(), cap(unassigned),
                other.size(), cap(other));
    }

    /** 정상 테이블은 total로만 세고, 임계 초과분만 상세를 싣는다. lag은 offset 건수다. */
    private OverviewResult.TablesSection tablesSection() {
        List<KafkaMetricsService.TableMetrics> all = metrics.tableMetrics();
        List<OverviewResult.LaggingTable> lagging = all.stream()
                .filter(m -> m.jdbcLag() > lagThreshold || m.icebergLag() > lagThreshold)
                .map(m -> new OverviewResult.LaggingTable(
                        m.schemaName(), m.tableName(), m.topic(), m.jdbcLag(), m.icebergLag()))
                .toList();
        return new OverviewResult.TablesSection(all.size(), lagging.size(), cap(lagging));
    }

    /**
     * 최근 {@value ERROR_WINDOW}건 안에서 severity=ERROR만 추린다.
     * detail(스택 등 대용량)은 싣지 않는다 — 깊이는 로그 검색으로 판다.
     */
    private OverviewResult.RecentErrorsSection recentErrorsSection() {
        List<TableEvent> errors = events.recent(ERROR_WINDOW).stream()
                .filter(e -> "ERROR".equals(e.severity()))
                .toList();
        List<OverviewResult.ErrorItem> items = errors.stream()
                .limit(ERROR_ITEMS)
                .map(e -> new OverviewResult.ErrorItem(
                        e.occurredAt(), e.schemaName(), e.tableName(), e.eventType(), e.message()))
                .toList();
        return new OverviewResult.RecentErrorsSection(errors.size(), items);
    }

    /** state=DETECTED만 승인 대기다 — SNAPSHOT(정보성)·APPROVED·REJECTED는 제외. */
    private OverviewResult.DdlSection ddlSection() {
        List<DdlEvent> pending = ddl.list().stream()
                .filter(e -> "DETECTED".equals(e.state()))
                .toList();
        List<OverviewResult.PendingDdl> items = pending.stream()
                .limit(MAX_LIST)
                .map(e -> new OverviewResult.PendingDdl(
                        e.id(), e.schemaName(), e.tableName(), e.eventTsMs(),
                        truncate(e.ddlText(), DDL_SUMMARY_CHARS)))
                .toList();
        return new OverviewResult.DdlSection(pending.size(), items);
    }

    /** trace 앞 {@value TRACE_HEAD_LINES}줄 — 잘렸으면 끝에 "…". null·공백이면 null. */
    static String traceHead(String trace) {
        if (trace == null || trace.isBlank()) {
            return null;
        }
        String stripped = trace.stripTrailing();
        String[] parts = stripped.split("\n", TRACE_HEAD_LINES + 1);
        if (parts.length <= TRACE_HEAD_LINES) {
            return stripped;
        }
        return String.join("\n", Arrays.copyOf(parts, TRACE_HEAD_LINES)) + "…";
    }

    /** 앞 max자 절삭 — 잘렸으면 끝에 "…". */
    static String truncate(String text, int max) {
        if (text == null || text.length() <= max) {
            return text;
        }
        return text.substring(0, max) + "…";
    }

    private static <T> List<T> cap(List<T> list) {
        return list.size() <= MAX_LIST ? List.copyOf(list) : List.copyOf(list.subList(0, MAX_LIST));
    }
}
