package io.deltazium.backend.assist;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.deltazium.backend.connect.ConnectClient;
import io.deltazium.backend.ddl.DdlEvent;
import io.deltazium.backend.ddl.DdlEventService;
import io.deltazium.backend.events.TableEvent;
import io.deltazium.backend.events.TableEventService;
import io.deltazium.backend.metrics.KafkaMetricsService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 파일명 : OverviewServiceTest.java
 * 작성일자 : 26. 08. 11.
 * 작성자 : 최남희
 * 설명 : overview 규약 단위 테스트 (외부 의존은 전부 mock) —
 * trace 5줄 절삭, lag 임계 판정, ERROR만 추림, DETECTED만 승인 대기,
 * 소스 장애 격리(섹션 null + UNREACHABLE), 상세 목록 20건 상한.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 11.       | 최남희  | 최초 생성
 * 26. 08. 11.       | 최남희  | other 버킷 검증 추가 (STOPPED·미지 상태)
 * --------------------------------------------------
 */
class OverviewServiceTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private ConnectClient connect;
    private KafkaMetricsService metrics;
    private TableEventService events;
    private DdlEventService ddl;
    private OverviewService service;

    @BeforeEach
    void setUp() throws Exception {
        connect = mock(ConnectClient.class);
        metrics = mock(KafkaMetricsService.class);
        events = mock(TableEventService.class);
        ddl = mock(DdlEventService.class);
        // 기본값: 전부 정상·빈 상태 — 각 테스트가 필요한 소스만 덮어쓴다
        when(connect.listConnectors()).thenReturn(JSON.readTree("{}"));
        when(metrics.tableMetrics()).thenReturn(List.of());
        when(events.recent(anyInt())).thenReturn(List.of());
        when(ddl.list()).thenReturn(List.of());
        service = new OverviewService(connect, metrics, events, ddl,
                new AssistProperties("/tmp", 1000L));
    }

    // ── 커넥터 ──────────────────────────────────────────

    @Test
    void FAILED_task의_trace는_앞_5줄로_절삭되고_말줄임이_붙는다() throws Exception {
        String trace = "org.apache.kafka.connect.errors.ConnectException: boom\\n"
                + "L2\\nL3\\nL4\\nL5\\nL6\\nL7";
        when(connect.listConnectors()).thenReturn(JSON.readTree("""
                {"dz-jdbc-sink-a": {"status": {
                    "connector": {"state": "RUNNING"},
                    "tasks": [{"id": 0, "state": "FAILED", "trace": "%s"}]
                }}}""".formatted(trace)));

        OverviewResult.ConnectorsSection c = service.overview().connectors();

        assertThat(c.failed()).hasSize(1);
        OverviewResult.FailedConnector fc = c.failed().get(0);
        // 커넥터는 RUNNING인데 task만 FAILED — connectorState로 유형이 구분돼야 한다
        assertThat(fc.connectorState()).isEqualTo("RUNNING");
        assertThat(fc.failedTasks()).hasSize(1);
        String head = fc.failedTasks().get(0).traceHead();
        assertThat(head).isEqualTo(
                "org.apache.kafka.connect.errors.ConnectException: boom\nL2\nL3\nL4\nL5…");
        assertThat(head).doesNotContain("L6");
        // task만 FAILED인 커넥터는 "건강한 running"에 세지 않는다
        assertThat(c.running()).isZero();
        assertThat(c.total()).isEqualTo(1);
    }

    @Test
    void trace가_5줄_이하면_절삭하지_않고_말줄임도_없다() {
        assertThat(OverviewService.traceHead("A\nB\nC")).isEqualTo("A\nB\nC");
        assertThat(OverviewService.traceHead("A\nB\nC\nD\nE\n")).isEqualTo("A\nB\nC\nD\nE");
        assertThat(OverviewService.traceHead(null)).isNull();
    }

    @Test
    void 커넥터_상태별로_running_paused_unassigned가_분류된다() throws Exception {
        when(connect.listConnectors()).thenReturn(JSON.readTree("""
                {
                  "a": {"status": {"connector": {"state": "RUNNING"},
                        "tasks": [{"id": 0, "state": "RUNNING"}]}},
                  "b": {"status": {"connector": {"state": "PAUSED"},
                        "tasks": [{"id": 0, "state": "PAUSED"}]}},
                  "c": {"status": {"connector": {"state": "UNASSIGNED"}, "tasks": []}},
                  "d": {"status": {"connector": {"state": "FAILED"},
                        "tasks": [{"id": 0, "state": "FAILED", "trace": "X"}]}}
                }"""));

        OverviewResult.ConnectorsSection c = service.overview().connectors();

        assertThat(c.total()).isEqualTo(4);
        assertThat(c.running()).isEqualTo(1);
        assertThat(c.paused()).containsExactly("b");
        assertThat(c.unassigned()).containsExactly("c");
        assertThat(c.failed()).hasSize(1);
        assertThat(c.failed().get(0).name()).isEqualTo("d");
        assertThat(c.failed().get(0).connectorState()).isEqualTo("FAILED");
    }

    @Test
    void STOPPED_등_그외_상태는_other에_상태_원문과_함께_실린다() throws Exception {
        when(connect.listConnectors()).thenReturn(JSON.readTree("""
                {
                  "a": {"status": {"connector": {"state": "RUNNING"},
                        "tasks": [{"id": 0, "state": "RUNNING"}]}},
                  "s": {"status": {"connector": {"state": "STOPPED"}, "tasks": []}},
                  "r": {"status": {"connector": {"state": "RESTARTING"}, "tasks": []}}
                }"""));

        OverviewResult.ConnectorsSection c = service.overview().connectors();

        assertThat(c.total()).isEqualTo(3);
        assertThat(c.running()).isEqualTo(1);
        assertThat(c.otherCount()).isEqualTo(2);
        assertThat(c.other()).containsExactlyInAnyOrder(
                new OverviewResult.OtherConnector("s", "STOPPED"),
                new OverviewResult.OtherConnector("r", "RESTARTING"));
        // other로 간 커넥터는 다른 버킷에 없어야 한다
        assertThat(c.failed()).isEmpty();
        assertThat(c.paused()).isEmpty();
        assertThat(c.unassigned()).isEmpty();
    }

    // ── 테이블 lag ──────────────────────────────────────

    @Test
    void 임계_이하_테이블은_lagging에_없고_total에는_잡힌다() {
        when(metrics.tableMetrics()).thenReturn(List.of(
                new KafkaMetricsService.TableMetrics("CDC", "OK_TBL", "dz.CDC.OK_TBL",
                        10_000, 1.0, 1000, 999),          // 정확히 임계 = 초과 아님
                new KafkaMetricsService.TableMetrics("CDC", "SLOW_TBL", "dz.CDC.SLOW_TBL",
                        10_000, 1.0, 1001, 0)));           // jdbcLag만 초과

        OverviewResult.TablesSection t = service.overview().tables();

        assertThat(t.total()).isEqualTo(2);
        assertThat(t.laggingCount()).isEqualTo(1);
        assertThat(t.lagging()).hasSize(1);
        assertThat(t.lagging().get(0).table()).isEqualTo("SLOW_TBL");
        assertThat(t.lagging().get(0).jdbcLag()).isEqualTo(1001);
    }

    // ── 최근 에러 ───────────────────────────────────────

    @Test
    void severity가_ERROR인_이벤트만_recentErrors에_들어간다() {
        LocalDateTime now = LocalDateTime.now();
        when(events.recent(OverviewService.ERROR_WINDOW)).thenReturn(List.of(
                event(1, now, "ERROR", "sink 실패"),
                event(2, now, "INFO", "등록 완료"),
                event(3, now, "WARN", "DDL 거부"),
                event(4, now, "ERROR", "task 죽음")));

        OverviewResult.RecentErrorsSection r = service.overview().recentErrors();

        assertThat(r.count()).isEqualTo(2);
        assertThat(r.items()).hasSize(2);
        assertThat(r.items()).allSatisfy(i ->
                assertThat(i.message()).isIn("sink 실패", "task 죽음"));
    }

    @Test
    void recentErrors_items는_최대_10건이고_count는_구간_내_전체_ERROR_수다() {
        List<TableEvent> many = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            many.add(event(i, LocalDateTime.now(), "ERROR", "e" + i));
        }
        when(events.recent(OverviewService.ERROR_WINDOW)).thenReturn(many);

        OverviewResult.RecentErrorsSection r = service.overview().recentErrors();

        assertThat(r.count()).isEqualTo(15);
        assertThat(r.items()).hasSize(OverviewService.ERROR_ITEMS);
    }

    // ── DDL ────────────────────────────────────────────

    @Test
    void state가_DETECTED인_DDL만_pendingApproval에_들어간다() {
        String longDdl = "ALTER TABLE CDC.T1 ADD (" + "C".repeat(150) + " NUMBER)";
        when(ddl.list()).thenReturn(List.of(
                ddlEvent(1, "SNAPSHOT", "CREATE TABLE ..."),
                ddlEvent(2, "DETECTED", longDdl),
                ddlEvent(3, "APPROVED", "ALTER ..."),
                ddlEvent(4, "REJECTED", "DROP ...")));

        OverviewResult.DdlSection d = service.overview().ddl();

        assertThat(d.pendingCount()).isEqualTo(1);
        assertThat(d.pendingApproval()).hasSize(1);
        OverviewResult.PendingDdl p = d.pendingApproval().get(0);
        assertThat(p.id()).isEqualTo(2);
        // ddlSummary는 앞 120자 + 말줄임
        assertThat(p.ddlSummary()).hasSize(OverviewService.DDL_SUMMARY_CHARS + 1);
        assertThat(p.ddlSummary()).startsWith("ALTER TABLE CDC.T1").endsWith("…");
    }

    // ── 장애 격리 ───────────────────────────────────────

    @Test
    void Connect가_죽어도_나머지_섹션은_정상이고_sources만_UNREACHABLE이다() {
        when(connect.listConnectors()).thenThrow(new RuntimeException("Connection refused"));
        when(metrics.tableMetrics()).thenReturn(List.of(
                new KafkaMetricsService.TableMetrics("CDC", "T1", "dz.CDC.T1", 10, 0.0, 0, 0)));

        OverviewResult result = service.overview();

        assertThat(result.sources().connect()).isEqualTo("UNREACHABLE");
        assertThat(result.connectors()).isNull();
        // 나머지 소스·섹션은 살아 있다 — Connect가 죽은 상황이야말로 이 API가 필요한 순간
        assertThat(result.sources().kafka()).isEqualTo("OK");
        assertThat(result.sources().db()).isEqualTo("OK");
        assertThat(result.tables().total()).isEqualTo(1);
        assertThat(result.recentErrors()).isNotNull();
        assertThat(result.ddl()).isNotNull();
    }

    @Test
    void Kafka가_죽으면_tables만_null이고_kafka가_UNREACHABLE이다() {
        when(metrics.tableMetrics()).thenThrow(new RuntimeException("timeout"));

        OverviewResult result = service.overview();

        assertThat(result.sources().kafka()).isEqualTo("UNREACHABLE");
        assertThat(result.tables()).isNull();
        assertThat(result.sources().connect()).isEqualTo("OK");
        assertThat(result.connectors()).isNotNull();
    }

    @Test
    void DB가_죽으면_recentErrors와_ddl이_null이고_db가_UNREACHABLE이다() {
        when(events.recent(anyInt())).thenThrow(new RuntimeException("PG down"));
        when(ddl.list()).thenThrow(new RuntimeException("PG down"));

        OverviewResult result = service.overview();

        assertThat(result.sources().db()).isEqualTo("UNREACHABLE");
        assertThat(result.recentErrors()).isNull();
        assertThat(result.ddl()).isNull();
        assertThat(result.sources().connect()).isEqualTo("OK");
    }

    // ── 상세 목록 상한 ──────────────────────────────────

    @Test
    void 상세_목록은_20건_상한이고_전체_규모는_count_필드로_남는다() throws Exception {
        StringBuilder json = new StringBuilder("{");
        for (int i = 0; i < 25; i++) {
            if (i > 0) {
                json.append(",");
            }
            json.append("\"p").append(i).append("\": {\"status\": {")
                    .append("\"connector\": {\"state\": \"PAUSED\"}, \"tasks\": []}}");
        }
        json.append("}");
        when(connect.listConnectors()).thenReturn(JSON.readTree(json.toString()));

        List<DdlEvent> pending = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            pending.add(ddlEvent(i, "DETECTED", "ALTER TABLE T" + i));
        }
        when(ddl.list()).thenReturn(pending);

        List<KafkaMetricsService.TableMetrics> tables = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            tables.add(new KafkaMetricsService.TableMetrics("CDC", "T" + i, "dz.CDC.T" + i,
                    10_000, 0.0, 5000, 0));
        }
        when(metrics.tableMetrics()).thenReturn(tables);

        OverviewResult result = service.overview();

        assertThat(result.connectors().paused()).hasSize(OverviewService.MAX_LIST);
        assertThat(result.connectors().pausedCount()).isEqualTo(25);
        assertThat(result.ddl().pendingApproval()).hasSize(OverviewService.MAX_LIST);
        assertThat(result.ddl().pendingCount()).isEqualTo(25);
        assertThat(result.tables().lagging()).hasSize(OverviewService.MAX_LIST);
        assertThat(result.tables().laggingCount()).isEqualTo(25);
    }

    // ── 헬퍼 ────────────────────────────────────────────

    private static TableEvent event(long id, LocalDateTime at, String severity, String message) {
        return new TableEvent(id, at, "CDC", "T1", "SINK_FAILED", severity, message,
                "detail은 응답에 실리면 안 된다");
    }

    private static DdlEvent ddlEvent(long id, String state, String ddlText) {
        return new DdlEvent(id, id, 1_700_000_000_000L + id, String.valueOf(100 + id),
                "CDC", "T" + id, ddlText, state, null, null);
    }
}
