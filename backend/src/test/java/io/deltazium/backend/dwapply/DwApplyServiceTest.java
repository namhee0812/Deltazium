package io.deltazium.backend.dwapply;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.deltazium.backend.dictionary.DictionaryRouter;
import io.deltazium.backend.dictionary.SourceDictionary;
import io.deltazium.backend.dictionary.TableColumn;
import io.deltazium.backend.registration.ColumnMapping;
import io.deltazium.backend.registration.RegisteredColumnRepository;
import io.deltazium.backend.registration.RegisteredTable;
import io.deltazium.backend.registration.RegisteredTableRepository;
import io.deltazium.backend.registry.DbConnection;
import io.deltazium.backend.registry.DbConnectionService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 파일명 : DwApplyServiceTest.java
 * 작성일자 : 26. 10. 19.
 * 작성자 : 최남희
 * 설명 : DW apply 워커 관리 — 설정 JSON 렌더링 골든, reconcile(설정 기록·기동·재기동·0개 시 종료),
 * 토큰이 argv가 아니라 환경변수로만 가는지, 감시(비정상 종료 재기동 백오프·exit 2 미재기동).
 * 프로세스는 ProcessStarter 가짜로 대체해 `sh -c`를 띄운다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
class DwApplyServiceTest {

    @TempDir
    Path tmp;

    RegisteredTableRepository tables = mock(RegisteredTableRepository.class);
    RegisteredColumnRepository columns = mock(RegisteredColumnRepository.class);
    DbConnectionService connections = mock(DbConnectionService.class);
    DictionaryRouter router = mock(DictionaryRouter.class);
    SourceDictionary sourceDict = mock(SourceDictionary.class);

    final List<List<String>> commands = new ArrayList<>();
    final List<Map<String, String>> envs = new ArrayList<>();
    final AtomicReference<String> shell = new AtomicReference<>("sleep 60");
    final List<Process> started = new ArrayList<>();

    DwApplyService service;

    DbConnection source = new DbConnection(1L, "src", "ORACLE", "SOURCE", "h", 1521, "XE", "u", "p", "dz");
    DbConnection dbx = new DbConnection(3L, "dbx", "DATABRICKS", "TARGET", "dbc-x.cloud.databricks.com", 443,
            "workspace", "token", "dapi-secret", null, "/sql/1.0/warehouses/abc", "default.dz_staging");

    @BeforeEach
    void setUp() {
        ProcessStarter starter = (command, env, log) -> {
            commands.add(command);
            envs.add(env);
            Process p = new ProcessBuilder("sh", "-c", shell.get()).start();
            started.add(p);
            return p;
        };
        service = new DwApplyService(tables, columns, connections, router, starter,
                "../dw-apply/build/install/dw-apply/bin/dw-apply", tmp.resolve("logs").toString(),
                tmp.resolve("run").toString(), 10_000, 4, "localhost:9092");
        when(connections.get(1L)).thenReturn(source);
        when(connections.get(3L)).thenReturn(dbx);
        when(connections.list()).thenReturn(List.of(source, dbx));
        when(router.forConnection(source)).thenReturn(sourceDict);
        when(sourceDict.listColumns(any(), eq("NHTEST"), eq("ORDERS"))).thenReturn(List.of(
                new TableColumn("ID", "NUMBER", true), new TableColumn("NAME", "VARCHAR2", false),
                new TableColumn("MEMO", "VARCHAR2", false)));
        RegisteredTable t = new RegisteredTable(7L, "NHTEST", "ORDERS", 1, 3, "default", "orders");
        when(tables.findAll()).thenReturn(List.of(t));
        when(columns.findByTable(7L)).thenReturn(List.of(
                new ColumnMapping("ID", "${ID}", true),
                new ColumnMapping("NAME", "${NAME}", true),
                new ColumnMapping("MEMO", "${MEMO}", false)));   // 비활성 — 설정에서 제외
    }

    @AfterEach
    void tearDown() {
        started.forEach(Process::destroyForcibly);
    }

    @Test
    void 설정_JSON_골든_토큰은_들어가지_않는다() {
        String json = DwApplyService.renderConfig(dbx,
                List.of(new DwApplyService.TableConfig("dz.NHTEST.ORDERS", "default", "orders", List.of("id"),
                        List.of(new String[] {"ID", "id"}, new String[] {"NAME", "name"}))),
                "localhost:9092", 10_000, 4, Path.of("/run/dw-apply-3.status.json"));
        assertThat(json).isEqualToIgnoringWhitespace("""
                {"targetId":3,"bootstrap":"localhost:9092","groupId":"dz-dw-apply-3",
                 "host":"dbc-x.cloud.databricks.com","httpPath":"/sql/1.0/warehouses/abc","catalog":"workspace",
                 "volume":"/Volumes/workspace/default/dz_staging","batchMs":10000,"batchMaxBytes":67108864,
                 "parallelism":4,"statusFile":"/run/dw-apply-3.status.json",
                 "tables":[{"topics":["dz.NHTEST.ORDERS"],"schema":"default","table":"orders",
                            "keyColumns":["id"],
                            "columns":[{"source":"ID","target":"id"},{"source":"NAME","target":"name"}]}]}""");
        assertThat(json).doesNotContain("dapi-secret");
    }

    @Test
    void reconcile은_설정을_쓰고_토큰을_환경변수로만_넘겨_기동한다() throws Exception {
        service.reconcile(3L);

        Path cfg = tmp.resolve("run/dw-apply-3.json");
        String json = Files.readString(cfg);
        assertThat(json).contains("\"topics\" : [ \"dz.NHTEST.ORDERS\" ]")
                .contains("\"keyColumns\" : [ \"ID\" ]")   // Oracle 소스 기본 매핑은 대문자 — 워커는 대소문자 무시
                .doesNotContain("MEMO")            // 비활성 컬럼 제외
                .doesNotContain("dapi-secret");
        assertThat(commands).hasSize(1);
        assertThat(commands.get(0).get(0)).endsWith("dw-apply/build/install/dw-apply/bin/dw-apply");
        assertThat(commands.get(0).get(1)).isEqualTo("config=" + cfg);
        assertThat(String.join(" ", commands.get(0))).doesNotContain("dapi-secret");
        assertThat(envs.get(0)).containsEntry("DZ_DATABRICKS_TOKEN", "dapi-secret");

        var info = service.list();
        assertThat(info).hasSize(1);
        assertThat(info.get(0).targetId()).isEqualTo(3L);
        assertThat(info.get(0).running()).isTrue();
    }

    @Test
    void reconcile을_다시_부르면_기존_워커를_종료하고_재기동한다() throws Exception {
        service.reconcile(3L);
        Process first = started.get(0);
        service.reconcile(3L);

        assertThat(first.waitFor(15, TimeUnit.SECONDS)).isTrue();
        assertThat(started).hasSize(2);
        assertThat(started.get(1).isAlive()).isTrue();
    }

    @Test
    void 등록_테이블이_0개면_워커를_종료하고_설정을_지운다() throws Exception {
        service.reconcile(3L);
        Process p = started.get(0);
        when(tables.findAll()).thenReturn(List.of());

        service.reconcile(3L);

        assertThat(p.waitFor(15, TimeUnit.SECONDS)).isTrue();
        assertThat(Files.exists(tmp.resolve("run/dw-apply-3.json"))).isFalse();
        assertThat(service.list().get(0).running()).isFalse();
    }

    @Test
    void 비정상_종료는_30초_후_재기동하고_연속_실패는_백오프가_늘어난다() throws Exception {
        shell.set("exit 1");
        service.reconcile(3L);
        started.get(0).waitFor();
        long t0 = System.currentTimeMillis();

        service.superviseOnce(t0);
        assertThat(service.list().get(0).error()).contains("exit 1");
        service.superviseOnce(t0 + 29_000);
        assertThat(started).hasSize(1);            // 아직 대기
        service.superviseOnce(t0 + 31_000);
        assertThat(started).hasSize(2);            // 30초 후 재기동
        assertThat(service.list().get(0).restarts()).isEqualTo(1);

        started.get(1).waitFor();
        service.superviseOnce(t0 + 32_000);        // 곧바로 또 죽음 → 다음 대기는 60초
        service.superviseOnce(t0 + 32_000 + 59_000);
        assertThat(started).hasSize(2);
        service.superviseOnce(t0 + 32_000 + 61_000);
        assertThat(started).hasSize(3);
    }

    @Test
    void exit_2는_자동_재기동하지_않는다() throws Exception {
        shell.set("exit 2");
        service.reconcile(3L);
        started.get(0).waitFor();

        service.superviseOnce(System.currentTimeMillis());
        service.superviseOnce(System.currentTimeMillis() + 600_000);

        assertThat(started).hasSize(1);
        assertThat(service.list().get(0).error()).contains("exit 2");
    }

    @Test
    void 토큰이_없으면_기동하지_않고_사유를_남긴다() {
        io.deltazium.backend.registry.DatabricksSupport.setEnvForTest(k -> null);
        try {
            DbConnection noToken = new DbConnection(3L, "dbx", "DATABRICKS", "TARGET", "h", 443, "workspace",
                    "token", "", null, "/p", "default.v");
            when(connections.get(3L)).thenReturn(noToken);
            when(connections.list()).thenReturn(List.of(noToken));

            service.reconcile(3L);

            assertThat(started).isEmpty();
            assertThat(service.list().get(0).error()).contains("DZ_DATABRICKS_TOKEN");
        } finally {
            io.deltazium.backend.registry.DatabricksSupport.setEnvForTest(null);
        }
    }

    @Test
    void 상태_파일_내용을_그대로_노출한다() throws IOException {
        service.reconcile(3L);
        Files.writeString(tmp.resolve("run/dw-apply-3.status.json"),
                "{\"lastBatchRows\":5,\"lastError\":null,\"tables\":{}}");

        assertThat(service.list().get(0).status().get("lastBatchRows").asInt()).isEqualTo(5);
    }
}
