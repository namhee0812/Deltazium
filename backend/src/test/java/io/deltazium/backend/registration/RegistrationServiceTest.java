package io.deltazium.backend.registration;

import java.util.List;
import java.util.Map;

import io.deltazium.backend.connect.ConnectorDeployService;
import io.deltazium.backend.dictionary.OracleDictionaryService;
import io.deltazium.backend.dictionary.SourceTableInfo;
import io.deltazium.backend.dictionary.TableColumn;
import io.deltazium.backend.iceberg.ChangelogTableService;
import io.deltazium.backend.iceberg.IcebergProperties;
import io.deltazium.backend.registry.DbConnection;
import io.deltazium.backend.registry.DbConnectionRepository;
import io.deltazium.backend.registry.DbConnectionService;
import io.deltazium.backend.registry.OracleConnectionTester;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@JdbcTest
@Import({RegisteredTableRepository.class, RegisteredColumnRepository.class,
        RegistrationService.class, DbConnectionRepository.class, DbConnectionService.class})
@EnableConfigurationProperties(IcebergProperties.class)
class RegistrationServiceTest {

    @Autowired
    RegistrationService service;

    @Autowired
    DbConnectionService connections;

    @MockitoBean
    OracleDictionaryService dictionary;

    @MockitoBean
    ConnectorDeployService deploy;

    @MockitoBean
    ChangelogTableService changelog;

    @MockitoBean
    OracleConnectionTester tester;

    long srcId;
    long tgtId;

    @BeforeEach
    void setUp() {
        srcId = connections.create(new DbConnection(null, "src", "ORACLE", "SOURCE",
                "srchost", 1521, "SRCPDB", "dbz", "pw")).id();
        tgtId = connections.create(new DbConnection(null, "tgt", "ORACLE", "TARGET",
                "tgthost", 1521, "TGTPDB", "apply", "pw")).id();
        when(changelog.changelogTableName(anyString(), anyString())).thenAnswer(inv ->
                "changelog." + (inv.getArgument(0) + "_" + inv.getArgument(1)).toString().toLowerCase());
    }

    private void mockTable(String qualified, boolean pk, boolean supp) {
        int dot = qualified.indexOf('.');
        String schema = qualified.substring(0, dot);
        String table = qualified.substring(dot + 1);
        when(dictionary.listTables(any(), eq(qualified))).thenReturn(List.of(
                new SourceTableInfo(schema, table, pk, supp, 100L)));
        when(dictionary.listColumns(any(), eq(schema), eq(table))).thenReturn(List.of(
                new TableColumn("ID", "NUMBER", true),
                new TableColumn("AMOUNT", "NUMBER", false),
                new TableColumn("STATUS", "VARCHAR2", false)));
    }

    private static RegistrationService.TableSpec spec(String source) {
        return new RegistrationService.TableSpec(source, null, null, null);
    }

    @Test
    @SuppressWarnings("unchecked")
    void 등록하면_jdbc_sink가_테이블별로_배포되고_기본_매핑이_저장된다() {
        mockTable("CDC.T1", true, true);
        mockTable("CDC.T2", true, true);

        List<RegisteredTable> result = service.register(srcId, tgtId,
                List.of(spec("CDC.T1"), spec("CDC.T2")));

        assertThat(result).hasSize(2);
        // 기본 매핑: 전 컬럼 동일명 활성
        assertThat(service.mappings(result.get(0).id()))
                .extracting(ColumnMapping::targetColumn)
                .containsExactly("ID", "AMOUNT", "STATUS");

        ArgumentCaptor<Map<String, String>> vars = ArgumentCaptor.forClass(Map.class);
        verify(deploy).deploy(eq("source"), vars.capture());
        assertThat(vars.getValue()).containsEntry("table_include_list", "CDC.T1,CDC.T2");

        ArgumentCaptor<Map<String, String>> jdbcVars = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<Map<String, String>> jdbcExtra = ArgumentCaptor.forClass(Map.class);
        verify(deploy, org.mockito.Mockito.times(2))
                .deploy(eq("jdbc-sink"), jdbcVars.capture(), jdbcExtra.capture());
        assertThat(jdbcVars.getAllValues().get(0))
                .containsEntry("connector_name", "dz-jdbc-sink-cdc_t1")
                .containsEntry("topics", "dz.CDC.T1")
                .containsEntry("collection_name", "CDC.T1");
        // 전 컬럼 동일명 활성 → include 필터 생략
        assertThat(jdbcExtra.getAllValues().get(0)).isEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    void 타깃_이름과_컬럼_매핑이_배포에_반영된다() {
        mockTable("CDC.T1", true, true);
        var columns = List.of(
                new ColumnMapping("ID", "${ID}", true),
                new ColumnMapping("AMOUNT", "${AMOUNT}", false),
                new ColumnMapping("STATUS", "${STATUS}", true));

        service.register(srcId, tgtId, List.of(new RegistrationService.TableSpec(
                "CDC.T1", "tgt_own", "t1_copy", columns)));

        ArgumentCaptor<Map<String, String>> vars = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<Map<String, String>> extra = ArgumentCaptor.forClass(Map.class);
        verify(deploy).deploy(eq("jdbc-sink"), vars.capture(), extra.capture());
        assertThat(vars.getValue()).containsEntry("collection_name", "TGT_OWN.T1_COPY");
        assertThat(extra.getValue()).containsEntry("field.include.list", "ID,STATUS");
    }

    @Test
    void 변환식_구문_오류는_거부된다() {
        mockTable("CDC.T1", true, true);
        var columns = List.of(
                new ColumnMapping("ID", "${ID}", true),
                new ColumnMapping("AMOUNT", "${{AMOUNT}", true));
        assertThatThrownBy(() -> service.register(srcId, tgtId, List.of(
                new RegistrationService.TableSpec("CDC.T1", null, null, columns))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("변환식 구문 오류");
    }

    @Test
    void 소스에_없는_컬럼_참조는_거부된다() {
        mockTable("CDC.T1", true, true);
        var columns = List.of(
                new ColumnMapping("ID", "${ID}", true),
                new ColumnMapping("X", "${NOPE}", true));
        assertThatThrownBy(() -> service.register(srcId, tgtId, List.of(
                new RegistrationService.TableSpec("CDC.T1", null, null, columns))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("소스에 없는 컬럼");
    }

    @Test
    void PK_컬럼이_해제되면_거부된다() {
        mockTable("CDC.T1", true, true);
        var columns = List.of(
                new ColumnMapping("ID", "${ID}", false),
                new ColumnMapping("AMOUNT", "${AMOUNT}", true));
        assertThatThrownBy(() -> service.register(srcId, tgtId, List.of(
                new RegistrationService.TableSpec("CDC.T1", null, null, columns))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PK 컬럼");
    }

    @Test
    void PK_없는_테이블은_등록_거부() {
        mockTable("CDC.NOPK", false, true);
        assertThatThrownBy(() -> service.register(srcId, tgtId, List.of(spec("CDC.NOPK"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PK 없는 테이블");
    }

    @Test
    void supp_log_미설정_테이블은_등록_거부() {
        mockTable("CDC.NOSUPP", true, false);
        assertThatThrownBy(() -> service.register(srcId, tgtId, List.of(spec("CDC.NOSUPP"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("supplemental logging");
    }

    @Test
    void 와일드카드는_등록_시점에_거부() {
        assertThatThrownBy(() -> service.register(srcId, tgtId, List.of(spec("CDC.*"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("와일드카드");
    }

    @Test
    void 역할이_뒤바뀐_연결은_거부() {
        assertThatThrownBy(() -> service.register(tgtId, srcId, List.of(spec("CDC.T1"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SOURCE");
    }

    @Test
    void 중복_등록_거부() {
        mockTable("CDC.T1", true, true);
        service.register(srcId, tgtId, List.of(spec("CDC.T1")));
        assertThatThrownBy(() -> service.register(srcId, tgtId, List.of(spec("CDC.T1"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("이미 등록된");
    }

    @Test
    void 다른_스키마의_동명_테이블은_라우팅_충돌로_거부() {
        mockTable("CDC.T1", true, true);
        service.register(srcId, tgtId, List.of(spec("CDC.T1")));

        mockTable("HR.T1", true, true);
        assertThatThrownBy(() -> service.register(srcId, tgtId, List.of(spec("HR.T1"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("라우팅");
    }

    @Test
    @SuppressWarnings("unchecked")
    void 등록하면_changelog_테이블_사전_생성_후_iceberg_sink가_배선된다() {
        mockTable("CDC.T1", true, true);
        service.register(srcId, tgtId, List.of(spec("CDC.T1")));

        verify(changelog).ensureChangelogTable("CDC", "T1");
        ArgumentCaptor<Map<String, String>> vars = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<Map<String, String>> extra = ArgumentCaptor.forClass(Map.class);
        verify(deploy).deploy(eq("iceberg-sink"), vars.capture(), extra.capture());
        assertThat(vars.getValue()).containsEntry("iceberg_tables", "changelog.cdc_t1");
        assertThat(extra.getValue())
                .containsEntry("iceberg.table.changelog.cdc_t1.route-regex", "^T1$");
    }
}
