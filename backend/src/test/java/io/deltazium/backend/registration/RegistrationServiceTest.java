package io.deltazium.backend.registration;

import java.util.List;
import java.util.Map;

import io.deltazium.backend.connect.ConnectorDeployService;
import io.deltazium.backend.dictionary.OracleDictionaryService;
import io.deltazium.backend.dictionary.SourceTableInfo;
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
@Import({RegisteredTableRepository.class, RegistrationService.class,
        DbConnectionRepository.class, DbConnectionService.class})
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
        when(dictionary.listTables(any(), eq(qualified))).thenReturn(List.of(new SourceTableInfo(
                qualified.substring(0, dot), qualified.substring(dot + 1), pk, supp, 100L)));
    }

    @Test
    @SuppressWarnings("unchecked")
    void 등록하면_source와_jdbc_sink가_전체_목록으로_배포된다() {
        mockTable("CDC.T1", true, true);
        mockTable("CDC.T2", true, true);

        List<RegisteredTable> result = service.register(srcId, tgtId, List.of("CDC.T1", "CDC.T2"));

        assertThat(result).hasSize(2);
        ArgumentCaptor<Map<String, String>> vars = ArgumentCaptor.forClass(Map.class);
        verify(deploy).deploy(eq("source"), vars.capture());
        assertThat(vars.getValue())
                .containsEntry("connector_name", "dz-source")
                .containsEntry("table_include_list", "CDC.T1,CDC.T2")
                .containsEntry("oracle_host", "srchost");
        verify(deploy).deploy(eq("jdbc-sink"), vars.capture());
        assertThat(vars.getValue())
                .containsEntry("topics", "dz.CDC.T1,dz.CDC.T2")
                .containsEntry("target_jdbc_url", "jdbc:oracle:thin:@//tgthost:1521/TGTPDB");
    }

    @Test
    @SuppressWarnings("unchecked")
    void 등록하면_changelog_테이블_사전_생성_후_iceberg_sink가_배선된다() {
        mockTable("CDC.T1", true, true);
        mockTable("CDC.T2", true, true);

        service.register(srcId, tgtId, List.of("CDC.T1", "CDC.T2"));

        verify(changelog).ensureChangelogTable("CDC", "T1");
        verify(changelog).ensureChangelogTable("CDC", "T2");
        ArgumentCaptor<Map<String, String>> vars = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<Map<String, String>> extra = ArgumentCaptor.forClass(Map.class);
        verify(deploy).deploy(eq("iceberg-sink"), vars.capture(), extra.capture());
        assertThat(vars.getValue())
                .containsEntry("connector_name", "dz-iceberg-sink")
                .containsEntry("topics", "dz.CDC.T1,dz.CDC.T2")
                .containsEntry("iceberg_tables", "changelog.cdc_t1,changelog.cdc_t2");
        assertThat(extra.getValue())
                .containsEntry("iceberg.table.changelog.cdc_t1.route-regex", "^T1$")
                .containsEntry("iceberg.table.changelog.cdc_t2.route-regex", "^T2$");
    }

    @Test
    void 다른_스키마의_동명_테이블은_라우팅_충돌로_거부() {
        mockTable("CDC.T1", true, true);
        service.register(srcId, tgtId, List.of("CDC.T1"));

        mockTable("HR.T1", true, true);
        assertThatThrownBy(() -> service.register(srcId, tgtId, List.of("HR.T1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("라우팅");
    }

    @Test
    void PK_없는_테이블은_등록_거부() {
        mockTable("CDC.NOPK", false, true);
        assertThatThrownBy(() -> service.register(srcId, tgtId, List.of("CDC.NOPK")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PK 없는 테이블");
    }

    @Test
    void supp_log_미설정_테이블은_등록_거부() {
        mockTable("CDC.NOSUPP", true, false);
        assertThatThrownBy(() -> service.register(srcId, tgtId, List.of("CDC.NOSUPP")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("supplemental logging");
    }

    @Test
    void 와일드카드는_등록_시점에_거부() {
        assertThatThrownBy(() -> service.register(srcId, tgtId, List.of("CDC.*")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("와일드카드");
    }

    @Test
    void 역할이_뒤바뀐_연결은_거부() {
        assertThatThrownBy(() -> service.register(tgtId, srcId, List.of("CDC.T1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SOURCE");
    }

    @Test
    void 중복_등록_거부() {
        mockTable("CDC.T1", true, true);
        service.register(srcId, tgtId, List.of("CDC.T1"));
        assertThatThrownBy(() -> service.register(srcId, tgtId, List.of("CDC.T1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("이미 등록된");
    }
}
