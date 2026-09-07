package io.deltazium.backend.registration;

import java.util.List;
import java.util.Map;

import io.deltazium.backend.connect.ConnectorDeployService;
import io.deltazium.backend.dictionary.DictionaryRouter;
import io.deltazium.backend.dictionary.OracleDictionaryService;
import io.deltazium.backend.dictionary.PostgresDictionaryService;
import io.deltazium.backend.dictionary.SourceTableInfo;
import io.deltazium.backend.dictionary.TableColumn;
import io.deltazium.backend.iceberg.ChangelogTableService;
import io.deltazium.backend.iceberg.IcebergProperties;
import io.deltazium.backend.registry.DbConnection;
import io.deltazium.backend.registry.DbConnectionRepository;
import io.deltazium.backend.registry.DbConnectionService;
import io.deltazium.backend.registry.DbType;
import io.deltazium.backend.registry.OracleConnectionTester;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mybatis.spring.boot.test.autoconfigure.MybatisTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.sql.init.SqlInitializationAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@MybatisTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ImportAutoConfiguration(SqlInitializationAutoConfiguration.class)
@Import({RegistrationService.class, DbConnectionService.class, DictionaryRouter.class,
        io.deltazium.backend.events.TableEventService.class})
/**
 * 파일명 : RegistrationServiceTest.java
 * 작성일자 : 26. 07. 25.
 * 작성자 : 최남희
 * 설명 : 테이블 등록 서비스(매핑 정규화·커넥터 설정) 단위 테스트.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 25.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 08. 04.       | 최남희  | resnapshot 의존성(TargetDdlExecutor·KafkaMetricsService) 목 추가,
 * |                          | truncate 재구축 순서 검증 테스트
 * --------------------------------------------------
 * 26. 08. 05.       | 최남희  | resnapshot 테스트를 ResnapshotOrchestratorTest로 이관
 * --------------------------------------------------
 * 26. 09. 05.       | 최남희  | 다중 소스·다중 타깃 ①: iceberg-sink 커넥터명·route-regex를
 * |                          | 소스별 인스턴스·토픽 이름 정확 일치 기준으로 갱신, 동명 테이블
 * |                          | 거부 테스트를 성공 케이스로 전환 (5.1절 제약 해소)
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ②: 커넥터 이름에 소스 topicPrefix 반영,
 * |                          | source 템플릿을 source-oracle로, 다중 소스 격리(소스별 독립
 * |                          | 배포·해제) 테스트 추가
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ③ 저장소 프로파일: iceberg-sink extraConfig에
 * |                          | 종전 템플릿의 iceberg.catalog.* 키·값이 그대로 들어가는지 검증
 * |                          | 추가(회귀 방지, 템플릿에서 backend 주입으로 이관됐기 때문)
 * --------------------------------------------------
 */
@EnableConfigurationProperties(IcebergProperties.class)
class RegistrationServiceTest {

    @Autowired
    RegistrationService service;

    @Autowired
    DbConnectionService connections;

    @MockitoBean
    OracleDictionaryService dictionary;

    /** DictionaryRouter가 List<SourceDictionary>를 주입받으므로 두 구현 모두 빈이어야 한다. */
    @MockitoBean
    PostgresDictionaryService pgDictionary;

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
        when(dictionary.dbType()).thenReturn(DbType.ORACLE);
        when(dictionary.captureSetupLabel()).thenReturn("supplemental logging (ALL) COLUMNS");
        when(pgDictionary.dbType()).thenReturn(DbType.POSTGRESQL);
        // 소스 커넥션의 topicPrefix가 커넥터 이름(dz-*-<prefix>-*)에 그대로 들어간다 — "dz"로 고정해
        // 기존(단일 소스 시절) 커넥터명 기댓값과의 diff를 prefix 삽입만으로 좁힌다.
        srcId = connections.create(new DbConnection(null, "src", "ORACLE", "SOURCE",
                "srchost", 1521, "SRCPDB", "dbz", "pw", "dz")).id();
        tgtId = connections.create(new DbConnection(null, "tgt", "ORACLE", "TARGET",
                "tgthost", 1521, "TGTPDB", "apply", "pw")).id();
        when(changelog.changelogTableName(anyString(), anyString(), anyString())).thenAnswer(inv ->
                "changelog." + (inv.getArgument(1) + "_" + inv.getArgument(2)).toString().toLowerCase());
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

    private void mockPgTable(String qualified, boolean pk, boolean ready) {
        int dot = qualified.indexOf('.');
        String schema = qualified.substring(0, dot);
        String table = qualified.substring(dot + 1);
        when(pgDictionary.listTables(any(), eq(qualified))).thenReturn(List.of(
                new SourceTableInfo(schema, table, pk, ready, 100L)));
        when(pgDictionary.listColumns(any(), eq(schema), eq(table))).thenReturn(List.of(
                new TableColumn("id", "int4", true),
                new TableColumn("status", "text", false)));
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
        verify(deploy).deploy(eq("source-oracle"), vars.capture());
        assertThat(vars.getValue()).containsEntry("table_include_list", "CDC.T1,CDC.T2");

        ArgumentCaptor<Map<String, String>> jdbcVars = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<Map<String, String>> jdbcExtra = ArgumentCaptor.forClass(Map.class);
        verify(deploy, org.mockito.Mockito.times(2))
                .deploy(eq("jdbc-sink"), jdbcVars.capture(), jdbcExtra.capture());
        assertThat(jdbcVars.getAllValues().get(0))
                .containsEntry("connector_name", "dz-jdbc-sink-dz-cdc_t1")
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
    void 다른_스키마의_동명_테이블도_등록된다() {
        // route-field가 토픽 이름 기준(_pos.topic)으로 바뀌며 동명 테이블 제약이 해소됨 (5.1절)
        mockTable("CDC.T1", true, true);
        service.register(srcId, tgtId, List.of(spec("CDC.T1")));

        mockTable("HR.T1", true, true);
        List<RegisteredTable> result = service.register(srcId, tgtId, List.of(spec("HR.T1")));

        assertThat(result).extracting(RegisteredTable::qualified)
                .contains("CDC.T1", "HR.T1");
    }

    @Test
    @SuppressWarnings("unchecked")
    void NO_DATA_모드로_등록하면_source에_no_data가_배포된다() {
        mockTable("CDC.T1", true, true);
        service.register(srcId, tgtId, List.of(spec("CDC.T1")), "NO_DATA");

        ArgumentCaptor<Map<String, String>> vars = ArgumentCaptor.forClass(Map.class);
        verify(deploy).deploy(eq("source-oracle"), vars.capture());
        assertThat(vars.getValue()).containsEntry("snapshot_mode", "no_data");
    }

    @Test
    @SuppressWarnings("unchecked")
    void 추가_등록의_모드는_첫_등록_모드를_바꾸지_못한다() {
        mockTable("CDC.T1", true, true);
        service.register(srcId, tgtId, List.of(spec("CDC.T1")), "INITIAL");
        mockTable("CDC.T2", true, true);
        service.register(srcId, tgtId, List.of(spec("CDC.T2")), "NO_DATA");

        ArgumentCaptor<Map<String, String>> vars = ArgumentCaptor.forClass(Map.class);
        verify(deploy, org.mockito.Mockito.times(2)).deploy(eq("source-oracle"), vars.capture());
        // 두 번째 배포도 첫 등록(INITIAL) 기준
        assertThat(vars.getAllValues().get(1)).containsEntry("snapshot_mode", "initial");
    }

    @Test
    void 잘못된_snapshotMode는_거부한다() {
        mockTable("CDC.T1", true, true);
        assertThatThrownBy(() -> service.register(srcId, tgtId, List.of(spec("CDC.T1")), "ALWAYS"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("snapshotMode");
    }

    @Test
    void 정지는_해당_테이블_jdbc_sink만_pause한다() {
        mockTable("CDC.T1", true, true);
        long id = service.register(srcId, tgtId, List.of(spec("CDC.T1"))).get(0).id();

        service.pause(id);
        verify(deploy).pauseConnector("dz-jdbc-sink-dz-cdc_t1");
        service.resume(id);
        verify(deploy).resumeConnector("dz-jdbc-sink-dz-cdc_t1");
    }

    @Test
    void 삭제하면_메타데이터와_테이블별_커넥터가_지워지고_남은_목록으로_재배포한다() {
        mockTable("CDC.T1", true, true);
        mockTable("CDC.T2", true, true);
        var all = service.register(srcId, tgtId, List.of(spec("CDC.T1"), spec("CDC.T2")));
        long t1 = all.stream().filter(t -> t.tableName().equals("T1")).findFirst().orElseThrow().id();

        var remaining = service.unregister(t1, false);

        assertThat(remaining).extracting(RegisteredTable::tableName).containsExactly("T2");
        assertThat(service.mappings(t1)).isEmpty();
        verify(deploy).deleteConnector("dz-jdbc-sink-dz-cdc_t1");
        // 기본은 changelog 보존
        verify(changelog, org.mockito.Mockito.never())
                .dropChangelogTable(anyString(), anyString(), anyString(), org.mockito.ArgumentMatchers.anyBoolean());
    }

    @Test
    void 마지막_테이블_삭제면_전역_커넥터도_지우고_dropChangelog면_S3까지_지운다() {
        mockTable("CDC.T1", true, true);
        long id = service.register(srcId, tgtId, List.of(spec("CDC.T1"))).get(0).id();

        var remaining = service.unregister(id, true);

        assertThat(remaining).isEmpty();
        verify(deploy).deleteConnector("dz-source-dz");
        verify(deploy).deleteConnector("dz-iceberg-dz");
        verify(changelog).dropChangelogTable("dz", "CDC", "T1", true);
    }

    @Test
    @SuppressWarnings("unchecked")
    void 등록하면_changelog_테이블_사전_생성_후_iceberg_sink가_배선된다() {
        mockTable("CDC.T1", true, true);
        service.register(srcId, tgtId, List.of(spec("CDC.T1")));

        verify(changelog).ensureChangelogTable("dz", "CDC", "T1");
        ArgumentCaptor<Map<String, String>> vars = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<Map<String, String>> extra = ArgumentCaptor.forClass(Map.class);
        verify(deploy).deploy(eq("iceberg-sink"), vars.capture(), extra.capture());
        assertThat(vars.getValue())
                .containsEntry("connector_name", "dz-iceberg-dz")
                .containsEntry("iceberg_tables", "changelog.cdc_t1");
        // route-regex는 토픽 이름 정확 일치 (5.1절) — 테이블명만 보던 종전 방식에서 전환
        // 카탈로그 접속 정보(iceberg.catalog.*)는 템플릿에서 빠지고 여기서 주입된다(TODO ③) —
        // minio 프로파일(테스트 application.yml)에서 종전 템플릿 하드코딩 값과 동일해야 한다
        assertThat(extra.getValue())
                .containsEntry("iceberg.table.changelog.cdc_t1.route-regex", "^\\Qdz.CDC.T1\\E$")
                .containsEntry("iceberg.catalog.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
                .containsEntry("iceberg.catalog.uri", "jdbc:postgresql://localhost:5433/iceberg_catalog")
                .containsEntry("iceberg.catalog.jdbc.user", "deltazium")
                .containsEntry("iceberg.catalog.jdbc.password", "deltazium")
                .containsEntry("iceberg.catalog.warehouse", "s3://deltazium-warehouse/warehouse")
                .containsEntry("iceberg.catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .containsEntry("iceberg.catalog.s3.endpoint", "http://localhost:9010")
                .containsEntry("iceberg.catalog.s3.path-style-access", "true")
                .containsEntry("iceberg.catalog.s3.access-key-id", "deltazium")
                .containsEntry("iceberg.catalog.s3.secret-access-key", "deltazium123")
                .containsEntry("iceberg.catalog.client.region", "us-east-1");
    }

    @Test
    @SuppressWarnings("unchecked")
    void 소스별로_독립_배포된다_다른_소스는_영향받지_않는다() {
        long pgSrcId = connections.create(new DbConnection(null, "pgsrc", "POSTGRESQL", "SOURCE",
                "pghost", 5432, "cdc", "dz_capture", "pw", "pgsrc")).id();

        mockTable("CDC.T1", true, true);
        mockPgTable("cdc_src.orders", true, true);

        service.register(srcId, tgtId, List.of(spec("CDC.T1")));
        service.register(pgSrcId, tgtId, List.of(spec("cdc_src.orders")));

        // deployConnectors()는 등록마다 등록된 소스 전부를 재배포한다(기존 단일 소스 시절 패턴과
        // 동일 — Connect REST upsert라 안전) — source-oracle은 두 번째 register()에서도 다시
        // 배포되고, source-postgresql은 그때 처음 배포된다.
        ArgumentCaptor<Map<String, String>> oracleVars = ArgumentCaptor.forClass(Map.class);
        verify(deploy, org.mockito.Mockito.times(2)).deploy(eq("source-oracle"), oracleVars.capture());
        assertThat(oracleVars.getValue()).containsEntry("connector_name", "dz-source-dz");

        ArgumentCaptor<Map<String, String>> pgVars = ArgumentCaptor.forClass(Map.class);
        verify(deploy).deploy(eq("source-postgresql"), pgVars.capture());
        assertThat(pgVars.getValue()).containsEntry("connector_name", "dz-source-pgsrc")
                .containsEntry("table_include_list", "cdc_src.orders");

        verify(deploy, org.mockito.Mockito.times(2)).deploy(eq("iceberg-sink"),
                org.mockito.ArgumentMatchers.argThat(m -> "dz-iceberg-dz".equals(m.get("connector_name"))),
                any());
        verify(deploy).deploy(eq("iceberg-sink"),
                org.mockito.ArgumentMatchers.argThat(m -> "dz-iceberg-pgsrc".equals(m.get("connector_name"))),
                any());
    }

    @Test
    void 소스의_마지막_테이블_해제는_그_소스의_커넥터만_지운다() {
        long pgSrcId = connections.create(new DbConnection(null, "pgsrc2", "POSTGRESQL", "SOURCE",
                "pghost", 5432, "cdc", "dz_capture", "pw", "pgsrc2")).id();

        mockTable("CDC.T1", true, true);
        mockPgTable("cdc_src.orders", true, true);
        long oracleId = service.register(srcId, tgtId, List.of(spec("CDC.T1"))).stream()
                .filter(t -> t.tableName().equals("T1")).findFirst().orElseThrow().id();
        service.register(pgSrcId, tgtId, List.of(spec("cdc_src.orders")));

        // Oracle 소스의 유일한 테이블을 해제 — PostgreSQL 소스 커넥터는 절대 건드리지 않는다
        service.unregister(oracleId, false);

        verify(deploy).deleteConnector("dz-source-dz");
        verify(deploy).deleteConnector("dz-iceberg-dz");
        verify(deploy, org.mockito.Mockito.never()).deleteConnector("dz-source-pgsrc2");
        verify(deploy, org.mockito.Mockito.never()).deleteConnector("dz-iceberg-pgsrc2");
    }

}
