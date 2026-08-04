package io.deltazium.backend.ddl;

import io.deltazium.backend.connect.ConnectClient;
import io.deltazium.backend.registration.RegisteredTableRepository;
import io.deltazium.backend.registry.DbConnection;
import io.deltazium.backend.registry.DbConnectionRepository;
import io.deltazium.backend.registry.DbConnectionService;
import io.deltazium.backend.registry.OracleConnectionTester;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mybatis.spring.boot.test.autoconfigure.MybatisTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.sql.init.SqlInitializationAutoConfiguration;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@MybatisTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ImportAutoConfiguration(SqlInitializationAutoConfiguration.class)
@Import({DdlEventService.class, DbConnectionService.class,
        io.deltazium.backend.events.TableEventService.class})
/**
 * 파일명 : DdlEventServiceTest.java
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : DDL 승인 워크플로(타깃 이름 치환 포함) 단위 테스트.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
class DdlEventServiceTest {

    @Autowired
    DdlEventService service;

    @Autowired
    DdlEventRepository events;

    @Autowired
    RegisteredTableRepository registrations;

    @Autowired
    DbConnectionService connections;

    @MockitoBean
    TargetDdlExecutor executor;

    @MockitoBean
    ConnectClient connect;

    @MockitoBean
    OracleConnectionTester tester;

    long targetId;

    @BeforeEach
    void setUp() {
        long sourceId = connections.create(new DbConnection(null, "s", "ORACLE", "SOURCE",
                "h", 1521, "SRC", "u", "p")).id();
        targetId = connections.create(new DbConnection(null, "t", "ORACLE", "TARGET",
                "h", 1521, "TGT", "u", "p")).id();
        registrations.insert("CDC", "AUTO_100", sourceId, targetId, null, null);
        events.insertIfAbsent(10, 1L, "100", "CDC", "AUTO_100",
                "ALTER TABLE CDC.AUTO_100 ADD (X NUMBER)", "DETECTED");
    }

    private long eventId() {
        return events.findAll().get(0).id();
    }

    @Test
    void 승인하면_타깃에_DDL을_실행하고_APPROVED가_된다() {
        DdlEvent result = service.approve(eventId());

        verify(executor).execute(any(), eq("ALTER TABLE CDC.AUTO_100 ADD (X NUMBER)"));
        assertThat(result.state()).isEqualTo("APPROVED");
        assertThat(result.decidedAt()).isNotNull();
    }

    @Test
    void 거부하면_해당_테이블의_jdbc_sink_커넥터가_pause된다() {
        DdlEvent result = service.reject(eventId());

        verify(connect).pause("dz-jdbc-sink-cdc_auto_100");
        assertThat(result.state()).isEqualTo("REJECTED");
        assertThat(result.note()).contains("apply 정지");
    }

    @Test
    void 등록되지_않은_테이블의_DDL은_승인_불가() {
        events.insertIfAbsent(11, 1L, "101", "HR", "UNKNOWN", "ALTER ...", "DETECTED");
        long id = events.findAll().stream()
                .filter(e -> "HR".equals(e.schemaName())).findFirst().orElseThrow().id();
        assertThatThrownBy(() -> service.approve(id))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("등록되지 않은");
    }

    @Test
    void SNAPSHOT_상태는_승인_대상이_아니다() {
        events.insertIfAbsent(12, 1L, "102", "CDC", "AUTO_100", "CREATE TABLE ...", "SNAPSHOT");
        long id = events.findAll().stream()
                .filter(e -> "SNAPSHOT".equals(e.state())).findFirst().orElseThrow().id();
        assertThatThrownBy(() -> service.approve(id))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("승인 대기 상태가 아니다");
    }

    @Test
    void 타깃_이름이_다르면_DDL을_치환해서_실행한다() {
        io.deltazium.backend.registration.RegisteredTable mapped =
                new io.deltazium.backend.registration.RegisteredTable(
                        9L, "CDC", "AUTO_100", 1, 2, "TGT_OWN", "AUTO_100_COPY");
        assertThat(DdlEventService.rewriteForTarget(
                "ALTER TABLE CDC.AUTO_100 ADD (X NUMBER)", mapped))
                .isEqualTo("ALTER TABLE \"TGT_OWN\".\"AUTO_100_COPY\" ADD (X NUMBER)");
        assertThat(DdlEventService.rewriteForTarget(
                "ALTER TABLE \"CDC\".\"AUTO_100\" DROP (Y)", mapped))
                .isEqualTo("ALTER TABLE \"TGT_OWN\".\"AUTO_100_COPY\" DROP (Y)");
        // 스키마 없이 실행된 DDL도 치환 (단어 경계 — IDX_AUTO_100 같은 이름은 보존)
        assertThat(DdlEventService.rewriteForTarget(
                "ALTER TABLE AUTO_100 ADD (Z NUMBER)", mapped))
                .isEqualTo("ALTER TABLE \"TGT_OWN\".\"AUTO_100_COPY\" ADD (Z NUMBER)");
        assertThat(DdlEventService.rewriteForTarget(
                "CREATE INDEX IDX_AUTO_100X ON CDC.AUTO_100(COL1)", mapped))
                .isEqualTo("CREATE INDEX IDX_AUTO_100X ON \"TGT_OWN\".\"AUTO_100_COPY\"(COL1)");
    }

    @Test
    void 타깃_이름이_같으면_원문_그대로_실행한다() {
        io.deltazium.backend.registration.RegisteredTable same =
                new io.deltazium.backend.registration.RegisteredTable(
                        9L, "CDC", "AUTO_100", 1, 2, null, null);
        String ddl = "ALTER TABLE CDC.AUTO_100 ADD (X NUMBER)";
        assertThat(DdlEventService.rewriteForTarget(ddl, same)).isSameAs(ddl);
    }

    @Test
    void 같은_offset은_중복_적재되지_않는다() {
        assertThat(events.insertIfAbsent(10, 1L, "100", "CDC", "AUTO_100", "dup", "DETECTED"))
                .isFalse();
        assertThat(events.findAll()).hasSize(1);
    }
}
