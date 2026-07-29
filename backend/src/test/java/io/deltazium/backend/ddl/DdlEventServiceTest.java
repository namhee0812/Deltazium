package io.deltazium.backend.ddl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.deltazium.backend.connect.ConnectClient;
import io.deltazium.backend.registration.RegisteredTableRepository;
import io.deltazium.backend.registry.DbConnection;
import io.deltazium.backend.registry.DbConnectionRepository;
import io.deltazium.backend.registry.DbConnectionService;
import io.deltazium.backend.registry.OracleConnectionTester;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import com.fasterxml.jackson.databind.JsonNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@JdbcTest
@Import({DdlEventRepository.class, DdlEventService.class, RegisteredTableRepository.class,
        DbConnectionRepository.class, DbConnectionService.class})
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
        registrations.insert("CDC", "AUTO_100", sourceId, targetId);
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
    void 거부하면_jdbc_sink_topics에서_해당_토픽이_빠진다() throws Exception {
        JsonNode config = new ObjectMapper().readTree(
                "{\"connector.class\":\"x\",\"topics\":\"dz.CDC.AUTO_100,dz.CDC.OTHER\"}");
        when(connect.getConfig("dz-jdbc-sink")).thenReturn(config);

        DdlEvent result = service.reject(eventId());

        ArgumentCaptor<JsonNode> updated = ArgumentCaptor.forClass(JsonNode.class);
        verify(connect).upsert(eq("dz-jdbc-sink"), updated.capture());
        assertThat(updated.getValue().get("topics").asText()).isEqualTo("dz.CDC.OTHER");
        assertThat(result.state()).isEqualTo("REJECTED");
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
    void 같은_offset은_중복_적재되지_않는다() {
        assertThat(events.insertIfAbsent(10, 1L, "100", "CDC", "AUTO_100", "dup", "DETECTED"))
                .isFalse();
        assertThat(events.findAll()).hasSize(1);
    }
}
