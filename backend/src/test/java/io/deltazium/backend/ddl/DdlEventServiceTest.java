package io.deltazium.backend.ddl;

import io.deltazium.backend.connect.ConnectClient;
import io.deltazium.backend.registration.RegisteredTableRepository;
import io.deltazium.backend.registration.RegistrationService;
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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
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
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ②: jdbc-sink 커넥터명에 소스 topicPrefix가
 * |                          | 들어가 소스 커넥션에 명시 topicPrefix("dz")를 지정
 * --------------------------------------------------
 * 26. 09. 27.       | 최남희  | 테이블별 DDL 반영 정책(architecture.md 7절 개정) 테스트 추가 —
 * |                          | 승인의 sink 재개 통합, skip(ADD/DROP 인식·재배포 mock 검증,
 * |                          | 인식 불가 폴백), AUTO 정책 자동 적용 성공·실패 폴백·초안 없음 폴백.
 * |                          | RegistrationService는 mock(skip의 매핑·재배포 호출 검증 전용)
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

    @MockitoBean
    RegistrationService registration;

    long sourceId;
    long targetId;
    long tableId;

    @BeforeEach
    void setUp() {
        sourceId = connections.create(new DbConnection(null, "dz", "ORACLE", "SOURCE",
                "h", 1521, "SRC", "u", "p", "dz")).id();
        targetId = connections.create(new DbConnection(null, "t", "ORACLE", "TARGET",
                "h", 1521, "TGT", "u", "p")).id();
        tableId = registrations.insert("CDC", "AUTO_100", sourceId, targetId, null, null);
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
    void 승인하면_해당_테이블_jdbc_sink를_재개한다() {
        service.approve(eventId());

        verify(connect).resumeAfterDdl("dz-jdbc-sink-dz-cdc_auto_100");
    }

    @Test
    void 건너뛰면_ADD_COLUMN을_비활성_매핑으로_추가하고_재배포한_뒤_재개한다() {
        DdlEvent result = service.skip(eventId());

        verify(registration).addDisabledColumn(tableId, "X");
        verify(connect).resumeAfterDdl("dz-jdbc-sink-dz-cdc_auto_100");
        assertThat(result.state()).isEqualTo("SKIPPED");
        assertThat(result.note()).contains("X");
    }

    @Test
    void 건너뛰면_DROP_COLUMN_매핑을_비활성화하고_재배포한_뒤_재개한다() {
        events.insertIfAbsent(11, 2L, "101", "CDC", "AUTO_100",
                "ALTER TABLE CDC.AUTO_100 DROP (Y)", "DETECTED");
        long dropId = events.findAll().stream()
                .filter(e -> e.ddlText().contains("DROP")).findFirst().orElseThrow().id();

        DdlEvent result = service.skip(dropId);

        verify(registration).disableColumn(tableId, "Y");
        assertThat(result.state()).isEqualTo("SKIPPED");
    }

    @Test
    void 인식_불가_DDL은_건너뛰면_재배포_없이_재개만_한다() {
        events.insertIfAbsent(12, 3L, "102", "CDC", "AUTO_100",
                "ALTER TABLE CDC.AUTO_100 MODIFY (X NUMBER)", "DETECTED");
        long modifyId = events.findAll().stream()
                .filter(e -> e.ddlText().contains("MODIFY")).findFirst().orElseThrow().id();

        DdlEvent result = service.skip(modifyId);

        verify(registration, never()).addDisabledColumn(anyLong(), anyString());
        verify(registration, never()).disableColumn(anyLong(), anyString());
        verify(connect).resumeAfterDdl("dz-jdbc-sink-dz-cdc_auto_100");
        assertThat(result.state()).isEqualTo("SKIPPED");
        assertThat(result.note()).contains("재개만");
    }

    @Test
    void 거부하면_해당_테이블의_jdbc_sink_커넥터가_pause된다() {
        DdlEvent result = service.reject(eventId());

        verify(connect).pause("dz-jdbc-sink-dz-cdc_auto_100");
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

    // ── DDL 반영 정책 = AUTO (handleNewEvent, architecture.md 7절 개정) ──

    private long autoTableId() {
        return registrations.insert("CDC", "AUTO_200", sourceId, targetId, null, null, "INITIAL", "AUTO");
    }

    @Test
    void AUTO_정책이면_감지_즉시_타깃에_적용하고_재개한_뒤_AUTO_APPLIED가_된다() {
        autoTableId();
        long id = events.insertFingerprintEvent(System.currentTimeMillis(), "CDC", "AUTO_200",
                "ALTER TABLE \"TGT\".\"AUTO_200\" ADD (\"X\" NUMBER)", "DETECTED", "추가: X(int32)");

        service.handleNewEvent(id);

        verify(executor).execute(any(), eq("ALTER TABLE \"TGT\".\"AUTO_200\" ADD (\"X\" NUMBER)"));
        verify(connect).resumeAfterDdl("dz-jdbc-sink-dz-cdc_auto_200");
        DdlEvent result = events.findById(id).orElseThrow();
        assertThat(result.state()).isEqualTo("AUTO_APPLIED");
    }

    @Test
    void AUTO_정책이어도_MANUAL_테이블은_건드리지_않는다() {
        // setUp의 CDC.AUTO_100은 기본 MANUAL 정책 — DETECTED 그대로 남아야 한다.
        service.handleNewEvent(eventId());

        verify(executor, never()).execute(any(), anyString());
        assertThat(events.findById(eventId()).orElseThrow().state()).isEqualTo("DETECTED");
    }

    @Test
    void AUTO_정책에서_실행_실패시_MANUAL로_폴백한다() {
        autoTableId();
        doThrow(new IllegalStateException("타깃 접속 실패")).when(executor).execute(any(), anyString());
        long id = events.insertFingerprintEvent(System.currentTimeMillis(), "CDC", "AUTO_200",
                "ALTER TABLE \"TGT\".\"AUTO_200\" ADD (\"X\" NUMBER)", "DETECTED", "추가: X(int32)");

        service.handleNewEvent(id);

        verify(connect, never()).resumeAfterDdl(anyString());
        DdlEvent result = events.findById(id).orElseThrow();
        assertThat(result.state()).isEqualTo("DETECTED");
        assertThat(result.note()).contains("자동 적용 실패");
    }

    @Test
    void AUTO_정책에서_지문_경로_타입변경으로_초안이_없으면_확인후로_전환한다() {
        autoTableId();
        long id = events.insertFingerprintEvent(System.currentTimeMillis(), "CDC", "AUTO_200",
                "", "SNAPSHOT", "타입변경: X int32→int64 (자동 초안 없음 — 확인 후 수동 DDL 필요)");

        service.handleNewEvent(id);

        verify(executor, never()).execute(any(), anyString());
        verify(connect, never()).resumeAfterDdl(anyString());
        DdlEvent result = events.findById(id).orElseThrow();
        assertThat(result.state()).isEqualTo("DETECTED");
        assertThat(result.note()).contains("확인 후 처리 필요");
    }
}
