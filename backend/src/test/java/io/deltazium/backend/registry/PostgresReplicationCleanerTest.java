package io.deltazium.backend.registry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 파일명 : PostgresReplicationCleanerTest.java
 * 작성일자 : 26. 09. 23.
 * 작성자 : 최남희
 * 설명 : PG 복제 슬롯·publication 정리 SQL·재시도 로직 단위 테스트(결함 2) — JDBC mock으로
 * 실제 접속 없이 검증한다. 재시도 간격은 테스트 전용 생성자로 0ms로 줄여 대기하지 않는다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 23.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
class PostgresReplicationCleanerTest {

    private static final String SELECT_ACTIVE_SQL = "SELECT active FROM pg_replication_slots WHERE slot_name = ?";
    private static final String DROP_SLOT_SQL = "SELECT pg_drop_replication_slot(?)";

    private final PostgresReplicationCleaner cleaner = new PostgresReplicationCleaner(0);

    private Connection conn;
    private PreparedStatement selectPs;
    private ResultSet rs;
    private PreparedStatement dropSlotPs;
    private Statement dropPubSt;

    private void mockConnection() throws SQLException {
        conn = mock(Connection.class);
        selectPs = mock(PreparedStatement.class);
        rs = mock(ResultSet.class);
        dropSlotPs = mock(PreparedStatement.class);
        dropPubSt = mock(Statement.class);

        when(conn.prepareStatement(eq(SELECT_ACTIVE_SQL))).thenReturn(selectPs);
        when(selectPs.executeQuery()).thenReturn(rs);
        when(conn.prepareStatement(eq(DROP_SLOT_SQL))).thenReturn(dropSlotPs);
        when(conn.createStatement()).thenReturn(dropPubSt);
    }

    @Test
    void 슬롯이_없으면_드롭_없이_완료로_취급하고_publication만_지운다() throws SQLException {
        mockConnection();
        when(rs.next()).thenReturn(false);

        PostgresReplicationCleaner.Result result = cleaner.cleanup(conn, "dz_pgsrc", "dz_pgsrc");

        assertThat(result.slotDropped()).isTrue();
        assertThat(result.stillActive()).isFalse();
        assertThat(result.publicationDropped()).isTrue();
        verify(conn, never()).prepareStatement(eq(DROP_SLOT_SQL));
        verify(dropPubSt).execute(eq("DROP PUBLICATION IF EXISTS \"dz_pgsrc\""));
    }

    @Test
    void 슬롯이_inactive면_재시도_없이_즉시_드롭한다() throws SQLException {
        mockConnection();
        when(rs.next()).thenReturn(true);
        when(rs.getBoolean("active")).thenReturn(false);

        PostgresReplicationCleaner.Result result = cleaner.cleanup(conn, "dz_pgsrc", "dz_pgsrc");

        assertThat(result.slotDropped()).isTrue();
        assertThat(result.stillActive()).isFalse();
        verify(conn, times(1)).prepareStatement(eq(SELECT_ACTIVE_SQL));
        verify(dropSlotPs).setString(1, "dz_pgsrc");
        verify(dropSlotPs).execute();
    }

    @Test
    void 슬롯이_active면_재시도하다가_inactive가_되면_드롭한다() throws SQLException {
        mockConnection();
        when(rs.next()).thenReturn(true);
        when(rs.getBoolean("active")).thenReturn(true, true, false);

        PostgresReplicationCleaner.Result result = cleaner.cleanup(conn, "dz_pgsrc", "dz_pgsrc");

        assertThat(result.slotDropped()).isTrue();
        assertThat(result.stillActive()).isFalse();
        // 최초 조회 1회 + 재시도 2회 = 3회
        verify(conn, times(3)).prepareStatement(eq(SELECT_ACTIVE_SQL));
        verify(dropSlotPs).execute();
    }

    @Test
    void 최대_재시도_후에도_active면_드롭을_포기하고_stillActive를_보고한다() throws SQLException {
        mockConnection();
        when(rs.next()).thenReturn(true);
        when(rs.getBoolean("active")).thenReturn(true);

        PostgresReplicationCleaner.Result result = cleaner.cleanup(conn, "dz_pgsrc", "dz_pgsrc");

        assertThat(result.slotDropped()).isFalse();
        assertThat(result.stillActive()).isTrue();
        // 최초 조회 1회 + 재시도 10회 = 11회, 드롭은 시도하지 않음
        verify(conn, times(11)).prepareStatement(eq(SELECT_ACTIVE_SQL));
        verify(conn, never()).prepareStatement(eq(DROP_SLOT_SQL));
        // active여도 publication은 정리한다
        assertThat(result.publicationDropped()).isTrue();
    }

    // cleanup(DbConnection,...) 공개 오버로드는 접속 속성 구성 후 cleanup(Connection,...)에
    // 위임하기만 한다 — 접속 실패 시 SQLException을 그대로 던지는 계약은 시그니처로 보장되고,
    // 실제 네트워크 접속을 쓰는 왕복 검증은 단위 테스트 범위 밖(리허설/E2E)이라 여기서는 다루지
    // 않는다. 실패를 WARN 이벤트로 남기는 책임은 RegistrationService에 있다.

    @Test
    void publication_삭제는_따옴표_식별자와_IF_EXISTS를_쓴다() throws SQLException {
        mockConnection();
        when(rs.next()).thenReturn(false);

        cleaner.cleanup(conn, "dz_other", "dz_other-prefix");

        verify(dropPubSt).execute(eq("DROP PUBLICATION IF EXISTS \"dz_other-prefix\""));
    }
}
