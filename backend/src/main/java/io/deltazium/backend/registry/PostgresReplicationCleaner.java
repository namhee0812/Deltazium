package io.deltazium.backend.registry;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.springframework.stereotype.Component;

/**
 * 파일명 : PostgresReplicationCleaner.java
 * 작성일자 : 26. 09. 23.
 * 작성자 : 최남희
 * 설명 : PostgreSQL 소스의 논리 복제 슬롯·publication 정리 — 결함 2 수정
 * (등록 해제로 소스의 마지막 테이블이 사라져 source 커넥터를 지운 뒤에도 슬롯·publication이
 * inactive로 남아 WAL 정리를 막던 문제, architecture.md 8절). RegistrationService.unregister가
 * 소스 커넥터 삭제 직후 호출한다 — 이름 규칙은 ConnectorNames.replicationSlot/Publication과
 * 공유(source-postgresql.json.tmpl과 동일).
 * <p>
 * 슬롯이 active(커넥터 task가 아직 완전히 종료되지 않음)면 1초 간격 최대
 * {@value #MAX_ATTEMPTS}회 재시도 후에도 inactive로 바뀌지 않으면 드롭을 포기하고
 * {@link Result#stillActive()}=true로 보고한다 — active 슬롯은 DROP이 거부되므로 억지로
 * 재시도를 늘리기보다 호출측이 이벤트로 남겨 운영자가 확인하게 한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 23.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@Component
public class PostgresReplicationCleaner {

    private static final int MAX_ATTEMPTS = 10;
    private static final long DEFAULT_RETRY_INTERVAL_MS = 1000;

    /**
     * @param slotDropped        슬롯을 지웠거나(또는 애초에 없어서) 정리가 끝났으면 true
     * @param publicationDropped publication DROP IF EXISTS 실행 여부(존재 유무와 무관하게 true)
     * @param stillActive        재시도 끝에도 슬롯이 active라 드롭하지 못했으면 true — 수동 정리 필요
     */
    public record Result(boolean slotDropped, boolean publicationDropped, boolean stillActive) {
    }

    private final long retryIntervalMs;

    public PostgresReplicationCleaner() {
        this(DEFAULT_RETRY_INTERVAL_MS);
    }

    /** 테스트 전용 — 재시도 간격을 줄여 단위 테스트가 실제로 대기하지 않게 한다. */
    PostgresReplicationCleaner(long retryIntervalMs) {
        this.retryIntervalMs = retryIntervalMs;
    }

    /** 소스 DB에 접속해 슬롯·publication을 정리한다. 접속·SQL 실패는 그대로 던진다 —
     * 실패해도 등록 해제 자체는 성공으로 두는 판단은 호출측(RegistrationService)의 몫. */
    public Result cleanup(DbConnection source, String slotName, String publicationName) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", source.username());
        props.setProperty("password", source.password());
        props.setProperty("loginTimeout", "5");
        props.setProperty("connectTimeout", "5");
        try (Connection conn = DriverManager.getConnection(source.jdbcUrl(), props)) {
            return cleanup(conn, slotName, publicationName);
        }
    }

    /** 이미 연 커넥션으로 정리 — SQL·재시도 로직 단위 테스트(JDBC mock) 전용 진입점. */
    Result cleanup(Connection conn, String slotName, String publicationName) throws SQLException {
        Boolean active = queryActive(conn, slotName);
        for (int attempt = 0; active != null && active && attempt < MAX_ATTEMPTS; attempt++) {
            sleep();
            active = queryActive(conn, slotName);
        }

        boolean slotDropped;
        boolean stillActive;
        if (active == null) {
            // 슬롯이 이미 없음 — 정리할 것 없이 완료로 취급
            slotDropped = true;
            stillActive = false;
        } else if (!active) {
            dropSlot(conn, slotName);
            slotDropped = true;
            stillActive = false;
        } else {
            slotDropped = false;
            stillActive = true;
        }

        boolean publicationDropped = dropPublication(conn, publicationName);
        return new Result(slotDropped, publicationDropped, stillActive);
    }

    /** @return 슬롯 존재 시 active 여부, 슬롯이 없으면 null */
    private Boolean queryActive(Connection conn, String slotName) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT active FROM pg_replication_slots WHERE slot_name = ?")) {
            ps.setString(1, slotName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getBoolean("active") : null;
            }
        }
    }

    private void dropSlot(Connection conn, String slotName) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT pg_drop_replication_slot(?)")) {
            ps.setString(1, slotName);
            ps.execute();
        }
    }

    /** DROP PUBLICATION IF EXISTS — 존재 유무와 무관하게 성공. 식별자는 따옴표로 감싼다
     * (architecture.md 8절 — 저장값이 이미 unquoted 폴딩 규칙으로 만들어져 있다). */
    private boolean dropPublication(Connection conn, String publicationName) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("DROP PUBLICATION IF EXISTS \"" + publicationName + "\"");
        }
        return true;
    }

    private void sleep() {
        try {
            Thread.sleep(retryIntervalMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
