package io.deltazium.backend.capture;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import io.deltazium.backend.registry.DbConnection;
import org.springframework.stereotype.Component;

/**
 * 파일명 : TargetTableGate.java
 * 작성일자 : 26. 08. 05.
 * 작성자 : 최남희
 * 설명 : truncate 재구축의 타깃 DB 게이트 — 권한 사전 점검(스키마 소유 또는
 * DROP ANY TABLE), 행 수 조회(비워짐 검증), TRUNCATE 실행.
 * 오케스트레이터에서 분리해 테스트 시 목으로 대체한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 05.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@Component
public class TargetTableGate {

    /**
     * TRUNCATE 가능 여부 — 모든 타깃 스키마가 접속 계정 소유이거나,
     * 아니면 session_privs에 DROP ANY TABLE이 있어야 한다.
     */
    public boolean canTruncate(DbConnection target, List<String> schemas) {
        boolean allOwned = schemas.stream()
                .allMatch(s -> s.equalsIgnoreCase(target.username()));
        if (allOwned) {
            return true;
        }
        try (Connection db = open(target);
             Statement st = db.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT COUNT(*) FROM session_privs WHERE privilege = 'DROP ANY TABLE'")) {
            rs.next();
            return rs.getLong(1) > 0;
        } catch (SQLException e) {
            throw new IllegalStateException("권한 점검 실패: " + message(e), e);
        }
    }

    public long rowCount(DbConnection target, String qualifiedTable) {
        try (Connection db = open(target);
             Statement st = db.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + qualifiedTable)) {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException("행 수 조회 실패(" + qualifiedTable + "): " + message(e), e);
        }
    }

    public void truncate(DbConnection target, String qualifiedTable) {
        try (Connection db = open(target);
             Statement st = db.createStatement()) {
            st.execute("TRUNCATE TABLE " + qualifiedTable);
        } catch (SQLException e) {
            throw new IllegalStateException("TRUNCATE 실패(" + qualifiedTable + "): " + message(e), e);
        }
    }

    private Connection open(DbConnection conn) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", conn.username());
        props.setProperty("password", conn.password());
        props.setProperty("oracle.net.CONNECT_TIMEOUT", "5000");
        return DriverManager.getConnection(conn.jdbcUrl(), props);
    }

    private static String message(SQLException e) {
        String m = e.getMessage();
        return m == null ? e.toString() : m.strip().toUpperCase(Locale.ROOT).startsWith("ORA-")
                ? m.strip().split("\n")[0] : m.strip();
    }
}
