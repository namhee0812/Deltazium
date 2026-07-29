package io.deltazium.backend.ddl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import io.deltazium.backend.registry.DbConnection;
import org.springframework.stereotype.Component;

/**
 * 승인된 DDL을 타깃 Oracle에 실행 (7절 2단계).
 * 사용자가 UI에서 [승인]을 눌렀을 때만 호출된다 — 다른 경로에서 부르지 말 것.
 */
@Component
public class TargetDdlExecutor {

    public void execute(DbConnection target, String ddl) {
        Properties props = new Properties();
        props.setProperty("user", target.username());
        props.setProperty("password", target.password());
        props.setProperty("oracle.net.CONNECT_TIMEOUT", "5000");
        try (Connection conn = DriverManager.getConnection(target.jdbcUrl(), props);
             Statement st = conn.createStatement()) {
            st.execute(ddl);
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "타깃 DDL 실행 실패: " + (e.getMessage() == null ? e.toString() : e.getMessage().strip()));
        }
    }
}
