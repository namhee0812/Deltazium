package io.deltazium.backend.registry;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.springframework.stereotype.Component;

/** Oracle 연결 확인. 성공 시 DB 버전 문자열, 실패 시 SQLException 메시지를 돌려준다. */
@Component
public class OracleConnectionTester {

    public record Result(boolean ok, String message) {
    }

    public Result test(DbConnection c) {
        Properties props = new Properties();
        props.setProperty("user", c.username());
        props.setProperty("password", c.password());
        // 접속 불가 호스트에서 무한 대기하지 않도록 타임아웃 고정 (ms)
        props.setProperty("oracle.net.CONNECT_TIMEOUT", "5000");
        try (Connection conn = DriverManager.getConnection(c.jdbcUrl(), props)) {
            return new Result(true, conn.getMetaData().getDatabaseProductVersion());
        } catch (SQLException e) {
            return new Result(false, e.getMessage() == null ? e.toString() : e.getMessage().strip());
        }
    }
}
