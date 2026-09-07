package io.deltazium.backend.registry;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.springframework.stereotype.Component;

/**
 * 파일명 : OracleConnectionTester.java
 * 작성일자 : 26. 07. 25.
 * 작성자 : 최남희
 * 설명 : DB 연결 확인 — dbType에 맞는 JDBC 드라이버는 DriverManager가 URL 스킴으로 자동
 * 선택한다(Oracle/PostgreSQL 드라이버 모두 클래스패스에 있음, 2026-09-07). 클래스명은
 * 최초 구현 당시(Oracle 전용) 흔적이나 지금은 dbType 무관 범용 테스터다.
 * 성공 시 DB 버전 문자열, 실패 시 SQLException 메시지를 돌려준다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 25.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ②: 접속 타임아웃 속성을 dbType별로
 * |                          | 분기(PostgreSQL은 loginTimeout) — Oracle 전용 속성은 PostgreSQL
 * |                          | 드라이버가 무시하므로 방치돼도 무해하지만 명시적으로 맞춘다.
 * --------------------------------------------------
 */
@Component
public class OracleConnectionTester {

    public record Result(boolean ok, String message) {
    }

    public Result test(DbConnection c) {
        Properties props = new Properties();
        props.setProperty("user", c.username());
        props.setProperty("password", c.password());
        // 접속 불가 호스트에서 무한 대기하지 않도록 타임아웃 고정 (초 단위 드라이버는 5, ms 단위는 5000)
        if ("POSTGRESQL".equalsIgnoreCase(c.dbType())) {
            props.setProperty("loginTimeout", "5");
            props.setProperty("connectTimeout", "5");
        } else {
            props.setProperty("oracle.net.CONNECT_TIMEOUT", "5000");
        }
        try (Connection conn = DriverManager.getConnection(c.jdbcUrl(), props)) {
            return new Result(true, conn.getMetaData().getDatabaseProductVersion());
        } catch (SQLException e) {
            return new Result(false, e.getMessage() == null ? e.toString() : e.getMessage().strip());
        }
    }
}
