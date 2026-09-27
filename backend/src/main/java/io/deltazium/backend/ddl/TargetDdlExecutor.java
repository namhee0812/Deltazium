package io.deltazium.backend.ddl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import io.deltazium.backend.registry.DbConnection;
import io.deltazium.backend.registry.DbType;
import org.springframework.stereotype.Component;

/**
 * 파일명 : TargetDdlExecutor.java
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : 승인된 DDL을 타깃 DB에 실행 (7절 2단계).
 * 사용자가 UI에서 [승인]을 눌렀을 때만 호출된다 — 다른 경로에서 부르지 말 것.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 09. 28.       | 최남희  | SingleStore 타깃 분기 추가(connectTimeout·socketTimeout ms) —
 * |                          | 기존 PostgreSQL 타깃은 이 파일에 분기가 없었다(Oracle 전용 속성을
 * |                          | 무해하게 무시하는 상태 그대로 유지, 이번 범위 밖의 별건 결함)
 * --------------------------------------------------
 */
@Component
public class TargetDdlExecutor {

    public void execute(DbConnection target, String ddl) {
        Properties props = new Properties();
        props.setProperty("user", target.username());
        props.setProperty("password", target.password());
        if (DbType.find(target.dbType()).orElse(DbType.ORACLE) == DbType.SINGLESTORE) {
            props.setProperty("connectTimeout", "5000");
            props.setProperty("socketTimeout", "5000");
        } else {
            props.setProperty("oracle.net.CONNECT_TIMEOUT", "5000");
        }
        try (Connection conn = DriverManager.getConnection(target.jdbcUrl(), props);
             Statement st = conn.createStatement()) {
            st.execute(ddl);
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "타깃 DDL 실행 실패: " + (e.getMessage() == null ? e.toString() : e.getMessage().strip()));
        }
    }
}
