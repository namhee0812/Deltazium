package io.deltazium.backend;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ScriptUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * 파일명 : SchemaMigrationIdempotencyTest.java
 * 작성일자 : 26. 09. 07.
 * 작성자 : 최남희
 * 설명 : schema.sql 멱등성 검증 — 신선한 DB에 두 번 연속 적용해도 예외가 없어야 한다
 * (backend는 기동마다 sql.init.mode=always로 이 파일을 재실행한다). 다중 소스·다중 타깃 ②의
 * 마이그레이션(topic_prefix 추가, registered_tables 등록 키 전환, ddl_events.origin·
 * kafka_offset nullable화)이 실제로 두 번째 실행에서도 깨지지 않는지가 이 테스트의 핵심.
 * H2(PostgreSQL 호환 모드)를 이 테스트 전용 인스턴스로 새로 띄운다 — Spring 컨텍스트의
 * 공유 인메모리 DB(deltazium-test)와 이름을 다르게 해 다른 테스트와 스키마 적용 시점이
 * 섞이지 않게 한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 최초 생성 — 다중 소스·다중 타깃 ②
 * --------------------------------------------------
 */
class SchemaMigrationIdempotencyTest {

    private static final String URL =
            "jdbc:h2:mem:deltazium-migration-test;MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
    private static final String URL2 =
            "jdbc:h2:mem:deltazium-migration-test-2;MODE=PostgreSQL;DB_CLOSE_DELAY=-1";

    @Test
    void schema_sql은_연속_두_번_적용해도_예외가_없다() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL, "sa", "")) {
            assertThatCode(() -> {
                runSchemaSqlTwice(conn);
            }).doesNotThrowAnyException();

            assertThat(columnExists(conn, "DB_CONNECTIONS", "TOPIC_PREFIX")).isTrue();
            assertThat(columnExists(conn, "DDL_EVENTS", "ORIGIN")).isTrue();
            assertThat(columnExists(conn, "REGISTERED_TABLES", "SCHEMA_FINGERPRINT")).isTrue();
            assertThat(columnExists(conn, "REGISTERED_TABLES", "SCHEMA_FIELDS_JSON")).isTrue();
        } finally {
            dropAll();
        }
    }

    @Test
    void 등록_키는_소스_스키마_테이블_복합_유니크로_동작한다() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL2, "sa", "")) {
            runSchemaSqlTwice(conn);
            try (Statement st = conn.createStatement()) {
                // 같은 소스에 같은 schema.table 재등록은 거부돼야 한다
                st.execute("INSERT INTO db_connections (name, db_type, role, host, port, "
                        + "database_name, username, password, topic_prefix) VALUES "
                        + "('s1','ORACLE','SOURCE','h',1521,'DB','u','p','dz')");
                st.execute("INSERT INTO db_connections (name, db_type, role, host, port, "
                        + "database_name, username, password) VALUES "
                        + "('t1','ORACLE','TARGET','h',1521,'DB','u','p')");
                st.execute("INSERT INTO registered_tables (schema_name, table_name, "
                        + "source_connection_id, target_connection_id) VALUES ('CDC','T1',1,2)");
                // 다른 소스 커넥션이면 같은 schema.table도 허용돼야 한다(다중 소스 전제)
                st.execute("INSERT INTO db_connections (name, db_type, role, host, port, "
                        + "database_name, username, password, topic_prefix) VALUES "
                        + "('s2','POSTGRESQL','SOURCE','h',5432,'DB','u','p','pg')");
                assertThatCode(() -> st.execute("INSERT INTO registered_tables (schema_name, table_name, "
                        + "source_connection_id, target_connection_id) VALUES ('CDC','T1',3,2)"))
                        .doesNotThrowAnyException();
                // 같은 소스(id=1)에 같은 schema.table 재등록은 유니크 위반으로 거부돼야 한다
                assertThatCode(() -> st.execute("INSERT INTO registered_tables (schema_name, table_name, "
                        + "source_connection_id, target_connection_id) VALUES ('CDC','T1',1,2)"))
                        .isInstanceOf(SQLException.class);
            }
        } finally {
            try (Connection conn = DriverManager.getConnection(URL2, "sa", "")) {
                conn.createStatement().execute("DROP ALL OBJECTS");
            } catch (SQLException ignored) {
                // 정리 실패는 무시 — 각 URL이 프로세스 종료 시 함께 사라진다
            }
        }
    }

    private static void runSchemaSqlTwice(Connection conn) throws SQLException {
        ScriptUtils.executeSqlScript(conn, new ClassPathResource("schema.sql"));
        ScriptUtils.executeSqlScript(conn, new ClassPathResource("schema.sql"));
    }

    private static boolean columnExists(Connection conn, String table, String column) throws SQLException {
        try (ResultSet rs = conn.getMetaData().getColumns(null, null, table, column)) {
            return rs.next();
        }
    }

    private static void dropAll() {
        try (Connection conn = DriverManager.getConnection(URL, "sa", "")) {
            conn.createStatement().execute("DROP ALL OBJECTS");
        } catch (SQLException ignored) {
            // 정리 실패는 무시 — DB_CLOSE_DELAY=-1인 인메모리 DB라 프로세스 종료 시 함께 사라진다
        }
    }
}
