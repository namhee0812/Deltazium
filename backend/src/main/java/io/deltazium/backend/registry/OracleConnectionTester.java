package io.deltazium.backend.registry;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
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
 * 26. 09. 28.       | 최남희  | SingleStore(mysql-connector-j) 분기 추가 — connectTimeout·
 * |                          | socketTimeout(ms 단위, MySQL 드라이버 속성명)
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | Databricks 타깃 분기 — UID/PWD Properties(DatabricksSupport),
 * |                          | 접속 후 SHOW VOLUMES로 스테이징 Volume 존재 확인, Files API로
 * |                          | probe 파일 PUT/DELETE까지 해 쓰기 권한을 확인한다(접속은 되는데
 * |                          | Volume이 없거나 쓸 수 없는 경우를 등록 전에 잡는다)
 * --------------------------------------------------
 */
@Component
public class OracleConnectionTester {

    public record Result(boolean ok, String message) {
    }

    public Result test(DbConnection c) {
        if ("DATABRICKS".equalsIgnoreCase(c.dbType())) {
            return testDatabricks(c);
        }
        Properties props = new Properties();
        props.setProperty("user", c.username());
        props.setProperty("password", c.password());
        // 접속 불가 호스트에서 무한 대기하지 않도록 타임아웃 고정 (초 단위 드라이버는 5, ms 단위는 5000)
        if ("POSTGRESQL".equalsIgnoreCase(c.dbType())) {
            props.setProperty("loginTimeout", "5");
            props.setProperty("connectTimeout", "5");
        } else if ("SINGLESTORE".equalsIgnoreCase(c.dbType())) {
            props.setProperty("connectTimeout", "5000");
            props.setProperty("socketTimeout", "5000");
        } else {
            props.setProperty("oracle.net.CONNECT_TIMEOUT", "5000");
        }
        try (Connection conn = DriverManager.getConnection(c.jdbcUrl(), props)) {
            return new Result(true, conn.getMetaData().getDatabaseProductVersion());
        } catch (SQLException e) {
            return new Result(false, e.getMessage() == null ? e.toString() : e.getMessage().strip());
        }
    }

    /**
     * Databricks: (1) SQL warehouse 접속, (2) SHOW VOLUMES로 stagingVolume 존재, (3) Files API
     * probe 파일 PUT/DELETE(쓰기 권한). 어느 단계에서 실패했는지 메시지에 드러낸다.
     */
    private Result testDatabricks(DbConnection c) {
        Properties props;
        try {
            props = DatabricksSupport.props(c);
        } catch (IllegalStateException e) {
            return new Result(false, e.getMessage());
        }
        props.setProperty("socketTimeout", "60"); // 초 단위 — warehouse 콜드 스타트를 감안
        String version;
        String[] sv = c.stagingVolume().split("\\.", 2);
        try (Connection conn = DriverManager.getConnection(c.jdbcUrl(), props)) {
            version = conn.getMetaData().getDatabaseProductVersion();
            boolean found = false;
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SHOW VOLUMES IN `" + c.databaseName().replace("`", "``")
                         + "`.`" + sv[0].replace("`", "``") + "`")) {
                while (rs.next()) {
                    if (sv[1].equalsIgnoreCase(rs.getString("volume_name"))) {
                        found = true;
                    }
                }
            }
            if (!found) {
                return new Result(false, "접속은 되지만 스테이징 Volume이 없다: " + c.stagingVolume()
                        + " (catalog " + c.databaseName() + ") — CREATE VOLUME으로 먼저 만들 것");
            }
        } catch (SQLException e) {
            return new Result(false, e.getMessage() == null ? e.toString() : e.getMessage().strip());
        }
        String probeError = probeFilesApi(c);
        if (probeError != null) {
            return new Result(false, "접속·Volume은 확인됐지만 Volume에 쓸 수 없다: " + probeError);
        }
        return new Result(true, "Databricks " + version + " · Volume " + c.stagingVolume() + " 쓰기 확인");
    }

    /** Files API로 작은 probe 파일을 올렸다 지운다. 성공이면 null, 실패면 사유. */
    private String probeFilesApi(DbConnection c) {
        try {
            HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
            String path = DatabricksSupport.volumePath(c) + "/_dz_probe.txt";
            URI uri = new URI("https", c.host(), "/api/2.0/fs/files" + path, "overwrite=true", null);
            String auth = "Bearer " + DatabricksSupport.token(c);
            HttpResponse<String> put = http.send(HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(30))
                    .header("Authorization", auth).header("Content-Type", "application/octet-stream")
                    .PUT(HttpRequest.BodyPublishers.ofString("dz")).build(), HttpResponse.BodyHandlers.ofString());
            if (put.statusCode() / 100 != 2) {
                return "Files API PUT HTTP " + put.statusCode();
            }
            http.send(HttpRequest.newBuilder(new URI("https", c.host(), "/api/2.0/fs/files" + path, null, null))
                    .timeout(Duration.ofSeconds(30)).header("Authorization", auth).DELETE().build(),
                    HttpResponse.BodyHandlers.ofString());
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "중단됨";
        } catch (Exception e) {
            return e.toString();
        }
    }
}
