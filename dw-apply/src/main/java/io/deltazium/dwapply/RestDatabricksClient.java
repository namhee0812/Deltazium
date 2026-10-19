package io.deltazium.dwapply;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 파일명 : RestDatabricksClient.java
 * 작성일자 : 26. 10. 19.
 * 작성자 : 최남희
 * 설명 : DatabricksClient 실구현 — 파일은 Files API(java.net.http, Bearer 토큰), SQL은 Databricks
 * JDBC(OSS 드라이버). JDBC 커넥션은 스레드당 1개를 보관하고 SQL 실패 시 버려 다음 호출에서 새로
 * 연다. 토큰은 Properties(PWD)로만 넘겨 URL·로그에 남기지 않는다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public final class RestDatabricksClient implements DatabricksClient {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String host;
    private final String catalog;
    private final String token;
    private final String jdbcUrl;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15)).build();
    private final ThreadLocal<Connection> conn = new ThreadLocal<>();
    private final ConcurrentLinkedQueue<Connection> all = new ConcurrentLinkedQueue<>();

    public RestDatabricksClient(String host, String httpPath, String catalog, String token) {
        this.host = host;
        this.catalog = catalog;
        this.token = token;
        // EnableArrow=0: Arrow 결과 포맷 회피(JDK 모듈 접근 문제 예방). 결과가 작은 문장만 쓴다.
        this.jdbcUrl = "jdbc:databricks://%s:443;httpPath=%s;AuthMech=3;UID=token;EnableArrow=0"
                .formatted(host, httpPath);
    }

    /** 드라이버 로드 확인 — 실패는 설정 오류(exit 2). */
    public void checkDriver() throws SQLException {
        DriverManager.getDriver(jdbcUrl);
    }

    private Connection connection() throws SQLException {
        Connection c = conn.get();
        if (c == null || c.isClosed()) {
            Properties p = new Properties();
            p.setProperty("PWD", token);
            c = DriverManager.getConnection(jdbcUrl, p);
            conn.set(c);
            all.add(c);
        }
        return c;
    }

    private void discardConnection() {
        Connection c = conn.get();
        conn.remove();
        if (c != null) {
            all.remove(c);
            try {
                c.close();
            } catch (SQLException ignore) {
                // 이미 깨진 커넥션
            }
        }
    }

    @Override
    public void merge(String sql) throws SQLException {
        try (Statement st = connection().createStatement()) {
            st.execute(sql);
        } catch (SQLException e) {
            discardConnection();
            throw e;
        }
    }

    @Override
    public Map<String, String> describeColumns(String schema, String table) throws SQLException {
        String sql = "SELECT column_name, full_data_type FROM `" + catalog.replace("`", "``")
                + "`.information_schema.columns WHERE table_schema = ? AND table_name = ?";
        try (PreparedStatement ps = connection().prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            Map<String, String> r = new HashMap<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    r.put(rs.getString(1).toLowerCase(), rs.getString(2));
                }
            }
            return r;
        } catch (SQLException e) {
            discardConnection();
            throw e;
        }
    }

    @Override
    public void upload(String path, byte[] content) throws IOException {
        HttpRequest req = HttpRequest.newBuilder(filesUri(path, "/api/2.0/fs/files", "overwrite=true"))
                .timeout(Duration.ofMinutes(5))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/octet-stream")
                .PUT(HttpRequest.BodyPublishers.ofByteArray(content)).build();
        HttpResponse<String> resp = send(req);
        if (resp.statusCode() / 100 != 2) {
            throw new IOException("Files API 업로드 실패 HTTP " + resp.statusCode() + " " + path + " " + trim(resp.body()));
        }
    }

    @Override
    public void delete(String path) throws IOException {
        HttpRequest req = HttpRequest.newBuilder(filesUri(path, "/api/2.0/fs/files", null))
                .timeout(Duration.ofSeconds(60))
                .header("Authorization", "Bearer " + token).DELETE().build();
        HttpResponse<String> resp = send(req);
        if (resp.statusCode() / 100 != 2 && resp.statusCode() != 404) {
            throw new IOException("Files API 삭제 실패 HTTP " + resp.statusCode() + " " + path + " " + trim(resp.body()));
        }
    }

    @Override
    public List<String> listDir(String path) throws IOException {
        List<String> files = new ArrayList<>();
        String pageToken = null;
        do {
            String q = pageToken == null ? null : "page_token=" + java.net.URLEncoder.encode(pageToken, java.nio.charset.StandardCharsets.UTF_8);
            HttpRequest req = HttpRequest.newBuilder(filesUri(path, "/api/2.0/fs/directories", q))
                    .timeout(Duration.ofSeconds(60))
                    .header("Authorization", "Bearer " + token).GET().build();
            HttpResponse<String> resp = send(req);
            if (resp.statusCode() == 404) {
                return List.of();
            }
            if (resp.statusCode() / 100 != 2) {
                throw new IOException("Files API 목록 실패 HTTP " + resp.statusCode() + " " + path + " " + trim(resp.body()));
            }
            JsonNode body = MAPPER.readTree(resp.body());
            for (JsonNode c : body.path("contents")) {
                if (!c.path("is_directory").asBoolean(false)) {
                    files.add(c.path("path").asText());
                }
            }
            pageToken = body.hasNonNull("next_page_token") && !body.get("next_page_token").asText().isEmpty()
                    ? body.get("next_page_token").asText() : null;
        } while (pageToken != null);
        return files;
    }

    private HttpResponse<String> send(HttpRequest req) throws IOException {
        try {
            return http.send(req, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("중단됨", e);
        }
    }

    private URI filesUri(String path, String api, String query) throws IOException {
        try {
            return new URI("https", host, api + path, query, null);
        } catch (URISyntaxException e) {
            throw new IOException("잘못된 경로: " + path, e);
        }
    }

    private static String trim(String s) {
        return s == null ? "" : s.length() > 300 ? s.substring(0, 300) : s;
    }

    @Override
    public void close() {
        for (Connection c : all) {
            try {
                c.close();
            } catch (SQLException ignore) {
                // 종료 중
            }
        }
        all.clear();
    }
}
