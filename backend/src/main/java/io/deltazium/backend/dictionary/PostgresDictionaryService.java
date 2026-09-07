package io.deltazium.backend.dictionary;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import io.deltazium.backend.registry.DbConnection;
import io.deltazium.backend.registry.DbType;
import org.springframework.stereotype.Service;

/**
 * 파일명 : PostgresDictionaryService.java
 * 작성일자 : 26. 09. 07.
 * 작성자 : 최남희
 * 설명 : 소스 PostgreSQL 딕셔너리 조회·사전 점검·REPLICA IDENTITY FULL 적용
 * (architecture.md 8절). 점검 항목·SQL은 Debezium PostgreSQL 커넥터 공식 문서(3.6/stable)
 * 확인 기준(2026-09-07, WebSearch — 원문 페이지는 403으로 직접 fetch 불가해 검색 요약으로 확정):
 * https://debezium.io/documentation/reference/stable/connectors/postgresql.html
 * - REPLICATION 권한 롤 + LOGIN (슈퍼유저 대체 가능하나 비권장)
 * - publication.autocreate.mode=filtered는 캡처 테이블만 묶은 publication을 커넥터가 자동 생성 —
 *   이때 연결 계정이 각 테이블의 소유자이거나 그에 준하는 권한이 있어야 한다.
 * - REPLICA IDENTITY FULL 미설정이면 UPDATE/DELETE의 before 이미지가 PK 컬럼만 남는다
 *   (Oracle supplemental logging (ALL) COLUMNS와 동등한 승인 대상, 6·8절).
 * DDL 적용(REPLICA IDENTITY FULL)은 사용자가 UI에서 명시적으로 승인했을 때만 호출된다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 최초 생성 — 다중 소스·다중 타깃 ② 두 번째 소스
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | PG 소스 실 배선 스모크 수정: 캡처 계정에 database CREATE
 * |                          | 권한이 없으면 publication.autocreate.mode=filtered가 publication을
 * |                          | 만들지 못해 source task가 죽는 것을 실측(에러: "Unable to create
 * |                          | filtered publication dz_pg", GRANT CREATE ON DATABASE로 해결).
 * |                          | 근거: Debezium PostgreSQL 커넥터 문서(3.6/stable)의 publication
 * |                          | 자동 생성 권한 절 — privilegeChecks에 blocking 항목으로 추가
 * --------------------------------------------------
 */
@Service
public class PostgresDictionaryService implements SourceDictionary {

    @Override
    public DbType dbType() {
        return DbType.POSTGRESQL;
    }

    @Override
    public List<SourceTableInfo> listTables(DbConnection source, String pattern) {
        // 패턴 파싱(SCHEMA.TABLE, *→%)은 Oracle과 동일 규칙을 재사용한다 — 결과는 대문자로
        // 정규화되지만 아래 조회가 ILIKE(대소문자 무관)라 PostgreSQL의 소문자 식별자도 매칭된다.
        String[] p = OracleDictionaryService.parsePattern(pattern);
        String schema = p[0];
        String table = p[1];
        String sql = """
                SELECT c.table_schema, c.table_name,
                       (SELECT COUNT(*) FROM information_schema.table_constraints tc
                         WHERE tc.table_schema = c.table_schema AND tc.table_name = c.table_name
                           AND tc.constraint_type = 'PRIMARY KEY') AS pk_cnt,
                       (SELECT relreplident FROM pg_class pc
                         JOIN pg_namespace n ON n.oid = pc.relnamespace
                         WHERE n.nspname = c.table_schema AND pc.relname = c.table_name) AS replident,
                       (SELECT reltuples::bigint FROM pg_class pc
                         JOIN pg_namespace n ON n.oid = pc.relnamespace
                         WHERE n.nspname = c.table_schema AND pc.relname = c.table_name) AS est_rows
                  FROM information_schema.tables c
                 WHERE c.table_schema ILIKE ? AND c.table_name ILIKE ? AND c.table_type = 'BASE TABLE'
                 ORDER BY c.table_schema, c.table_name""";
        List<SourceTableInfo> result = new ArrayList<>();
        try (Connection conn = open(source);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long estRows = rs.getLong("est_rows");
                    String replident = rs.getString("replident");
                    result.add(new SourceTableInfo(
                            rs.getString("table_schema"),
                            rs.getString("table_name"),
                            rs.getInt("pk_cnt") > 0,
                            "f".equals(replident),
                            rs.wasNull() || estRows < 0 ? null : estRows));
                }
            }
            return result;
        } catch (SQLException e) {
            throw new SourceDictionary.DictionaryException("딕셔너리 조회 실패: " + e.getMessage(), e);
        }
    }

    @Override
    public List<TableColumn> listColumns(DbConnection conn, String schema, String table) {
        String sql = """
                SELECT c.column_name, c.data_type,
                       CASE WHEN pk.column_name IS NULL THEN 0 ELSE 1 END AS is_pk
                  FROM information_schema.columns c
                  LEFT JOIN (
                      SELECT kcu.column_name
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                          ON kcu.constraint_name = tc.constraint_name
                         AND kcu.table_schema = tc.table_schema
                       WHERE tc.table_schema = ? AND tc.table_name = ? AND tc.constraint_type = 'PRIMARY KEY'
                  ) pk ON pk.column_name = c.column_name
                 WHERE c.table_schema = ? AND c.table_name = ?
                 ORDER BY c.ordinal_position""";
        List<TableColumn> result = new ArrayList<>();
        try (Connection db = open(conn);
             PreparedStatement ps = db.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            ps.setString(3, schema);
            ps.setString(4, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new TableColumn(rs.getString(1), rs.getString(2), rs.getInt(3) == 1));
                }
            }
            return result;
        } catch (SQLException e) {
            throw new SourceDictionary.DictionaryException("컬럼 조회 실패: " + e.getMessage(), e);
        }
    }

    @Override
    public List<PrecheckItem> databaseChecks(DbConnection source) {
        List<PrecheckItem> checks = new ArrayList<>();
        try (Connection conn = open(source);
             Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT setting FROM pg_settings WHERE name = 'wal_level'")) {
                rs.next();
                String walLevel = rs.getString(1);
                checks.add(new PrecheckItem("wal_level", "wal_level=logical",
                        "logical".equalsIgnoreCase(walLevel), walLevel, true));
            }
            return checks;
        } catch (SQLException e) {
            throw new SourceDictionary.DictionaryException("소스 접속 실패: " + e.getMessage(), e);
        }
    }

    /**
     * 캡처 계정 권한 — REPLICATION 속성(또는 superuser) + LOGIN(접속 성공이 곧 증명) +
     * database CREATE(publication.autocreate.mode=filtered가 publication을 만들 때 필요 —
     * 없으면 source task가 "Unable to create filtered publication ..."로 죽는다, 2026-09-07 실측).
     * 테이블 소유권 등 세부 권한은 테이블마다 달라 여기서는 확인하지 않고 등록 실패 시 Connect
     * 커넥터 trace로 드러난다(캡처 롤 준비 절차는 deploy/pg-source-setup.sh).
     */
    @Override
    public List<PrecheckItem> privilegeChecks(DbConnection source) {
        List<PrecheckItem> result = new ArrayList<>();
        result.add(new PrecheckItem("LOGIN", "LOGIN", true, "보유(접속 성공)", true));
        try (Connection conn = open(source)) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT rolreplication OR rolsuper FROM pg_roles WHERE rolname = current_user")) {
                try (ResultSet rs = ps.executeQuery()) {
                    boolean ok = rs.next() && rs.getBoolean(1);
                    result.add(new PrecheckItem("REPLICATION", "REPLICATION(또는 superuser)",
                            ok, ok ? "보유" : "누락", true));
                }
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT has_database_privilege(current_user, current_database(), 'CREATE')")) {
                try (ResultSet rs = ps.executeQuery()) {
                    boolean ok = rs.next() && rs.getBoolean(1);
                    result.add(new PrecheckItem("CREATE ON DATABASE",
                            "CREATE ON DATABASE (publication 자동 생성에 필요)", ok,
                            ok ? "보유" : "누락 — GRANT CREATE ON DATABASE " + currentDatabase(conn)
                                    + " TO " + source.username() + ";",
                            true));
                }
            }
            return result;
        } catch (SQLException e) {
            throw new SourceDictionary.DictionaryException("권한 점검 실패: " + e.getMessage(), e);
        }
    }

    private static String currentDatabase(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT current_database()")) {
            rs.next();
            return rs.getString(1);
        }
    }

    @Override
    public String captureSetupLabel() {
        return "REPLICA IDENTITY FULL";
    }

    @Override
    public Map<String, String> captureSetupPreview(DbConnection source, List<String> qualifiedTables) {
        Map<String, String> preview = new LinkedHashMap<>();
        for (String qt : qualifiedTables) {
            int dot = qt.indexOf('.');
            String schema = qt.substring(0, dot);
            String table = qt.substring(dot + 1);
            preview.put(qt, "ALTER TABLE \"%s\".\"%s\" REPLICA IDENTITY FULL;".formatted(schema, table));
        }
        return preview;
    }

    @Override
    public Map<String, String> applyCaptureSetup(DbConnection source, List<String> qualifiedTables) {
        Map<String, String> results = new LinkedHashMap<>();
        try (Connection conn = open(source);
             Statement st = conn.createStatement()) {
            for (String qt : qualifiedTables) {
                int dot = qt.indexOf('.');
                if (dot <= 0) {
                    results.put(qt, "SCHEMA.TABLE 형식이어야 한다");
                    continue;
                }
                String schema = qt.substring(0, dot);
                String table = qt.substring(dot + 1);
                try {
                    st.execute("ALTER TABLE \"%s\".\"%s\" REPLICA IDENTITY FULL".formatted(schema, table));
                    results.put(qt, "OK");
                } catch (SQLException e) {
                    results.put(qt, e.getMessage() == null ? e.toString() : e.getMessage().strip());
                }
            }
            return results;
        } catch (SQLException e) {
            throw new SourceDictionary.DictionaryException("소스 접속 실패: " + e.getMessage(), e);
        }
    }

    private Connection open(DbConnection c) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", c.username());
        props.setProperty("password", c.password());
        props.setProperty("connectTimeout", "5");
        return DriverManager.getConnection(c.jdbcUrl(), props);
    }
}
