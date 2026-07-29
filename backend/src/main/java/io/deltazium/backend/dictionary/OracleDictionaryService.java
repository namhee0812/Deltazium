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
import org.springframework.stereotype.Service;

/**
 * 소스 Oracle 딕셔너리 조회·사전 점검·supplemental logging 적용.
 * DDL 적용은 사용자가 UI에서 명시적으로 승인("적용하겠습니까?" YES)했을 때만 호출된다 —
 * 이 서비스가 임의로 호출되는 경로를 만들지 말 것.
 */
@Service
public class OracleDictionaryService {

    /** "SCHEMA.PATTERN" 입력 파싱. 와일드카드 *는 LIKE %로 변환. 예: CDC.* / CDC.TEST_% / CDC.T1 */
    static String[] parsePattern(String pattern) {
        if (pattern == null || !pattern.contains(".")) {
            throw new IllegalArgumentException("패턴은 SCHEMA.TABLE 형식이어야 한다 (예: CDC.* 또는 CDC.T1): " + pattern);
        }
        int dot = pattern.indexOf('.');
        String schema = pattern.substring(0, dot).trim().toUpperCase(Locale.ROOT);
        String table = pattern.substring(dot + 1).trim().toUpperCase(Locale.ROOT).replace("*", "%");
        if (schema.isEmpty() || table.isEmpty()) {
            throw new IllegalArgumentException("패턴 형식 오류: " + pattern);
        }
        return new String[] {schema, table};
    }

    /** 패턴에 걸리는 테이블 목록 + 테이블별 PK/supp.log 상태. */
    public List<SourceTableInfo> listTables(DbConnection source, String pattern) {
        String[] p = parsePattern(pattern);
        String sql = """
                SELECT t.owner, t.table_name, t.num_rows,
                       (SELECT COUNT(*) FROM all_constraints c
                         WHERE c.owner = t.owner AND c.table_name = t.table_name
                           AND c.constraint_type = 'P') AS pk_cnt,
                       (SELECT COUNT(*) FROM all_log_groups g
                         WHERE g.owner = t.owner AND g.table_name = t.table_name
                           AND g.log_group_type = 'ALL COLUMN LOGGING') AS supp_cnt
                  FROM all_tables t
                 WHERE t.owner = ? AND t.table_name LIKE ?
                 ORDER BY t.owner, t.table_name""";
        List<SourceTableInfo> result = new ArrayList<>();
        try (Connection conn = open(source);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, p[0]);
            ps.setString(2, p[1]);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long numRows = rs.getLong("num_rows");
                    result.add(new SourceTableInfo(
                            rs.getString("owner"),
                            rs.getString("table_name"),
                            rs.getInt("pk_cnt") > 0,
                            rs.getInt("supp_cnt") > 0,
                            rs.wasNull() ? null : numRows));
                }
            }
            return result;
        } catch (SQLException e) {
            throw new DictionaryException("딕셔너리 조회 실패: " + e.getMessage(), e);
        }
    }

    /** DB 레벨 점검: ARCHIVELOG 모드, DB 최소 supplemental logging. 권한 부족 시 원인 메시지 포함. */
    public Map<String, String> databaseChecks(DbConnection source) {
        Map<String, String> checks = new LinkedHashMap<>();
        try (Connection conn = open(source);
             Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT log_mode, supplemental_log_data_min FROM v$database")) {
                rs.next();
                checks.put("archivelog", rs.getString(1));            // ARCHIVELOG | NOARCHIVELOG
                checks.put("db_supplemental_log_min", rs.getString(2)); // YES | NO
            } catch (SQLException e) {
                // v$database 권한(SELECT ANY DICTIONARY 등) 부족 — 실패 사유를 그대로 노출
                checks.put("archivelog", "확인 불가: " + e.getMessage().strip());
                checks.put("db_supplemental_log_min", "확인 불가");
            }
            return checks;
        } catch (SQLException e) {
            throw new DictionaryException("소스 접속 실패: " + e.getMessage(), e);
        }
    }

    /** 컬럼 목록 + PK 여부. 소스(매핑 원본)와 타깃(매핑 대상) 모두 이걸로 조회한다. */
    public List<TableColumn> listColumns(DbConnection conn, String schema, String table) {
        String sql = """
                SELECT c.column_name, c.data_type,
                       CASE WHEN pk.column_name IS NULL THEN 0 ELSE 1 END AS is_pk
                  FROM all_tab_columns c
                  LEFT JOIN (SELECT cc.column_name
                               FROM all_constraints k
                               JOIN all_cons_columns cc
                                 ON cc.owner = k.owner AND cc.constraint_name = k.constraint_name
                              WHERE k.owner = ? AND k.table_name = ? AND k.constraint_type = 'P') pk
                    ON pk.column_name = c.column_name
                 WHERE c.owner = ? AND c.table_name = ?
                 ORDER BY c.column_id""";
        List<TableColumn> result = new ArrayList<>();
        try (Connection db = open(conn);
             PreparedStatement ps = db.prepareStatement(sql)) {
            ps.setString(1, schema.toUpperCase(Locale.ROOT));
            ps.setString(2, table.toUpperCase(Locale.ROOT));
            ps.setString(3, schema.toUpperCase(Locale.ROOT));
            ps.setString(4, table.toUpperCase(Locale.ROOT));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new TableColumn(
                            rs.getString(1), rs.getString(2), rs.getInt(3) == 1));
                }
            }
            return result;
        } catch (SQLException e) {
            throw new DictionaryException("컬럼 조회 실패: " + e.getMessage(), e);
        }
    }

    /** 캡처 계정 최소 권한 8개 (architecture.md 8절, 2026-07-28 확정). CREATE SESSION은 접속 성공이 곧 증명. */
    static final List<String> REQUIRED_SYS_PRIVS = List.of(
            "LOGMINING", "SELECT ANY DICTIONARY", "SELECT ANY TABLE",
            "FLASHBACK ANY TABLE", "CREATE TABLE");

    static final List<String> REQUIRED_EXEC_PACKAGES = List.of("DBMS_LOGMNR", "DBMS_LOGMNR_D");

    /**
     * LogMiner 권한 점검 — 권한명 → 보유 여부.
     * 권한은 계정 스스로 부여할 수 없으므로(DBA 필요) 적용 API는 없다.
     * 누락 시 UI가 DBA용 GRANT 스크립트를 보여주고 배포를 차단한다.
     */
    public Map<String, Boolean> privilegeChecks(DbConnection source) {
        Map<String, Boolean> result = new LinkedHashMap<>();
        try (Connection conn = open(source)) {
            result.put("CREATE SESSION", true); // 접속 성공

            var sysPrivs = new java.util.HashSet<String>();
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT privilege FROM session_privs")) {
                while (rs.next()) {
                    sysPrivs.add(rs.getString(1));
                }
            }
            for (String p : REQUIRED_SYS_PRIVS) {
                result.put(p, sysPrivs.contains(p));
            }

            // EXECUTE는 직접/PUBLIC/롤 경유 모두 인정
            String sql = """
                    SELECT table_name FROM all_tab_privs
                     WHERE privilege = 'EXECUTE' AND table_name = ?
                       AND (grantee = USER OR grantee = 'PUBLIC'
                            OR grantee IN (SELECT role FROM session_roles))""";
            for (String pkg : REQUIRED_EXEC_PACKAGES) {
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, pkg);
                    try (ResultSet rs = ps.executeQuery()) {
                        result.put("EXECUTE ON " + pkg, rs.next());
                    }
                }
            }
            return result;
        } catch (SQLException e) {
            throw new DictionaryException("권한 점검 실패: " + e.getMessage(), e);
        }
    }

    /**
     * 테이블별 ALL COLUMNS supplemental logging 적용 시도.
     * 반환: qualified name → "OK" 또는 Oracle 에러 메시지 (권한 부족 등은 메시지 그대로).
     */
    public Map<String, String> applySupplementalLogging(DbConnection source, List<String> qualifiedTables) {
        Map<String, String> results = new LinkedHashMap<>();
        try (Connection conn = open(source);
             Statement st = conn.createStatement()) {
            for (String qt : qualifiedTables) {
                String[] p = parsePattern(qt); // SCHEMA.TABLE 검증 겸용 (와일드카드 불허)
                if (p[1].contains("%")) {
                    results.put(qt, "적용 대상은 개별 테이블이어야 한다");
                    continue;
                }
                try {
                    st.execute("ALTER TABLE \"%s\".\"%s\" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"
                            .formatted(p[0], p[1]));
                    results.put(qt, "OK");
                } catch (SQLException e) {
                    results.put(qt, e.getMessage() == null ? e.toString() : e.getMessage().strip());
                }
            }
            return results;
        } catch (SQLException e) {
            throw new DictionaryException("소스 접속 실패: " + e.getMessage(), e);
        }
    }

    private Connection open(DbConnection c) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", c.username());
        props.setProperty("password", c.password());
        props.setProperty("oracle.net.CONNECT_TIMEOUT", "5000");
        return DriverManager.getConnection(c.jdbcUrl(), props);
    }

    public static class DictionaryException extends RuntimeException {
        public DictionaryException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
