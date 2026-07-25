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
