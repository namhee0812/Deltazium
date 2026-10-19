package io.deltazium.backend.dictionary;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.deltazium.backend.registry.DatabricksSupport;
import io.deltazium.backend.registry.DbConnection;
import io.deltazium.backend.registry.DbType;
import org.springframework.stereotype.Service;

/**
 * 파일명 : DatabricksDictionaryService.java
 * 작성일자 : 26. 10. 19.
 * 작성자 : 최남희
 * 설명 : Databricks 딕셔너리 조회 — **타깃 전용**(DbType.DATABRICKS는 sourceCapable=false).
 * SingleStoreDictionaryService와 같은 형태: 타깃 조회에 필요한 {@link #listColumns}만 구현하고
 * 소스 전용 메서드는 UnsupportedOperationException. UC information_schema(카탈로그 한정)로
 * 컬럼과 PK(PRIMARY KEY 제약)를 읽는다. UC는 식별자를 소문자로 저장하므로 schema·table은
 * 호출측이 DbType.foldIdentifier로 접은 값을 그대로 비교한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 최초 생성 — Databricks 타깃 지원
 * --------------------------------------------------
 */
@Service
public class DatabricksDictionaryService implements SourceDictionary {

    @Override
    public DbType dbType() {
        return DbType.DATABRICKS;
    }

    @Override
    public List<SourceTableInfo> listTables(DbConnection conn, String pattern) {
        throw unsupported();
    }

    @Override
    public List<TableColumn> listColumns(DbConnection conn, String schema, String table) {
        String cat = "`" + conn.databaseName().replace("`", "``") + "`";
        String sql = "SELECT c.column_name, c.data_type, "
                + "CASE WHEN pk.column_name IS NULL THEN 0 ELSE 1 END AS is_pk "
                + "FROM " + cat + ".information_schema.columns c "
                + "LEFT JOIN (SELECT kcu.column_name FROM " + cat + ".information_schema.table_constraints tc "
                + "JOIN " + cat + ".information_schema.key_column_usage kcu "
                + "ON kcu.constraint_catalog = tc.constraint_catalog AND kcu.constraint_schema = tc.constraint_schema "
                + "AND kcu.constraint_name = tc.constraint_name "
                + "WHERE tc.table_schema = ? AND tc.table_name = ? AND tc.constraint_type = 'PRIMARY KEY') pk "
                + "ON pk.column_name = c.column_name "
                + "WHERE c.table_schema = ? AND c.table_name = ? ORDER BY c.ordinal_position";
        List<TableColumn> result = new ArrayList<>();
        try (Connection db = DriverManager.getConnection(conn.jdbcUrl(), DatabricksSupport.props(conn));
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
    public List<PrecheckItem> databaseChecks(DbConnection conn) {
        throw unsupported();
    }

    @Override
    public List<PrecheckItem> privilegeChecks(DbConnection conn) {
        throw unsupported();
    }

    @Override
    public String captureSetupLabel() {
        throw unsupported();
    }

    @Override
    public Map<String, String> captureSetupPreview(DbConnection conn, List<String> qualifiedTables) {
        throw unsupported();
    }

    @Override
    public Map<String, String> applyCaptureSetup(DbConnection conn, List<String> qualifiedTables) {
        throw unsupported();
    }

    private static UnsupportedOperationException unsupported() {
        return new UnsupportedOperationException("Databricks는 소스로 지원하지 않는다 (타깃 전용, architecture.md 6.5절)");
    }
}
