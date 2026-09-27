package io.deltazium.backend.dictionary;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.deltazium.backend.registry.DbConnection;
import io.deltazium.backend.registry.DbType;
import org.springframework.stereotype.Service;

/**
 * 파일명 : SingleStoreDictionaryService.java
 * 작성일자 : 26. 09. 28.
 * 작성자 : 최남희
 * 설명 : SingleStore 딕셔너리 조회 — **타깃 전용**(architecture.md 8절, DbType.SINGLESTORE는
 * sourceCapable=false). {@link SourceDictionary}는 소스·타깃 공용 계약(DictionaryRouter가
 * 컬럼 조회를 타깃에도 쓴다, 클래스 설명 참고)이라 인터페이스 전체를 구현해야 하지만, 이
 * 구현은 타깃 조회에 필요한 {@link #listColumns}만 의미 있게 구현하고 나머지 소스 전용
 * 메서드(테이블 목록·DB/권한 점검·캡처 설정)는 {@link UnsupportedOperationException}으로
 * 명확히 실패시킨다 — SingleStore가 SOURCE로 등록될 길이 없어(DbConnectionService.validate)
 * 실제로 호출되지 않지만, 호출된다면 조용히 빈 값을 돌려주는 것보다 원인을 바로 드러내는
 * 쪽이 안전하다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 28.       | 최남희  | 최초 생성 — SingleStore 타깃 지원
 * --------------------------------------------------
 */
@Service
public class SingleStoreDictionaryService implements SourceDictionary {

    @Override
    public DbType dbType() {
        return DbType.SINGLESTORE;
    }

    @Override
    public List<SourceTableInfo> listTables(DbConnection conn, String pattern) {
        throw unsupported();
    }

    /**
     * 컬럼 목록 + PK 여부 — 타깃 테이블 존재 확인(RegistrationService)·복구 sink key 컬럼
     * (RecoveryService)이 쓴다. information_schema는 대소문자를 보존하므로(폴딩 없음,
     * DbType.foldIdentifier) schema·table을 원문 그대로 비교한다.
     */
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
                         AND kcu.table_name = tc.table_name
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
        return new UnsupportedOperationException("SingleStore는 소스로 지원하지 않는다 (타깃 전용, architecture.md 8절)");
    }

    private Connection open(DbConnection c) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", c.username());
        props.setProperty("password", c.password());
        props.setProperty("connectTimeout", "5000");
        props.setProperty("socketTimeout", "5000");
        return DriverManager.getConnection(c.jdbcUrl(), props);
    }
}
