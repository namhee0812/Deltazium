package io.deltazium.backend.registration;

import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class RegisteredTableRepository {

    private static final RowMapper<RegisteredTable> MAPPER = (rs, n) -> new RegisteredTable(
            rs.getLong("id"),
            rs.getString("schema_name"),
            rs.getString("table_name"),
            rs.getLong("source_connection_id"),
            rs.getLong("target_connection_id"));

    private final JdbcTemplate jdbc;

    public RegisteredTableRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<RegisteredTable> findAll() {
        return jdbc.query("SELECT * FROM registered_tables ORDER BY schema_name, table_name", MAPPER);
    }

    public boolean exists(String schema, String table) {
        Integer cnt = jdbc.queryForObject(
                "SELECT COUNT(*) FROM registered_tables WHERE schema_name = ? AND table_name = ?",
                Integer.class, schema, table);
        return cnt != null && cnt > 0;
    }

    /** 다른 스키마에 같은 이름의 테이블이 있는지 — iceberg route-field(source.table) 충돌 검사용 */
    public boolean existsTableNameInOtherSchema(String schema, String table) {
        Integer cnt = jdbc.queryForObject(
                "SELECT COUNT(*) FROM registered_tables WHERE table_name = ? AND schema_name <> ?",
                Integer.class, table, schema);
        return cnt != null && cnt > 0;
    }

    public void insert(String schema, String table, long sourceConnId, long targetConnId) {
        jdbc.update("""
                INSERT INTO registered_tables (schema_name, table_name, source_connection_id, target_connection_id)
                VALUES (?, ?, ?, ?)""", schema, table, sourceConnId, targetConnId);
    }

    public boolean delete(long id) {
        return jdbc.update("DELETE FROM registered_tables WHERE id = ?", id) == 1;
    }
}
