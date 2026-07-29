package io.deltazium.backend.registration;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.Objects;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

@Repository
public class RegisteredTableRepository {

    private static final RowMapper<RegisteredTable> MAPPER = (rs, n) -> new RegisteredTable(
            rs.getLong("id"),
            rs.getString("schema_name"),
            rs.getString("table_name"),
            rs.getLong("source_connection_id"),
            rs.getLong("target_connection_id"),
            rs.getString("target_schema_name"),
            rs.getString("target_table_name"));

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

    public long insert(String schema, String table, long sourceConnId, long targetConnId,
                       String targetSchema, String targetTable) {
        KeyHolder keys = new GeneratedKeyHolder();
        jdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement("""
                    INSERT INTO registered_tables
                      (schema_name, table_name, source_connection_id, target_connection_id,
                       target_schema_name, target_table_name)
                    VALUES (?, ?, ?, ?, ?, ?)""", new String[] {"id"});
            ps.setString(1, schema);
            ps.setString(2, table);
            ps.setLong(3, sourceConnId);
            ps.setLong(4, targetConnId);
            ps.setString(5, targetSchema);
            ps.setString(6, targetTable);
            return ps;
        }, keys);
        return Objects.requireNonNull(keys.getKeyAs(Number.class), "generated key").longValue();
    }

    public boolean delete(long id) {
        return jdbc.update("DELETE FROM registered_tables WHERE id = ?", id) == 1;
    }
}
