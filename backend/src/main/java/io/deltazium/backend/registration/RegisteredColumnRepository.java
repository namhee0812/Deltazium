package io.deltazium.backend.registration;

import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class RegisteredColumnRepository {

    private static final RowMapper<ColumnMapping> MAPPER = (rs, n) -> new ColumnMapping(
            rs.getString("target_column"),
            rs.getString("source_expr"),
            rs.getBoolean("enabled"));

    private final JdbcTemplate jdbc;

    public RegisteredColumnRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<ColumnMapping> findByTable(long registeredTableId) {
        return jdbc.query(
                "SELECT * FROM registered_columns WHERE registered_table_id = ? ORDER BY id",
                MAPPER, registeredTableId);
    }

    public void deleteByTable(long registeredTableId) {
        jdbc.update("DELETE FROM registered_columns WHERE registered_table_id = ?", registeredTableId);
    }

    public void insertAll(long registeredTableId, List<ColumnMapping> mappings) {
        for (ColumnMapping m : mappings) {
            jdbc.update("""
                    INSERT INTO registered_columns (registered_table_id, target_column, source_expr, enabled)
                    VALUES (?, ?, ?, ?)""",
                    registeredTableId, m.targetColumn(), m.sourceExpr(), m.enabled());
        }
    }
}
