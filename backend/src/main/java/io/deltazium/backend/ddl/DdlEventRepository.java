package io.deltazium.backend.ddl;

import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class DdlEventRepository {

    private static final RowMapper<DdlEvent> MAPPER = (rs, n) -> new DdlEvent(
            rs.getLong("id"),
            rs.getLong("kafka_offset"),
            rs.getLong("event_ts_ms"),
            rs.getString("scn"),
            rs.getString("schema_name"),
            rs.getString("table_name"),
            rs.getString("ddl_text"),
            rs.getString("state"),
            rs.getString("note"),
            rs.getTimestamp("decided_at") == null ? null
                    : rs.getTimestamp("decided_at").toLocalDateTime());

    private final JdbcTemplate jdbc;

    public DdlEventRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<DdlEvent> findAll() {
        return jdbc.query("SELECT * FROM ddl_events ORDER BY kafka_offset DESC", MAPPER);
    }

    public Optional<DdlEvent> findById(long id) {
        return jdbc.query("SELECT * FROM ddl_events WHERE id = ?", MAPPER, id).stream().findFirst();
    }

    /** kafka_offset 기준 멱등 삽입 — 이미 있으면 false. */
    public boolean insertIfAbsent(long kafkaOffset, long tsMs, String scn,
                                  String schema, String table, String ddl, String state) {
        Integer dup = jdbc.queryForObject(
                "SELECT COUNT(*) FROM ddl_events WHERE kafka_offset = ?", Integer.class, kafkaOffset);
        if (dup != null && dup > 0) {
            return false;
        }
        jdbc.update("""
                INSERT INTO ddl_events (kafka_offset, event_ts_ms, scn, schema_name, table_name, ddl_text, state)
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                kafkaOffset, tsMs, scn, schema, table, ddl, state);
        return true;
    }

    public void decide(long id, String state, String note) {
        jdbc.update("UPDATE ddl_events SET state = ?, note = ?, decided_at = CURRENT_TIMESTAMP WHERE id = ?",
                state, note, id);
    }
}
