package io.deltazium.backend.registry;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

@Repository
public class DbConnectionRepository {

    private static final RowMapper<DbConnection> MAPPER = (rs, n) -> new DbConnection(
            rs.getLong("id"),
            rs.getString("name"),
            rs.getString("db_type"),
            rs.getString("role"),
            rs.getString("host"),
            rs.getInt("port"),
            rs.getString("database_name"),
            rs.getString("username"),
            rs.getString("password"));

    private final JdbcTemplate jdbc;

    public DbConnectionRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<DbConnection> findAll() {
        return jdbc.query("SELECT * FROM db_connections ORDER BY id", MAPPER);
    }

    public Optional<DbConnection> findById(long id) {
        return jdbc.query("SELECT * FROM db_connections WHERE id = ?", MAPPER, id)
                .stream().findFirst();
    }

    public Optional<DbConnection> findByName(String name) {
        return jdbc.query("SELECT * FROM db_connections WHERE name = ?", MAPPER, name)
                .stream().findFirst();
    }

    public DbConnection insert(DbConnection c) {
        KeyHolder keys = new GeneratedKeyHolder();
        jdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement("""
                    INSERT INTO db_connections
                      (name, db_type, role, host, port, database_name, username, password)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", new String[] {"id"});
            ps.setString(1, c.name());
            ps.setString(2, c.dbType());
            ps.setString(3, c.role());
            ps.setString(4, c.host());
            ps.setInt(5, c.port());
            ps.setString(6, c.databaseName());
            ps.setString(7, c.username());
            ps.setString(8, c.password());
            return ps;
        }, keys);
        long id = Objects.requireNonNull(keys.getKeyAs(Number.class), "generated key").longValue();
        return c.withId(id);
    }

    public boolean update(DbConnection c) {
        return jdbc.update("""
                UPDATE db_connections
                   SET name = ?, role = ?, host = ?, port = ?,
                       database_name = ?, username = ?, password = ?
                 WHERE id = ?""",
                c.name(), c.role(), c.host(), c.port(),
                c.databaseName(), c.username(), c.password(), c.id()) == 1;
    }

    public boolean delete(long id) {
        return jdbc.update("DELETE FROM db_connections WHERE id = ?", id) == 1;
    }
}
