package io.deltazium.backend.registry;

import java.util.List;
import java.util.Optional;

import org.apache.ibatis.annotations.Mapper;

/**
 * 파일명 : DbConnectionRepository.java
 * 작성일자 : 26. 07. 25.
 * 작성자 : 최남희
 * 설명 : SQL은 resources/mappers/db-connection.xml
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 25.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@Mapper
public interface DbConnectionRepository {

    List<DbConnection> findAll();

    Optional<DbConnection> findById(long id);

    Optional<DbConnection> findByName(String name);

    /** generated key를 받으려면 가변 홀더가 필요하다 (record는 불변) */
    class InsertRow {
        public Long id;
        public String name;
        public String dbType;
        public String role;
        public String host;
        public int port;
        public String databaseName;
        public String username;
        public String password;
    }

    void insertRow(InsertRow row);

    default DbConnection insert(DbConnection c) {
        InsertRow row = new InsertRow();
        row.name = c.name();
        row.dbType = c.dbType();
        row.role = c.role();
        row.host = c.host();
        row.port = c.port();
        row.databaseName = c.databaseName();
        row.username = c.username();
        row.password = c.password();
        insertRow(row);
        return c.withId(row.id);
    }

    int updateRow(DbConnection c);

    default boolean update(DbConnection c) {
        return updateRow(c) == 1;
    }

    int deleteRow(long id);

    default boolean delete(long id) {
        return deleteRow(id) == 1;
    }
}
