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
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ②: topic_prefix 컬럼 매핑, findByTopicPrefix
 * |                          | 추가(연결 등록 시 SOURCE 유일성 검증용)
 * --------------------------------------------------
 */
@Mapper
public interface DbConnectionRepository {

    List<DbConnection> findAll();

    Optional<DbConnection> findById(long id);

    Optional<DbConnection> findByName(String name);

    /** SOURCE 연결의 topic_prefix 유일성 검증용 — role 무관하게 조회하고 서비스에서 role을 본다. */
    Optional<DbConnection> findByTopicPrefix(String topicPrefix);

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
        public String topicPrefix;
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
        row.topicPrefix = c.topicPrefix();
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
