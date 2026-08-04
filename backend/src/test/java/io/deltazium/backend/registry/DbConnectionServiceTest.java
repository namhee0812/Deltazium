package io.deltazium.backend.registry;

import org.junit.jupiter.api.Test;
import org.mybatis.spring.boot.test.autoconfigure.MybatisTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.sql.init.SqlInitializationAutoConfiguration;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 파일명 : DbConnectionServiceTest.java
 * 작성일자 : 26. 07. 25.
 * 작성자 : 최남희
 * 설명 : DB 연결 저장소 서비스 단위 테스트.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 25.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@MybatisTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ImportAutoConfiguration(SqlInitializationAutoConfiguration.class)
@Import(DbConnectionService.class)
class DbConnectionServiceTest {

    @Autowired
    DbConnectionService service;

    @MockitoBean
    OracleConnectionTester tester;

    private static DbConnection oracle(String name) {
        return new DbConnection(null, name, "ORACLE", "SOURCE",
                "oracledev", 1521, "XEPDB1", "dbzuser", "secret");
    }

    @Test
    void 등록_조회_삭제() {
        DbConnection saved = service.create(oracle("src-dev"));
        assertThat(saved.id()).isNotNull();
        assertThat(service.list()).extracting(DbConnection::name).contains("src-dev");
        assertThat(service.get(saved.id()).jdbcUrl())
                .isEqualTo("jdbc:oracle:thin:@//oracledev:1521/XEPDB1");

        service.delete(saved.id());
        assertThatThrownBy(() -> service.get(saved.id()))
                .isInstanceOf(DbConnectionService.NotFoundException.class);
    }

    @Test
    void 이름_중복은_거부한다() {
        service.create(oracle("dup"));
        assertThatThrownBy(() -> service.create(oracle("dup")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("이미 존재");
    }

    @Test
    void 미지원_dbType은_거부한다() {
        DbConnection mysql = new DbConnection(null, "m", "MYSQL", "SOURCE",
                "h", 3306, "db", "u", "p");
        assertThatThrownBy(() -> service.create(mysql))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("아직 지원하지 않는");
    }

    @Test
    void 알_수_없는_dbType은_거부한다() {
        DbConnection unknown = new DbConnection(null, "x", "MONGODB", "SOURCE",
                "h", 27017, "db", "u", "p");
        assertThatThrownBy(() -> service.create(unknown))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("알 수 없는 DB 종류");
    }

    @Test
    void 잘못된_role은_거부한다() {
        DbConnection bad = new DbConnection(null, "r", "ORACLE", "REPLICA",
                "h", 1521, "db", "u", "p");
        assertThatThrownBy(() -> service.create(bad))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("role");
    }

    @Test
    void 수정_시_빈_password는_기존_값을_유지한다() {
        DbConnection saved = service.create(oracle("keep-pw"));
        DbConnection edit = new DbConnection(null, "keep-pw", "ORACLE", "TARGET",
                "newhost", 1522, "NEWSVC", "dbzuser", "");
        DbConnection updated = service.update(saved.id(), edit);

        assertThat(updated.password()).isEqualTo("secret");
        assertThat(updated.role()).isEqualTo("TARGET");
        assertThat(updated.host()).isEqualTo("newhost");
    }
}
