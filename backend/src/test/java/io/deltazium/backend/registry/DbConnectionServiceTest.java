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
 * 26. 10. 19.       | 최남희  | Databricks 타깃: 필수값(httpPath·catalog·stagingVolume 형식)·토큰 폴백(환경변수)·
 * |                          | port/username 자동 보정·update의 빈 password 유지 검증
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
    void topicPrefix는_이름_슬러그로_고정되고_요청값과_이름_변경을_무시한다() {
        DbConnection req = new DbConnection(null, "Src-Dev 1", "ORACLE", "SOURCE",
                "oracledev", 1521, "XEPDB1", "dbzuser", "secret", "custom");
        DbConnection saved = service.create(req);
        assertThat(saved.topicPrefix()).isEqualTo("src_dev_1");

        DbConnection renamed = new DbConnection(saved.id(), "renamed", "ORACLE", "SOURCE",
                "oracledev", 1521, "XEPDB1", "dbzuser", "secret", "another");
        assertThat(service.update(saved.id(), renamed).topicPrefix()).isEqualTo("src_dev_1");
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

    private static DbConnection databricks(String name, String password, String httpPath, String volume) {
        return new DbConnection(null, name, "DATABRICKS", "TARGET", "dbc-x.cloud.databricks.com", 0,
                "workspace", null, password, null, httpPath, volume);
    }

    @org.junit.jupiter.api.AfterEach
    void resetEnv() {
        DatabricksSupport.setEnvForTest(null);
    }

    @Test
    void Databricks는_port_443과_username_token으로_보정돼_저장된다() {
        DbConnection saved = service.create(databricks("dbx", "dapi-1", "/sql/1.0/warehouses/a", "default.dz_staging"));
        assertThat(saved.port()).isEqualTo(443);
        assertThat(saved.username()).isEqualTo("token");
        DbConnection loaded = service.get(saved.id());
        assertThat(loaded.httpPath()).isEqualTo("/sql/1.0/warehouses/a");
        assertThat(loaded.stagingVolume()).isEqualTo("default.dz_staging");
        assertThat(loaded.databaseName()).isEqualTo("workspace");
    }

    @Test
    void Databricks_필수값이_빠지면_거부한다() {
        assertThatThrownBy(() -> service.create(databricks("d1", "t", null, "default.v")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("httpPath");
        assertThatThrownBy(() -> service.create(databricks("d2", "t", "/sql/1.0/warehouses/a", null)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Volume");
        assertThatThrownBy(() -> service.create(databricks("d3", "t", "/sql/1.0/warehouses/a", "novolume")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("<schema>.<volume>");
    }

    @Test
    void Databricks_토큰이_없으면_환경변수로_폴백하고_둘_다_없으면_거부한다() {
        DatabricksSupport.setEnvForTest(k -> null);
        assertThatThrownBy(() -> service.create(databricks("d4", "", "/sql/1.0/warehouses/a", "default.v")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("DZ_DATABRICKS_TOKEN");

        DatabricksSupport.setEnvForTest(k -> "DZ_DATABRICKS_TOKEN".equals(k) ? "env-token" : null);
        DbConnection saved = service.create(databricks("d5", "", "/sql/1.0/warehouses/a", "default.v"));
        assertThat(DatabricksSupport.token(saved)).isEqualTo("env-token");
    }

    @Test
    void Databricks는_소스로_등록할_수_없다() {
        DbConnection asSource = new DbConnection(null, "dbxs", "DATABRICKS", "SOURCE", "h", 443, "c", "token", "t",
                null, "/p", "s.v");
        assertThatThrownBy(() -> service.create(asSource)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void Databricks_수정_시_빈_password는_기존_토큰을_유지한다() {
        DbConnection saved = service.create(databricks("d6", "dapi-keep", "/sql/1.0/warehouses/a", "default.v"));
        service.update(saved.id(), new DbConnection(saved.id(), "d6", "DATABRICKS", "TARGET", "dbc-y.cloud.databricks.com",
                443, "workspace", "token", "", null, "/sql/1.0/warehouses/b", "default.v"));
        DbConnection after = service.get(saved.id());
        assertThat(after.password()).isEqualTo("dapi-keep");
        assertThat(after.host()).isEqualTo("dbc-y.cloud.databricks.com");
        assertThat(after.httpPath()).isEqualTo("/sql/1.0/warehouses/b");
    }
}
