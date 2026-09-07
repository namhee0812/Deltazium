package io.deltazium.backend.connect;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import io.deltazium.backend.template.TemplateRenderer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.http.HttpMethod.PUT;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.jsonPath;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

/**
 * 파일명 : ConnectorDeployServiceTest.java
 * 작성일자 : 26. 07. 24.
 * 작성자 : 최남희
 * 설명 : 커넥터 템플릿 렌더링·배포 서비스 단위 테스트.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 24.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ②: "source" 템플릿이 source-oracle로
 * |                          | 분리됨에 따라 갱신, source-postgresql 렌더링 테스트 추가
 * --------------------------------------------------
 */
class ConnectorDeployServiceTest {

    private MockRestServiceServer server;
    private ConnectorDeployService service;

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder();
        server = MockRestServiceServer.bindTo(builder).build();
        // repo의 실제 connectors/ 템플릿으로 검증 (backend/에서 실행되므로 ..)
        service = new ConnectorDeployService(
                new TemplateRenderer("../connectors"),
                new ConnectClient(builder, "http://connect-test"));
    }

    private static Map<String, String> sourceVars() {
        Map<String, String> vars = new HashMap<>();
        vars.put("connector_name", "dz-source");
        vars.put("oracle_host", "oracledev");
        vars.put("oracle_port", "1521");
        vars.put("oracle_user", "dbzuser");
        vars.put("oracle_password", "pw");
        vars.put("oracle_dbname", "XE");
        vars.put("topic_prefix", "dz");
        vars.put("table_include_list", "SRC.ORDERS");
        vars.put("kafka_bootstrap", "localhost:9092");
        vars.put("snapshot_mode", "initial");
        return vars;
    }

    @Test
    void source_템플릿을_렌더링해_config만_PUT한다() {
        server.expect(requestTo("http://connect-test/connectors/dz-source/config"))
                .andExpect(method(PUT))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.['connector.class']")
                        .value("io.debezium.connector.oracle.OracleConnector"))
                .andExpect(jsonPath("$.['table.include.list']").value("SRC.ORDERS"))
                .andRespond(withSuccess("{\"name\":\"dz-source\"}", MediaType.APPLICATION_JSON));

        JsonNode result = service.deploy("source-oracle", sourceVars());

        assertThat(result.get("name").asText()).isEqualTo("dz-source");
        server.verify();
    }

    private static Map<String, String> pgSourceVars() {
        Map<String, String> vars = new HashMap<>();
        vars.put("connector_name", "dz-source-pgsrc");
        vars.put("pg_host", "pgdev");
        vars.put("pg_port", "5432");
        vars.put("pg_user", "dz_capture");
        vars.put("pg_password", "pw");
        vars.put("pg_dbname", "cdc");
        vars.put("topic_prefix", "pgsrc");
        vars.put("table_include_list", "cdc_src.orders");
        vars.put("snapshot_mode", "initial");
        vars.put("slot_name", "dz_pgsrc");
        vars.put("publication_name", "dz_pgsrc");
        return vars;
    }

    @Test
    void source_postgresql_템플릿을_렌더링해_config만_PUT한다() {
        server.expect(requestTo("http://connect-test/connectors/dz-source-pgsrc/config"))
                .andExpect(method(PUT))
                .andExpect(jsonPath("$.['connector.class']")
                        .value("io.debezium.connector.postgresql.PostgresConnector"))
                .andExpect(jsonPath("$.['plugin.name']").value("pgoutput"))
                .andExpect(jsonPath("$.['publication.autocreate.mode']").value("filtered"))
                .andExpect(jsonPath("$.['table.include.list']").value("cdc_src.orders"))
                .andRespond(withSuccess("{\"name\":\"dz-source-pgsrc\"}", MediaType.APPLICATION_JSON));

        JsonNode result = service.deploy("source-postgresql", pgSourceVars());

        assertThat(result.get("name").asText()).isEqualTo("dz-source-pgsrc");
        server.verify();
    }

    @Test
    void 허용되지_않은_템플릿이면_거부한다() {
        assertThatThrownBy(() -> service.deploy("../etc/passwd", sourceVars()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void connector_name이_없으면_거부한다() {
        Map<String, String> vars = sourceVars();
        vars.remove("connector_name");
        assertThatThrownBy(() -> service.deploy("source-oracle", vars))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("connector_name");
    }
}
