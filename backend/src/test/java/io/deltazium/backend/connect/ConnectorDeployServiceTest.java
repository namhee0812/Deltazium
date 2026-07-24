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

        JsonNode result = service.deploy("source", sourceVars());

        assertThat(result.get("name").asText()).isEqualTo("dz-source");
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
        assertThatThrownBy(() -> service.deploy("source", vars))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("connector_name");
    }
}
