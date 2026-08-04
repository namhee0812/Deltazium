package io.deltazium.backend.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.http.HttpMethod.PUT;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import org.springframework.http.MediaType;

/**
 * 파일명 : ConnectClientTest.java
 * 작성일자 : 26. 07. 24.
 * 작성자 : 최남희
 * 설명 : Kafka Connect REST 클라이언트 단위 테스트.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 24.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
class ConnectClientTest {

    private MockRestServiceServer server;
    private ConnectClient client;

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder();
        server = MockRestServiceServer.bindTo(builder).build();
        client = new ConnectClient(builder, "http://connect-test");
    }

    @Test
    void 커넥터_목록은_status_확장으로_조회한다() {
        server.expect(requestTo("http://connect-test/connectors?expand=status"))
                .andRespond(withSuccess("{\"src\":{}}", MediaType.APPLICATION_JSON));
        JsonNode result = client.listConnectors();
        assertThat(result.has("src")).isTrue();
        server.verify();
    }

    @Test
    void upsert는_PUT_config로_멱등_배포한다() throws Exception {
        server.expect(requestTo("http://connect-test/connectors/src-1/config"))
                .andExpect(method(PUT))
                .andRespond(withSuccess("{\"name\":\"src-1\"}", MediaType.APPLICATION_JSON));
        JsonNode config = new ObjectMapper().readTree("{\"connector.class\":\"x\"}");
        JsonNode result = client.upsert("src-1", config);
        assertThat(result.get("name").asText()).isEqualTo("src-1");
        server.verify();
    }
}
