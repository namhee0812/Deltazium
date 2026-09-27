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
import static org.springframework.http.HttpMethod.POST;
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
 * 26. 09. 27.       | 최남희  | resumeAfterDdl 테스트 추가 — FAILED/PAUSED/RUNNING 분기
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

    @Test
    void resumeAfterDdl_FAILED이면_restartFailed를_호출한다() {
        server.expect(requestTo("http://connect-test/connectors/x/status"))
                .andRespond(withSuccess(
                        "{\"connector\":{\"state\":\"RUNNING\"},\"tasks\":[{\"state\":\"FAILED\"}]}",
                        MediaType.APPLICATION_JSON));
        server.expect(requestTo("http://connect-test/connectors/x/restart?includeTasks=true&onlyFailed=true"))
                .andExpect(method(POST))
                .andRespond(withSuccess());

        client.resumeAfterDdl("x");

        server.verify();
    }

    @Test
    void resumeAfterDdl_PAUSED면_resume을_호출한다() {
        server.expect(requestTo("http://connect-test/connectors/x/status"))
                .andRespond(withSuccess("{\"connector\":{\"state\":\"PAUSED\"},\"tasks\":[]}",
                        MediaType.APPLICATION_JSON));
        server.expect(requestTo("http://connect-test/connectors/x/resume"))
                .andExpect(method(PUT))
                .andRespond(withSuccess());

        client.resumeAfterDdl("x");

        server.verify();
    }

    @Test
    void resumeAfterDdl_RUNNING이면_아무것도_하지_않는다() {
        server.expect(requestTo("http://connect-test/connectors/x/status"))
                .andRespond(withSuccess("{\"connector\":{\"state\":\"RUNNING\"},\"tasks\":[]}",
                        MediaType.APPLICATION_JSON));

        client.resumeAfterDdl("x");

        server.verify();
    }
}
