package io.deltazium.backend.connect;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * Kafka Connect REST API 클라이언트 (제어면의 유일한 커넥터 조작 경로).
 * https://kafka.apache.org/documentation/#connect_rest
 */
@Component
public class ConnectClient {

    private final RestClient rest;

    public ConnectClient(RestClient.Builder builder,
                         @Value("${deltazium.connect.base-url}") String baseUrl) {
        this.rest = builder.baseUrl(baseUrl).build();
    }

    /** 커넥터 목록 + 상태 (?expand=status). */
    public JsonNode listConnectors() {
        return rest.get().uri("/connectors?expand=status").retrieve().body(JsonNode.class);
    }

    public JsonNode status(String name) {
        return rest.get().uri("/connectors/{name}/status", name).retrieve().body(JsonNode.class);
    }

    /** 커넥터 생성 또는 설정 갱신 (PUT /connectors/{name}/config — 멱등). */
    public JsonNode upsert(String name, JsonNode config) {
        return rest.put().uri("/connectors/{name}/config", name)
                .body(config).retrieve().body(JsonNode.class);
    }

    public void pause(String name) {
        rest.put().uri("/connectors/{name}/pause", name).retrieve().toBodilessEntity();
    }

    public void resume(String name) {
        rest.put().uri("/connectors/{name}/resume", name).retrieve().toBodilessEntity();
    }

    public void delete(String name) {
        rest.delete().uri("/connectors/{name}", name).retrieve().toBodilessEntity();
    }
}
