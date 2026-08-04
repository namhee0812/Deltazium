package io.deltazium.backend.connect;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * 파일명 : ConnectClient.java
 * 작성일자 : 26. 07. 24.
 * 작성자 : 최남희
 * 설명 : Kafka Connect REST API 클라이언트 (제어면의 유일한 커넥터 조작 경로).
 * https://kafka.apache.org/documentation/#connect_rest
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 24.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 08. 04.       | 최남희  | restartFailed 추가 — FAILED task 재시작 (KIP-745)
 * --------------------------------------------------
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

    public JsonNode getConfig(String name) {
        return rest.get().uri("/connectors/{name}/config", name).retrieve().body(JsonNode.class);
    }

    /** 커넥터 생성 또는 설정 갱신 (PUT /connectors/{name}/config — 멱등). */
    public JsonNode upsert(String name, JsonNode config) {
        return rest.put().uri("/connectors/{name}/config", name)
                .body(config).retrieve().body(JsonNode.class);
    }

    public void pause(String name) {
        rest.put().uri("/connectors/{name}/pause", name).retrieve().toBodilessEntity();
    }

    /** STOPPED 상태로 전환 (offset 삭제의 전제 조건). */
    public void stop(String name) {
        rest.put().uri("/connectors/{name}/stop", name).retrieve().toBodilessEntity();
    }

    /** 커넥터 offset 삭제 — STOPPED 상태에서만 허용된다. */
    public void deleteOffsets(String name) {
        rest.delete().uri("/connectors/{name}/offsets", name).retrieve().toBodilessEntity();
    }

    public void resume(String name) {
        rest.put().uri("/connectors/{name}/resume", name).retrieve().toBodilessEntity();
    }

    /** 커넥터 + FAILED task 재시작 (KIP-745). 원인 해결 후 재시도 경로. */
    public void restartFailed(String name) {
        rest.post().uri("/connectors/{name}/restart?includeTasks=true&onlyFailed=true", name)
                .retrieve().toBodilessEntity();
    }

    public void delete(String name) {
        rest.delete().uri("/connectors/{name}", name).retrieve().toBodilessEntity();
    }
}
