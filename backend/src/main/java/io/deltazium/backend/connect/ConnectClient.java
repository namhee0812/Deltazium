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
 * 26. 09. 27.       | 최남희  | resumeAfterDdl 추가 — DDL 승인·건너뛰기·자동 적용 후 sink 재개를 (보완: 커넥터 생성 직후 status 404를 최대 10초 재시도 — 복구 sink 재배포 시 500 실측)
 * |                          | 한곳에서(FAILED면 restartFailed, PAUSED면 resume, architecture.md 7절)
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

    /**
     * DDL 승인·건너뛰기(SKIPPED)·자동 적용 후 sink 재개 공용 진입점(architecture.md 7절) —
     * 상태를 조회해 FAILED면 restartFailed, PAUSED면 resume, 그 외(RUNNING 등)는 이미 정상이라
     * 아무것도 하지 않는다.
     */
    public void resumeAfterDdl(String name) {
        JsonNode status = null;
        // 커넥터를 막 만든 직후(복구 sink 재배포)엔 Connect가 status를 아직 안 만들어 404가 난다 —
        // 최대 10초 재시도 후에도 없으면 resume만 시도한다(2026-09-27 복구 재트리거 500 실측)
        for (int i = 0; i < 20 && status == null; i++) {
            try {
                status = status(name);
            } catch (org.springframework.web.client.HttpClientErrorException.NotFound e) {
                try { Thread.sleep(500); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
            }
        }
        if (status == null) {
            resume(name);
            return;
        }
        String state = effectiveState(status);
        if ("FAILED".equals(state)) {
            restartFailed(name);
        } else if ("PAUSED".equals(state)) {
            resume(name);
        }
    }

    /** 커넥터 자체 또는 태스크 중 하나라도 FAILED면 FAILED로 본다 — SystemWarningService·
     * ConnectorHealthWatcher와 같은 판정이지만 목적이 달라 각자 중복 정의돼 있다(docs/internals.md). */
    private static String effectiveState(JsonNode status) {
        String state = status.path("connector").path("state").asText("UNKNOWN");
        for (JsonNode task : status.path("tasks")) {
            if ("FAILED".equals(task.path("state").asText())) {
                return "FAILED";
            }
        }
        return state;
    }
}
