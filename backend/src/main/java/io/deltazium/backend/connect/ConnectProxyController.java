package io.deltazium.backend.connect;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 파일명 : ConnectProxyController.java
 * 작성일자 : 26. 07. 24.
 * 작성자 : 최남희
 * 설명 : UI ⇄ Connect REST 프록시. UI는 Connect에 직접 붙지 않는다 (제어면 단일 경로).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 24.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 08. 04.       | 최남희  | POST /{name}/restart 추가 — FAILED task 재시도
 * --------------------------------------------------
 */
@RestController
@RequestMapping("/api/connectors")
public class ConnectProxyController {

    private final ConnectClient connect;

    public ConnectProxyController(ConnectClient connect) {
        this.connect = connect;
    }

    @GetMapping
    public JsonNode list() {
        return connect.listConnectors();
    }

    @GetMapping("/{name}/status")
    public JsonNode status(@PathVariable String name) {
        return connect.status(name);
    }

    @PutMapping("/{name}/config")
    public JsonNode upsert(@PathVariable String name, @RequestBody JsonNode config) {
        return connect.upsert(name, config);
    }

    @PutMapping("/{name}/pause")
    public ResponseEntity<Void> pause(@PathVariable String name) {
        connect.pause(name);
        return ResponseEntity.accepted().build();
    }

    @PutMapping("/{name}/resume")
    public ResponseEntity<Void> resume(@PathVariable String name) {
        connect.resume(name);
        return ResponseEntity.accepted().build();
    }

    /** 원인 해결 후 재시도 — FAILED된 커넥터·task만 재시작. */
    @PostMapping("/{name}/restart")
    public ResponseEntity<Void> restart(@PathVariable String name) {
        connect.restartFailed(name);
        return ResponseEntity.accepted().build();
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<Void> delete(@PathVariable String name) {
        connect.delete(name);
        return ResponseEntity.noContent().build();
    }
}
