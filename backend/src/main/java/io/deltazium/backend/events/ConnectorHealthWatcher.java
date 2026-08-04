package io.deltazium.backend.events;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import io.deltazium.backend.connect.ConnectClient;
import io.deltazium.backend.registration.RegisteredTable;
import io.deltazium.backend.registration.RegisteredTableRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 파일명 : ConnectorHealthWatcher.java
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : 커넥터 상태 전이 감시 — RUNNING→비정상 전이를 테이블 이벤트로 적재 (trace 포함).
 * "언제부터 왜 죽었나"를 UI 타임라인에 남기는 것이 목적.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@Component
@ConditionalOnProperty(name = "deltazium.health-watcher.enabled", havingValue = "true", matchIfMissing = true)
public class ConnectorHealthWatcher {

    private static final Logger log = LoggerFactory.getLogger(ConnectorHealthWatcher.class);

    private final ConnectClient connect;
    private final RegisteredTableRepository registrations;
    private final TableEventService events;
    /** connector명 → 마지막 관측 상태 (전이 감지용) */
    private final Map<String, String> lastStates = new HashMap<>();

    public ConnectorHealthWatcher(ConnectClient connect,
                                  RegisteredTableRepository registrations,
                                  TableEventService events) {
        this.connect = connect;
        this.registrations = registrations;
        this.events = events;
    }

    @Scheduled(fixedDelayString = "${deltazium.health-watcher.interval-ms:30000}")
    public void poll() {
        JsonNode all;
        try {
            all = connect.listConnectors();
        } catch (Exception e) {
            log.debug("Connect 상태 폴링 실패: {}", e.getMessage());
            return;
        }
        Iterator<String> names = all.fieldNames();
        while (names.hasNext()) {
            String name = names.next();
            JsonNode status = all.get(name).path("status");
            String state = effectiveState(status);
            String prev = lastStates.put(name, state);
            if (prev == null || prev.equals(state)) {
                continue;
            }
            RegisteredTable table = tableOf(name);
            String schema = table != null ? table.schemaName() : "-";
            String tbl = table != null ? table.tableName() : name;
            if (isBroken(state) && !isBroken(prev)) {
                events.record(schema, tbl, "CONNECTOR_FAILED", "ERROR",
                        name + " " + prev + " → " + state, trace(status));
            } else if (!isBroken(state) && isBroken(prev)) {
                events.info(schema, tbl, "CONNECTOR_RECOVERED", name + " " + prev + " → " + state);
            }
        }
    }

    /** 커넥터 자체 또는 태스크 중 하나라도 FAILED면 FAILED로 본다. */
    private static String effectiveState(JsonNode status) {
        String state = status.path("connector").path("state").asText("UNKNOWN");
        for (JsonNode task : status.path("tasks")) {
            if ("FAILED".equals(task.path("state").asText())) {
                return "FAILED";
            }
        }
        return state;
    }

    private static boolean isBroken(String state) {
        return "FAILED".equals(state) || "UNKNOWN".equals(state);
    }

    private static String trace(JsonNode status) {
        for (JsonNode task : status.path("tasks")) {
            if (task.hasNonNull("trace")) {
                return task.get("trace").asText();
            }
        }
        return status.path("connector").path("trace").asText(null);
    }

    /** 커넥터 이름에서 대상 테이블 추정 — 전역 커넥터(dz-source 등)는 등록 1건이면 그 테이블로 귀속 */
    private RegisteredTable tableOf(String connectorName) {
        var all = registrations.findAll();
        for (RegisteredTable t : all) {
            if (connectorName.endsWith(t.suffix())) {
                return t;
            }
        }
        return all.size() == 1 ? all.get(0) : null;
    }
}
