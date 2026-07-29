package io.deltazium.backend.ddl;

import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Debezium schema change 이벤트(JSON converter, schemas.enabled) 파싱.
 * 구조: {schema, payload:{source:{snapshot,scn,...}, ts_ms, databaseName, ddl, tableChanges:[{type,id}]}}
 */
final class DdlEventParser {

    record Parsed(long tsMs, String scn, String schemaName, String tableName,
                  String ddl, boolean snapshot) {
    }

    private static final ObjectMapper JSON = new ObjectMapper();

    private DdlEventParser() {
    }

    static Optional<Parsed> parse(String value) {
        try {
            JsonNode root = JSON.readTree(value);
            JsonNode p = root.has("payload") ? root.get("payload") : root;
            JsonNode ddl = p.get("ddl");
            if (ddl == null || ddl.isNull() || ddl.asText().isBlank()) {
                return Optional.empty();
            }
            JsonNode source = p.path("source");
            String snapshot = source.path("snapshot").asText("false");
            String schema = null;
            String table = null;
            JsonNode changes = p.path("tableChanges");
            if (changes.isArray() && !changes.isEmpty()) {
                // id 형식: "SCHEMA.TABLE" 또는 "\"SCHEMA\".\"TABLE\""
                String id = changes.get(0).path("id").asText("").replace("\"", "");
                int dot = id.indexOf('.');
                if (dot > 0) {
                    schema = id.substring(0, dot);
                    table = id.substring(dot + 1);
                }
            }
            return Optional.of(new Parsed(
                    p.path("ts_ms").asLong(0),
                    source.path("scn").isMissingNode() || source.path("scn").isNull()
                            ? null : source.path("scn").asText(),
                    schema, table, ddl.asText(),
                    !"false".equalsIgnoreCase(snapshot)));
        } catch (Exception e) {
            return Optional.empty(); // 형식 밖 이벤트는 건너뛴다 (poller가 로그로 남김)
        }
    }
}
