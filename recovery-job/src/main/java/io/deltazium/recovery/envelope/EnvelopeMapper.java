package io.deltazium.recovery.envelope;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Debezium envelope payload ↔ changelog 레코드(5.1절) 변환.
 *
 * 재조립 동등성의 정의(왕복 테스트가 검증하는 범위):
 * - apply에 필요한 것: op, before, after
 * - 재생 순서·경계에 필요한 것: source.scn, source.txId, source.ts_ms
 * source의 나머지 필드(version, connector, snapshot 등)는 apply 시맨틱에 관여하지 않으므로
 * 재조립 시 복원 대상이 아니다. 재조립 envelope의 source는 위 3개 + schema/table만 담는다.
 */
public final class EnvelopeMapper {

    private final ObjectMapper mapper = new ObjectMapper();

    /** envelope payload(JSON converter의 payload 부분) → changelog 레코드. */
    public ChangelogRecord toChangelog(JsonNode payload) {
        JsonNode source = require(payload, "source");
        String schema = require(source, "schema").asText();
        String table = require(source, "table").asText();
        return new ChangelogRecord(
                require(payload, "op").asText(),
                nullable(payload.get("before")),
                nullable(payload.get("after")),
                // Debezium Oracle의 source.scn은 문자열 — 5.1절 스키마에 맞춰 long으로 고정
                Long.parseLong(require(source, "scn").asText()),
                require(source, "txId").asText(),
                require(source, "ts_ms").asLong(),
                schema + "." + table);
    }

    /** changelog 레코드 → 복구 토픽에 발행할 envelope payload 재조립. */
    public ObjectNode toEnvelopePayload(ChangelogRecord rec) {
        int dot = rec.sourceTable().indexOf('.');
        if (dot <= 0 || dot == rec.sourceTable().length() - 1) {
            throw new IllegalArgumentException("source_table 형식 오류: " + rec.sourceTable());
        }
        ObjectNode source = mapper.createObjectNode();
        source.put("scn", String.valueOf(rec.scn()));
        source.put("txId", rec.txId());
        source.put("ts_ms", rec.tsMs());
        source.put("schema", rec.sourceTable().substring(0, dot));
        source.put("table", rec.sourceTable().substring(dot + 1));

        ObjectNode payload = mapper.createObjectNode();
        payload.put("op", rec.op());
        payload.set("before", rec.before() == null ? NullNode.getInstance() : rec.before());
        payload.set("after", rec.after() == null ? NullNode.getInstance() : rec.after());
        payload.set("source", source);
        payload.put("ts_ms", rec.tsMs());
        return payload;
    }

    private static JsonNode require(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull()) {
            throw new IllegalArgumentException("envelope 필수 필드 누락: " + field);
        }
        return v;
    }

    private static JsonNode nullable(JsonNode node) {
        return (node == null || node.isNull()) ? null : node;
    }
}
