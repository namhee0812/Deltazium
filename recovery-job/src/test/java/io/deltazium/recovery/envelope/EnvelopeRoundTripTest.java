package io.deltazium.recovery.envelope;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * envelope 왕복 테스트 (CLAUDE.md 필수):
 * 원본 envelope payload → changelog 레코드(5.1절) → 재조립 envelope payload.
 * 5.1절 "이 컬럼들만으로 envelope을 손실 없이 재조립" 불변식의 회귀 방어선.
 *
 * 동등성 범위는 EnvelopeMapper 주석 참조 — op/before/after 전체와
 * source의 scn/txId/ts_ms/schema/table. source의 부가 필드는 대상 아님.
 */
class EnvelopeRoundTripTest {

    private final ObjectMapper json = new ObjectMapper();
    private final EnvelopeMapper mapper = new EnvelopeMapper();

    private static final String SOURCE_COMMON = """
            "source": {
              "version": "3.6.0.Final", "connector": "oracle", "name": "dz",
              "ts_ms": 1753300000000, "snapshot": "false",
              "db": "XE", "schema": "SRC", "table": "ORDERS",
              "txId": "02000a00b3050000", "scn": "3452117", "commit_scn": "3452130"
            }""";

    private void assertRoundTrip(String payloadJson) throws Exception {
        JsonNode original = json.readTree(payloadJson);
        ChangelogRecord rec = mapper.toChangelog(original);
        JsonNode rebuilt = mapper.toEnvelopePayload(rec);

        assertEquals(original.get("op"), rebuilt.get("op"));
        assertEquals(original.get("before"), rebuilt.get("before"));
        assertEquals(original.get("after"), rebuilt.get("after"));
        JsonNode src = original.get("source");
        JsonNode rsrc = rebuilt.get("source");
        assertEquals(src.get("scn").asText(), rsrc.get("scn").asText());
        assertEquals(src.get("txId"), rsrc.get("txId"));
        assertEquals(src.get("ts_ms"), rsrc.get("ts_ms"));
        assertEquals(src.get("schema"), rsrc.get("schema"));
        assertEquals(src.get("table"), rsrc.get("table"));
    }

    @Test
    void insert_왕복() throws Exception {
        assertRoundTrip("""
                { "op": "c", "before": null,
                  "after": {"ID": 1, "AMOUNT": "120.50", "STATUS": "NEW"},
                  %s, "ts_ms": 1753300000123 }""".formatted(SOURCE_COMMON));
    }

    @Test
    void update_왕복_before_유지() throws Exception {
        assertRoundTrip("""
                { "op": "u",
                  "before": {"ID": 1, "AMOUNT": "120.50", "STATUS": "NEW"},
                  "after":  {"ID": 1, "AMOUNT": "99.00",  "STATUS": "PAID"},
                  %s, "ts_ms": 1753300000456 }""".formatted(SOURCE_COMMON));
    }

    @Test
    void delete_왕복() throws Exception {
        assertRoundTrip("""
                { "op": "d",
                  "before": {"ID": 1, "AMOUNT": "99.00", "STATUS": "PAID"},
                  "after": null,
                  %s, "ts_ms": 1753300000789 }""".formatted(SOURCE_COMMON));
    }

    @Test
    void snapshot_read_왕복() throws Exception {
        assertRoundTrip("""
                { "op": "r", "before": null,
                  "after": {"ID": 7, "AMOUNT": "1.00", "STATUS": "OLD"},
                  %s, "ts_ms": 1753300000001 }""".formatted(SOURCE_COMMON));
    }

    @Test
    void changelog_레코드가_5_1절_값을_담는다() throws Exception {
        JsonNode payload = json.readTree("""
                { "op": "c", "before": null, "after": {"ID": 1},
                  %s, "ts_ms": 1 }""".formatted(SOURCE_COMMON));
        ChangelogRecord rec = mapper.toChangelog(payload);
        assertEquals(3452117L, rec.scn());
        assertEquals("02000a00b3050000", rec.txId());
        assertEquals(1753300000000L, rec.tsMs());
        assertEquals("SRC.ORDERS", rec.sourceTable());
        assertTrue(rec.before() == null);
    }

    @Test
    void 필수_필드_누락이면_예외() throws Exception {
        JsonNode noSource = json.readTree("""
                { "op": "c", "before": null, "after": {"ID": 1}, "ts_ms": 1 }""");
        assertThrows(IllegalArgumentException.class, () -> mapper.toChangelog(noSource));
    }
}
