package io.deltazium.recovery.envelope;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * envelope 왕복 테스트 (CLAUDE.md 필수):
 * 원본 envelope payload → Iceberg changelog 행(envelope-as-is, 5.1절 개정판) →
 * 재조립 envelope payload 동등성. 5.1절 무손실 불변식의 회귀 방어선.
 *
 * recovery-sink(JDBC sink)가 live와 동일하게 apply할 수 있는 형태
 * (schemas.enabled JSON + PK key)가 재조립의 목표다.
 */
class EnvelopeRoundTripTest {

    private final ObjectMapper json = new ObjectMapper();
    private final ConnectJsonAssembler assembler = new ConnectJsonAssembler();

    /** 5.1절 changelog 스키마 — before/after는 소스 테이블 컬럼, source는 envelope 원본 */
    private static Types.StructType rowType(int base) {
        return Types.StructType.of(
                Types.NestedField.optional(base, "ID", Types.LongType.get()),
                Types.NestedField.optional(base + 1, "AMOUNT", Types.StringType.get()),
                Types.NestedField.optional(base + 2, "STATUS", Types.StringType.get()));
    }

    private static Schema changelogSchema() {
        return new Schema(
                Types.NestedField.optional(1, "op", Types.StringType.get()),
                Types.NestedField.optional(2, "ts_ms", Types.LongType.get()),
                Types.NestedField.optional(3, "source", Types.StructType.of(
                        Types.NestedField.optional(4, "scn", Types.StringType.get()),
                        Types.NestedField.optional(5, "txId", Types.StringType.get()),
                        Types.NestedField.optional(6, "ts_ms", Types.LongType.get()),
                        Types.NestedField.optional(7, "schema", Types.StringType.get()),
                        Types.NestedField.optional(8, "table", Types.StringType.get()))),
                Types.NestedField.optional(20, "before", rowType(21)),
                Types.NestedField.optional(30, "after", rowType(31)));
    }

    /** 원본 envelope payload(JSON) → sink가 적재했을 changelog 행 재현 */
    private static GenericRecord toChangelogRow(JsonNode payload, Schema schema) {
        GenericRecord row = GenericRecord.create(schema.asStruct());
        row.setField("op", payload.get("op").asText());
        row.setField("ts_ms", payload.get("ts_ms").asLong());

        GenericRecord source = GenericRecord.create(
                schema.findField("source").type().asStructType());
        JsonNode src = payload.get("source");
        source.setField("scn", src.get("scn").asText());
        source.setField("txId", src.get("txId").asText());
        source.setField("ts_ms", src.get("ts_ms").asLong());
        source.setField("schema", src.get("schema").asText());
        source.setField("table", src.get("table").asText());
        row.setField("source", source);

        for (String side : new String[] {"before", "after"}) {
            JsonNode image = payload.get(side);
            if (image == null || image.isNull()) {
                continue;
            }
            GenericRecord rec = GenericRecord.create(
                    schema.findField(side).type().asStructType());
            rec.setField("ID", image.get("ID").asLong());
            rec.setField("AMOUNT", image.get("AMOUNT").asText());
            rec.setField("STATUS", image.get("STATUS").asText());
            row.setField(side, rec);
        }
        return row;
    }

    /** IntNode(1) vs LongNode(1) 같은 표현 차이는 동등으로 취급 (수치 값 기준) */
    private static final java.util.Comparator<JsonNode> NUMERIC_EQ = (a, b) -> {
        if (a.isNumber() && b.isNumber()) {
            return a.decimalValue().compareTo(b.decimalValue());
        }
        return a.equals(b) ? 0 : 1;
    };

    private static void assertJsonEquals(JsonNode expected, JsonNode actual) {
        assertTrue(expected.equals(NUMERIC_EQ, actual),
                "expected=" + expected + " actual=" + actual);
    }

    private void assertRoundTrip(String envelopePayload) throws Exception {
        JsonNode original = json.readTree(envelopePayload);
        Schema schema = changelogSchema();
        GenericRecord row = toChangelogRow(original, schema);

        ObjectNode rebuilt = assembler.value(schema, row);
        JsonNode payload = rebuilt.get("payload");

        assertJsonEquals(original.get("op"), payload.get("op"));
        assertJsonEquals(original.get("ts_ms"), payload.get("ts_ms"));
        assertJsonEquals(original.get("before"), payload.get("before"));
        assertJsonEquals(original.get("after"), payload.get("after"));
        assertJsonEquals(original.get("source"), payload.get("source"));
        // 스키마도 envelope 구조여야 recovery-sink가 파싱한다
        assertEquals("struct", rebuilt.get("schema").get("type").asText());
    }

    private static String envelope(String op, String before, String after, long scn) {
        return """
                { "op": "%s", "before": %s, "after": %s,
                  "source": {"scn": "%d", "txId": "tx-%d", "ts_ms": 1753300000000,
                             "schema": "CDC", "table": "AUTO_100"},
                  "ts_ms": 1753300000123 }""".formatted(op, before, after, scn, scn);
    }

    private static final String ROW_V1 = "{\"ID\": 1, \"AMOUNT\": \"120.50\", \"STATUS\": \"NEW\"}";
    private static final String ROW_V2 = "{\"ID\": 1, \"AMOUNT\": \"99.00\", \"STATUS\": \"PAID\"}";

    @Test
    void insert_왕복() throws Exception {
        assertRoundTrip(envelope("c", "null", ROW_V1, 1000));
    }

    @Test
    void update_왕복_before_유지() throws Exception {
        assertRoundTrip(envelope("u", ROW_V1, ROW_V2, 1010));
    }

    @Test
    void delete_왕복() throws Exception {
        assertRoundTrip(envelope("d", ROW_V2, "null", 1020));
    }

    @Test
    void snapshot_read_왕복() throws Exception {
        assertRoundTrip(envelope("r", "null", ROW_V1, 900));
    }

    @Test
    void key는_after의_PK_컬럼으로_만든다() throws Exception {
        Schema schema = changelogSchema();
        GenericRecord row = toChangelogRow(
                json.readTree(envelope("c", "null", ROW_V1, 1000)), schema);
        ObjectNode key = assembler.key(schema, row, List.of("ID"));
        assertEquals(1L, key.get("payload").get("ID").asLong());
        assertEquals("int64", key.get("schema").get("fields").get(0).get("type").asText());
        assertEquals("ID", key.get("schema").get("fields").get(0).get("field").asText());
    }

    @Test
    void delete는_before에서_key를_만든다() throws Exception {
        Schema schema = changelogSchema();
        GenericRecord row = toChangelogRow(
                json.readTree(envelope("d", ROW_V2, "null", 1020)), schema);
        ObjectNode key = assembler.key(schema, row, List.of("ID"));
        assertEquals(1L, key.get("payload").get("ID").asLong());
    }

    @Test
    void before_after_모두_없으면_key_불가() throws Exception {
        Schema schema = changelogSchema();
        GenericRecord row = toChangelogRow(
                json.readTree(envelope("c", "null", ROW_V1, 1)), schema);
        row.setField("after", null);
        assertNull(assembler.key(schema, row, List.of("ID")));
    }

    @Test
    void decimal_타입은_Connect_Decimal_스키마로_나온다() {
        Schema schema = new Schema(Types.NestedField.optional(1, "after",
                Types.StructType.of(Types.NestedField.optional(2, "AMT",
                        Types.DecimalType.of(14, 2)))));
        GenericRecord row = GenericRecord.create(schema.asStruct());
        GenericRecord after = GenericRecord.create(
                schema.findField("after").type().asStructType());
        after.setField("AMT", new java.math.BigDecimal("120.50"));
        row.setField("after", after);

        ObjectNode value = assembler.value(schema, row);
        JsonNode amt = value.get("schema").get("fields").get(0).get("fields").get(0);
        assertEquals("bytes", amt.get("type").asText());
        assertEquals("org.apache.kafka.connect.data.Decimal", amt.get("name").asText());
        assertEquals("2", amt.get("parameters").get("scale").asText());
        // payload는 unscaled 값(12050)의 base64
        byte[] decoded = java.util.Base64.getDecoder().decode(
                value.get("payload").get("after").get("AMT").asText());
        assertTrue(new java.math.BigInteger(decoded).intValue() == 12050);
    }
}
