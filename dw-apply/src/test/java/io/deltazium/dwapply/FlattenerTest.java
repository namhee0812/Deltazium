package io.deltazium.dwapply;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

/**
 * 파일명 : FlattenerTest.java
 * 작성일자 : 26. 10. 19.
 * 작성자 : 최남희
 * 설명 : envelope 평탄화 — c/u/r/d/tombstone/t, 논리 타입 변환 골든.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 타임스탬프 골든을 공백 구분 포맷으로, ZonedTimestamp UTC 정규화(오프셋·가변 소수) 검증
 * --------------------------------------------------
 */
class FlattenerTest {

    private static final ObjectMapper M = new ObjectMapper();
    private final Flattener f = new Flattener();

    private static final TableSpec SPEC = new TableSpec(List.of("dz1.T.ORDERS"), "default", "orders",
            List.of("id"), List.of(new ColumnMapping("ID", "id"), new ColumnMapping("NAME", "name"),
                    new ColumnMapping("AMT", "amt"), new ColumnMapping("D", "d"), new ColumnMapping("TS", "ts"),
                    new ColumnMapping("TSU", "tsu"), new ColumnMapping("TSN", "tsn"),
                    new ColumnMapping("TSZ", "tsz")));

    private static final String FIELDS = """
            [{"field":"ID","type":"int64"},{"field":"NAME","type":"string"},
             {"field":"AMT","type":"bytes","name":"org.apache.kafka.connect.data.Decimal","parameters":{"scale":"2"}},
             {"field":"D","type":"int32","name":"io.debezium.time.Date"},
             {"field":"TS","type":"int64","name":"io.debezium.time.Timestamp"},
             {"field":"TSU","type":"int64","name":"io.debezium.time.MicroTimestamp"},
             {"field":"TSN","type":"int64","name":"io.debezium.time.NanoTimestamp"},
             {"field":"TSZ","type":"string","name":"io.debezium.time.ZonedTimestamp"}]""";

    private static String env(String op, String before, String after) {
        return """
                {"schema":{"type":"struct","fields":[
                  {"field":"before","type":"struct","fields":%s},
                  {"field":"after","type":"struct","fields":%s}]},
                 "payload":{"op":"%s","before":%s,"after":%s,"source":{"scn":"1"}}}"""
                .formatted(FIELDS, FIELDS, op, before, after);
    }

    private static final String ROW = """
            {"ID":7,"NAME":"kim","AMT":"MDk=","D":19723,"TS":1704110400123,
             "TSU":1704110400123456,"TSN":1704110400123456789,"TSZ":"2024-01-01T12:00:00Z"}""";

    @Test
    void createAndSnapshotAndUpdateFlattenAfterWithLogicalTypes() throws Exception {
        for (String op : new String[] {"c", "r", "u"}) {
            FlatRow r = f.flatten(SPEC, "dz1.T.ORDERS", 2, 41L, env(op, "null", ROW)).orElseThrow();
            assertEquals("{\"id\":7,\"name\":\"kim\",\"amt\":123.45,\"d\":\"2024-01-01\","
                    + "\"ts\":\"2024-01-01 12:00:00.123000\",\"tsu\":\"2024-01-01 12:00:00.123456\","
                    + "\"tsn\":\"2024-01-01 12:00:00.123456\",\"tsz\":\"2024-01-01 12:00:00.000000\","
                    + "\"_op\":\"" + op + "\",\"_offset\":41,\"_part\":2}", r.line());
            assertEquals(r.line().length() + 1, r.bytes());
        }
    }

    @Test
    void deleteKeepsOnlyKeyColumnsFromBefore() throws Exception {
        FlatRow r = f.flatten(SPEC, "dz1.T.ORDERS", 0, 5L, env("d", ROW, "null")).orElseThrow();
        JsonNode n = M.readTree(r.line());
        assertEquals(7, n.get("id").asInt());
        assertTrue(n.get("name").isNull() && n.get("amt").isNull() && n.get("ts").isNull());
        assertEquals("d", n.get("_op").asText());
    }

    @Test
    void tombstoneTruncateAndUnknownOpAreSkipped() throws Exception {
        assertTrue(f.flatten(SPEC, "t", 0, 1, null).isEmpty());
        assertTrue(f.flatten(SPEC, "t", 0, 1, env("t", "null", "null")).isEmpty());
        assertTrue(f.flatten(SPEC, "t", 0, 1, env("x", "null", ROW)).isEmpty());
    }

    @Test
    void negativeMicroTimestampFloorsCorrectly() throws Exception {
        String row = """
                {"ID":1,"TSU":-1}""";
        FlatRow r = f.flatten(SPEC, "t", 0, 1, env("c", "null", row)).orElseThrow();
        assertEquals("1969-12-31 23:59:59.999999", M.readTree(r.line()).get("tsu").asText());
    }

    @Test
    void zonedTimestampIsNormalizedToUtc() throws Exception {
        String row = """
                {"ID":1,"TSZ":"2024-01-01T21:00:00.5+09:00"}""";
        FlatRow r = f.flatten(SPEC, "t", 0, 1, env("c", "null", row)).orElseThrow();
        assertEquals("2024-01-01 12:00:00.500000", M.readTree(r.line()).get("tsz").asText());
    }
}
