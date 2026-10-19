package io.deltazium.dwapply;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 파일명 : Flattener.java
 * 작성일자 : 26. 10. 19.
 * 작성자 : 최남희
 * 설명 : Debezium envelope(JSON converter, schemas.enabled=true) → 타깃 컬럼명 기준 평탄 행.
 * 읽는 것은 payload.op/before/after와 schema 블록의 필드 타입뿐이다 — `source.*`는 읽지
 * 않는다(architecture.md 5.1 불변식 2). op가 c/r/u면 after 전체, d면 before의 키 컬럼만
 * (나머지 null), tombstone(null 값)·t·그 외는 skip. 추가 컬럼 _op/_offset/_part.
 * 논리 타입은 schema 블록의 필드 name/parameters를 보고 변환한다:
 * Decimal(base64+scale)→JSON 숫자, Date(epoch days)→yyyy-MM-dd,
 * Timestamp(ms)·MicroTimestamp(µs)·NanoTimestamp(ns)→yyyy-MM-dd HH:mm:ss.SSSSSS(UTC 해석,
 * 타임존 없음), ZonedTimestamp→UTC로 정규화해 같은 포맷. binary는 증분 1 범위 밖 — base64 문자열 그대로.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 타임스탬프 포맷을 `yyyy-MM-dd HH:mm:ss.SSSSSS`(공백 구분)로 변경, ZonedTimestamp를
 * |                          | OffsetDateTime 파싱→UTC 정규화 후 같은 포맷으로 출력 — Spark JSON 리더 기본 추론에 맡기면
 * |                          | 6자리 소수가 null로 떨어질 수 있어 read_files에 같은 포맷을 명시한다(MergeSqlBuilder)
 * --------------------------------------------------
 */
public final class Flattener {

    private static final Logger log = LoggerFactory.getLogger(Flattener.class);

    static final DateTimeFormatter TS_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private final ObjectMapper mapper = new ObjectMapper()
            .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN);

    /** 변환 대상이 아니면 Optional.empty (tombstone·t·알 수 없는 op·after 없음). */
    public Optional<FlatRow> flatten(TableSpec spec, String topic, int partition, long offset, String value)
            throws JsonProcessingException {
        if (value == null || value.isBlank()) {
            return Optional.empty(); // tombstone
        }
        JsonNode root = mapper.readTree(value);
        JsonNode payload = root.has("payload") ? root.get("payload") : root;
        if (payload == null || payload.isNull()) {
            return Optional.empty();
        }
        String op = payload.path("op").asText("");
        boolean delete;
        String side;
        switch (op) {
            case "c", "r", "u" -> {
                delete = false;
                side = "after";
            }
            case "d" -> {
                delete = true;
                side = "before";
            }
            default -> {
                log.warn("지원하지 않는 op 건너뜀 op='{}' topic={} partition={} offset={}", op, topic, partition, offset);
                return Optional.empty();
            }
        }
        JsonNode data = payload.get(side);
        if (data == null || data.isNull()) {
            log.warn("{}가 없는 이벤트 건너뜀 op={} topic={} partition={} offset={}", side, op, topic, partition, offset);
            return Optional.empty();
        }
        Map<String, JsonNode> fieldSchemas = fieldSchemas(root.path("schema"), side);
        ObjectNode out = mapper.createObjectNode();
        for (ColumnMapping m : spec.columns()) {
            if (delete && spec.keyColumns().stream().noneMatch(k -> k.equalsIgnoreCase(m.target()))) {
                out.set(m.target(), NullNode.instance); // d: 키 외 컬럼은 null
                continue;
            }
            String actual = actualName(data, m.source());
            JsonNode v = actual == null ? null : data.get(actual);
            out.set(m.target(), convert(v, actual == null ? null : fieldSchemas.get(actual)));
        }
        out.put("_op", op);
        out.put("_offset", offset);
        out.put("_part", partition);
        String line = mapper.writeValueAsString(out);
        return Optional.of(new FlatRow(topic, partition, offset, line,
                line.getBytes(StandardCharsets.UTF_8).length + 1));
    }

    /** 소스 컬럼명 정확 일치 우선, 없으면 대소문자 무시 일치. */
    private static String actualName(JsonNode data, String source) {
        if (data.has(source)) {
            return source;
        }
        var it = data.fieldNames();
        while (it.hasNext()) {
            String f = it.next();
            if (f.equalsIgnoreCase(source)) {
                return f;
            }
        }
        return null;
    }

    /** envelope schema.fields 중 field=before|after 의 하위 필드 스키마 → 필드명 맵. */
    private static Map<String, JsonNode> fieldSchemas(JsonNode schema, String side) {
        Map<String, JsonNode> m = new HashMap<>();
        for (JsonNode f : schema.path("fields")) {
            if (side.equals(f.path("field").asText())) {
                for (JsonNode c : f.path("fields")) {
                    m.put(c.path("field").asText(), c);
                }
            }
        }
        return m;
    }

    private static JsonNode convert(JsonNode v, JsonNode fs) {
        if (v == null || v.isNull()) {
            return NullNode.instance;
        }
        String logical = fs == null ? "" : fs.path("name").asText("");
        switch (logical) {
            case "org.apache.kafka.connect.data.Decimal" -> {
                int scale = Integer.parseInt(fs.path("parameters").path("scale").asText("0"));
                return decimal(v.asText(), scale);
            }
            case "io.debezium.data.VariableScaleDecimal" -> {
                // struct {scale:int, value:base64} — Oracle NUMBER 무정밀 등
                return decimal(v.path("value").asText(), v.path("scale").asInt(0));
            }
            case "io.debezium.time.Date", "org.apache.kafka.connect.data.Date" -> {
                return TextNode.valueOf(LocalDate.ofEpochDay(v.asLong()).toString());
            }
            case "io.debezium.time.Timestamp", "org.apache.kafka.connect.data.Timestamp" -> {
                return ts(Instant.ofEpochMilli(v.asLong()));
            }
            case "io.debezium.time.MicroTimestamp" -> {
                long us = v.asLong();
                return ts(Instant.ofEpochSecond(Math.floorDiv(us, 1_000_000L), Math.floorMod(us, 1_000_000L) * 1000L));
            }
            case "io.debezium.time.NanoTimestamp" -> {
                long ns = v.asLong();
                return ts(Instant.ofEpochSecond(Math.floorDiv(ns, 1_000_000_000L), Math.floorMod(ns, 1_000_000_000L)));
            }
            case "io.debezium.time.ZonedTimestamp" -> {
                // ISO 오프셋 문자열(소수 자릿수 가변) → UTC 정규화. Databricks TIMESTAMP는 UTC 기준 타입
                try {
                    return ts(OffsetDateTime.parse(v.asText()).toInstant());
                } catch (DateTimeParseException e) {
                    return v;
                }
            }
            default -> {
                return v; // 일반 타입·binary(base64)는 그대로
            }
        }
    }

    private static JsonNode decimal(String base64, int scale) {
        byte[] raw = Base64.getDecoder().decode(base64);
        return DecimalNode.valueOf(new BigDecimal(new BigInteger(raw), scale));
    }

    private static JsonNode ts(Instant i) {
        return TextNode.valueOf(LocalDateTime.ofInstant(i, ZoneOffset.UTC).format(TS_FORMAT));
    }
}
