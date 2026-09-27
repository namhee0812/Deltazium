package io.deltazium.recovery.envelope;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * 파일명 : ConnectJsonAssembler.java
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : Iceberg changelog 한 행(envelope-as-is, 5.1절) → Kafka Connect JSON
 * (schemas.enabled=true 형식: {"schema":..., "payload":...}) 재조립.
 * recovery-sink(JDBC sink)가 live와 동일하게 소비할 수 있어야 하므로 value 스키마를
 * Iceberg 테이블 스키마에서 유도한다. 타입 대응은 JDBC sink apply에 필요한 수준으로:
 * long→int64, decimal→Connect Decimal(logical), timestamp→Connect Timestamp(epoch ms).
 * before/after(source 테이블 컬럼) 필드에 한해 **컬럼별 논리 타입 힌트**(FieldHint, 결함 R1)가
 * 주어지면 원본 Debezium 논리 타입명(io.debezium.time.*)까지 복원한다 — Iceberg 타입만으로는
 * PostgreSQL timestamptz(io.debezium.time.ZonedTimestamp) 같은 컬럼의 apply 동등성이 깨지기
 * 때문이다(docs/experiments/2026-09-27-pg2pg-recovery-rehearsal.md 결함 R1). 힌트가 없는
 * 필드는 기존 Iceberg 타입 기반 동작을 그대로 유지한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 09. 05.       | 최남희  | `_pos`(파이프라인 부여 위치, 5.1절)를 재조립 envelope에서 제외 —
 * |                          | 파이프라인 전용 컬럼이라 원본 Debezium envelope에는 없던 필드다
 * --------------------------------------------------
 * 26. 09. 27.       | 최남희  | 결함 R1 수정(feature/recovery-type-hints): 컬럼별 FieldHint를
 * |                          | 받는 생성자 추가 — before/after struct의 필드만 힌트를 적용한다
 * |                          | (source 등 다른 struct는 대상 밖). 힌트가 있으면 스키마의
 * |                          | type·optional·name·parameters를 힌트대로 내고 값도 힌트 논리
 * |                          | 타입에 맞게 변환한다(대부분 Iceberg 저장값 그대로 통과 — 상세는
 * |                          | docs/internals.md 표). 기존 무인자 생성자는 빈 힌트로 위임해
 * |                          | 힌트 없는 기존 동작·왕복 테스트는 그대로 유지된다.
 * --------------------------------------------------
 */
public final class ConnectJsonAssembler {

    /** 파이프라인이 부여한 위치 컬럼 — 원본 envelope에는 없던 필드라 재조립에서 제외한다 (5.1절). */
    private static final String POSITION_FIELD = "_pos";

    private final ObjectMapper json = new ObjectMapper();
    /** 컬럼명 → 논리 타입 힌트. before/after struct의 직계 필드에만 적용한다(결함 R1). */
    private final Map<String, FieldHint> columnHints;

    public ConnectJsonAssembler() {
        this(Map.of());
    }

    public ConnectJsonAssembler(Map<String, FieldHint> columnHints) {
        this.columnHints = columnHints == null ? Map.of() : columnHints;
    }

    /** value 전체: {"schema": <envelope struct>, "payload": <행 그대로>} */
    public ObjectNode value(Schema tableSchema, Record row) {
        ObjectNode out = json.createObjectNode();
        Types.StructType struct = tableSchema.asStruct();
        out.set("schema", structSchema(struct, "recovery.Envelope", Map.of()));
        out.set("payload", structPayload(struct, row, Map.of()));
        return out;
    }

    /**
     * key: after(또는 delete면 before)에서 PK 컬럼만 뽑아 만든다.
     * @return null이면 키를 만들 수 없는 행 (before/after 모두 없음 — 발행 측이 건너뜀)
     */
    public ObjectNode key(Schema tableSchema, Record row, List<String> keyColumns) {
        Types.StructType rowType = imageType(tableSchema);
        Object after = row.getField("after");
        Record image = after != null ? (Record) after : (Record) row.getField("before");
        if (image == null || rowType == null) {
            return null;
        }
        ObjectNode schema = json.createObjectNode();
        schema.put("type", "struct");
        schema.put("name", "recovery.Key");
        schema.put("optional", false);
        ArrayNode fields = schema.putArray("fields");
        ObjectNode payload = json.createObjectNode();
        for (String col : keyColumns) {
            Types.NestedField f = rowType.field(col);
            if (f == null) {
                throw new IllegalArgumentException("key 컬럼이 changelog 스키마에 없다: " + col);
            }
            FieldHint hint = columnHints.get(col);
            ObjectNode fs = fieldSchema(f.type(), false, hint, Map.of());
            fs.put("field", col);
            fields.add(fs);
            payload.set(col, valueNode(f.type(), image.getField(col), hint, Map.of()));
        }
        ObjectNode out = json.createObjectNode();
        out.set("schema", schema);
        out.set("payload", payload);
        return out;
    }

    /** after(없으면 before) struct 타입 — key 스키마 유도용 */
    private static Types.StructType imageType(Schema tableSchema) {
        Types.NestedField after = tableSchema.findField("after");
        Types.NestedField before = tableSchema.findField("before");
        Types.NestedField image = after != null ? after : before;
        return image == null ? null : image.type().asStructType();
    }

    /** before/after 필드 — 이 이름의 struct 안에서만 columnHints를 적용한다(source 등은 대상 밖). */
    private static boolean isImageField(String fieldName) {
        return "before".equals(fieldName) || "after".equals(fieldName);
    }

    private ObjectNode structSchema(Types.StructType struct, String name, Map<String, FieldHint> hints) {
        ObjectNode node = json.createObjectNode();
        node.put("type", "struct");
        if (name != null) {
            node.put("name", name);
        }
        node.put("optional", true);
        ArrayNode fields = node.putArray("fields");
        for (Types.NestedField f : struct.fields()) {
            if (POSITION_FIELD.equals(f.name())) {
                continue;
            }
            FieldHint hint = hints.get(f.name());
            Map<String, FieldHint> childHints = isImageField(f.name()) ? columnHints : Map.of();
            ObjectNode fs = fieldSchema(f.type(), true, hint, childHints);
            fs.put("field", f.name());
            fields.add(fs);
        }
        return node;
    }

    /**
     * @param hint       이 필드 자체에 적용할 힌트(있으면 type·optional·name·parameters를
     *                   힌트대로 내고 Iceberg 타입은 참고하지 않는다)
     * @param childHints 이 필드가 STRUCT일 때 재귀 호출에 넘길 컬럼 힌트(before/after일 때만
     *                   columnHints, 그 외는 빈 맵)
     */
    private ObjectNode fieldSchema(Type type, boolean optional, FieldHint hint, Map<String, FieldHint> childHints) {
        if (hint != null) {
            return hintedFieldSchema(hint);
        }
        ObjectNode node = json.createObjectNode();
        switch (type.typeId()) {
            case BOOLEAN -> node.put("type", "boolean");
            case INTEGER -> node.put("type", "int32");
            case LONG -> node.put("type", "int64");
            case FLOAT -> node.put("type", "float");
            case DOUBLE -> node.put("type", "double");
            case STRING -> node.put("type", "string");
            case BINARY, FIXED -> node.put("type", "bytes");
            case DECIMAL -> {
                Types.DecimalType d = (Types.DecimalType) type;
                node.put("type", "bytes");
                node.put("name", "org.apache.kafka.connect.data.Decimal");
                node.put("version", 1);
                ObjectNode params = node.putObject("parameters");
                params.put("scale", String.valueOf(d.scale()));
                params.put("connect.decimal.precision", String.valueOf(d.precision()));
            }
            case DATE -> {
                node.put("type", "int32");
                node.put("name", "org.apache.kafka.connect.data.Date");
                node.put("version", 1);
            }
            case TIMESTAMP -> {
                node.put("type", "int64");
                node.put("name", "org.apache.kafka.connect.data.Timestamp");
                node.put("version", 1);
            }
            case STRUCT -> {
                ObjectNode struct = structSchema(type.asStructType(), null, childHints);
                node.setAll(struct);
            }
            default -> throw new IllegalArgumentException("지원하지 않는 Iceberg 타입: " + type);
        }
        node.put("optional", optional);
        return node;
    }

    /** 힌트가 있는 필드의 스키마 노드 — Iceberg 타입은 참고하지 않고 힌트값을 그대로 낸다
     * (결함 R1: 캡처 토픽에서 읽은 원본 Debezium 논리 타입명을 복원하는 목적이므로). */
    private ObjectNode hintedFieldSchema(FieldHint hint) {
        ObjectNode node = json.createObjectNode();
        node.put("type", hint.type());
        if (hint.name() != null && !hint.name().isBlank()) {
            node.put("name", hint.name());
            node.put("version", 1);
        }
        if (!hint.parameters().isEmpty()) {
            ObjectNode params = node.putObject("parameters");
            hint.parameters().forEach(params::put);
        }
        node.put("optional", hint.optional());
        return node;
    }

    private ObjectNode structPayload(Types.StructType struct, Record row, Map<String, FieldHint> hints) {
        ObjectNode node = json.createObjectNode();
        for (Types.NestedField f : struct.fields()) {
            if (POSITION_FIELD.equals(f.name())) {
                continue;
            }
            FieldHint hint = hints.get(f.name());
            Map<String, FieldHint> childHints = isImageField(f.name()) ? columnHints : Map.of();
            Object value = row == null ? null : row.getField(f.name());
            node.set(f.name(), valueNode(f.type(), value, hint, childHints));
        }
        return node;
    }

    private JsonNode valueNode(Type type, Object value, FieldHint hint, Map<String, FieldHint> childHints) {
        if (value == null) {
            return json.nullNode();
        }
        if (hint != null) {
            return hintedValueNode(hint, value);
        }
        return switch (type.typeId()) {
            case BOOLEAN -> json.getNodeFactory().booleanNode((Boolean) value);
            case INTEGER -> json.getNodeFactory().numberNode((Integer) value);
            case LONG -> json.getNodeFactory().numberNode((Long) value);
            case FLOAT -> json.getNodeFactory().numberNode((Float) value);
            case DOUBLE -> json.getNodeFactory().numberNode((Double) value);
            case STRING -> json.getNodeFactory().textNode(value.toString());
            case BINARY -> json.getNodeFactory().textNode(
                    Base64.getEncoder().encodeToString(((ByteBuffer) value).array()));
            case FIXED -> json.getNodeFactory().textNode(
                    Base64.getEncoder().encodeToString((byte[]) value));
            case DECIMAL -> json.getNodeFactory().textNode(Base64.getEncoder()
                    .encodeToString(((BigDecimal) value).unscaledValue().toByteArray()));
            case DATE -> json.getNodeFactory().numberNode(
                    (int) ((java.time.LocalDate) value).toEpochDay());
            case TIMESTAMP -> json.getNodeFactory().numberNode(toEpochMillis(value));
            case STRUCT -> structPayload(type.asStructType(), (Record) value, childHints);
            default -> throw new IllegalArgumentException("지원하지 않는 Iceberg 타입: " + type);
        };
    }

    /**
     * 힌트가 있는 필드의 값 변환 — Iceberg 저장 표현 → Debezium 와이어 표현(docs/internals.md
     * "논리 타입 힌트 값 변환" 표). 실제로는 대부분 Iceberg에 이미 원시값 그대로 저장돼 있어
     * (JsonConverter가 io.debezium.time.* 논리명을 모르는 채로 원시 타입만 보고 파싱하므로
     * Iceberg-sink도 원시 타입으로 저장한다) 스케일 변환 없이 그대로 통과한다 — MicroTimestamp도
     * int64 값을 그대로 옮길 뿐 밀리초로 환산하지 않는다(환산하면 오히려 값이 바뀐다).
     */
    private JsonNode hintedValueNode(FieldHint hint, Object value) {
        return switch (hint.type()) {
            case "string" -> json.getNodeFactory().textNode(value.toString());
            case "int8", "int16", "int32" -> json.getNodeFactory().numberNode(asInt(value));
            case "int64" -> json.getNodeFactory().numberNode(asLong(value));
            case "boolean" -> json.getNodeFactory().booleanNode((Boolean) value);
            case "float32" -> json.getNodeFactory().numberNode(((Number) value).floatValue());
            case "float64" -> json.getNodeFactory().numberNode(((Number) value).doubleValue());
            case "bytes" -> hintedBytesValue(hint, value);
            default -> throw new IllegalArgumentException("힌트 타입 값 변환 미지원: " + hint.type());
        };
    }

    private JsonNode hintedBytesValue(FieldHint hint, Object value) {
        if ("org.apache.kafka.connect.data.Decimal".equals(hint.name()) && value instanceof BigDecimal bd) {
            return json.getNodeFactory().textNode(
                    Base64.getEncoder().encodeToString(bd.unscaledValue().toByteArray()));
        }
        if (value instanceof ByteBuffer bb) {
            return json.getNodeFactory().textNode(Base64.getEncoder().encodeToString(bb.array()));
        }
        if (value instanceof byte[] arr) {
            return json.getNodeFactory().textNode(Base64.getEncoder().encodeToString(arr));
        }
        throw new IllegalArgumentException("bytes 힌트와 저장값 타입이 맞지 않는다: " + value.getClass());
    }

    /** int64 힌트의 저장값 변환 — Iceberg LONG(원시 통과)이 검증된 경로. OffsetDateTime 등
     * TIMESTAMP 계열은 표준 Kafka Connect 논리 타입(org.apache.kafka.connect.data.Timestamp)이
     * Iceberg TIMESTAMP로 저장된 경우를 대비한 것으로, 현재 아키텍처에서 관측되지 않아
     * 미검증이다(docs/internals.md). */
    private static long asLong(Object value) {
        if (value instanceof Long l) {
            return l;
        }
        if (value instanceof Integer i) {
            return i;
        }
        if (value instanceof OffsetDateTime || value instanceof LocalDateTime || value instanceof Instant) {
            return toEpochMillis(value); // 미검증 경로 — 위 클래스 설명 참고
        }
        throw new IllegalArgumentException("int64 힌트와 저장값 타입이 맞지 않는다: " + value.getClass());
    }

    /** int32 힌트의 저장값 변환 — Iceberg INTEGER(원시 통과) 또는 DATE(LocalDate→epoch day)가
     * 검증된 경로. */
    private static int asInt(Object value) {
        if (value instanceof Integer i) {
            return i;
        }
        if (value instanceof Long l) {
            return l.intValue();
        }
        if (value instanceof java.time.LocalDate d) {
            return (int) d.toEpochDay();
        }
        throw new IllegalArgumentException("int32 힌트와 저장값 타입이 맞지 않는다: " + value.getClass());
    }

    private static long toEpochMillis(Object ts) {
        if (ts instanceof OffsetDateTime odt) {
            return odt.toInstant().toEpochMilli();
        }
        if (ts instanceof LocalDateTime ldt) {
            return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
        }
        if (ts instanceof Instant i) {
            return i.toEpochMilli();
        }
        throw new IllegalArgumentException("timestamp 값 타입 미지원: " + ts.getClass());
    }
}
