package io.deltazium.recovery.envelope;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.List;

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
 * 원본 Debezium 논리 타입명(io.debezium.time.*)까지는 복원하지 않는다 — apply 동등성이 기준.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public final class ConnectJsonAssembler {

    private final ObjectMapper json = new ObjectMapper();

    /** value 전체: {"schema": <envelope struct>, "payload": <행 그대로>} */
    public ObjectNode value(Schema tableSchema, Record row) {
        ObjectNode out = json.createObjectNode();
        Types.StructType struct = tableSchema.asStruct();
        out.set("schema", structSchema(struct, "recovery.Envelope"));
        out.set("payload", structPayload(struct, row));
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
            ObjectNode fs = fieldSchema(f.type(), false);
            fs.put("field", col);
            fields.add(fs);
            payload.set(col, valueNode(f.type(), image.getField(col)));
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

    private ObjectNode structSchema(Types.StructType struct, String name) {
        ObjectNode node = json.createObjectNode();
        node.put("type", "struct");
        if (name != null) {
            node.put("name", name);
        }
        node.put("optional", true);
        ArrayNode fields = node.putArray("fields");
        for (Types.NestedField f : struct.fields()) {
            ObjectNode fs = fieldSchema(f.type(), true);
            fs.put("field", f.name());
            fields.add(fs);
        }
        return node;
    }

    private ObjectNode fieldSchema(Type type, boolean optional) {
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
                ObjectNode struct = structSchema(type.asStructType(), null);
                node.setAll(struct);
            }
            default -> throw new IllegalArgumentException("지원하지 않는 Iceberg 타입: " + type);
        }
        node.put("optional", optional);
        return node;
    }

    private ObjectNode structPayload(Types.StructType struct, Record row) {
        ObjectNode node = json.createObjectNode();
        for (Types.NestedField f : struct.fields()) {
            node.set(f.name(), valueNode(f.type(), row == null ? null : row.getField(f.name())));
        }
        return node;
    }

    private com.fasterxml.jackson.databind.JsonNode valueNode(Type type, Object value) {
        if (value == null) {
            return json.nullNode();
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
            case STRUCT -> structPayload(type.asStructType(), (Record) value);
            default -> throw new IllegalArgumentException("지원하지 않는 Iceberg 타입: " + type);
        };
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
