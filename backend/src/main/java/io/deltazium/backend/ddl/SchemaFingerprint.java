package io.deltazium.backend.ddl;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * 파일명 : SchemaFingerprint.java
 * 작성일자 : 26. 09. 07.
 * 작성자 : 최남희
 * 설명 : schema change topic이 없는 소스(PostgreSQL 등)의 DDL 감지 — 순수 로직만 분리
 * (architecture.md 7절 개정). Kafka 폴링·상태 저장은 SchemaFingerprintPoller가 맡고,
 * 여기는 (1) Debezium envelope JSON에서 after struct 필드 지문 뽑기 (2) 지문 비교 diff
 * (3) ADD/DROP COLUMN 초안 DDL 생성만 다룬다 — 단위 테스트가 이 클래스만으로 가능하도록.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 최초 생성 — 다중 소스·다중 타깃 ② 스키마 지문 감지
 * --------------------------------------------------
 */
public final class SchemaFingerprint {

    private static final ObjectMapper JSON = new ObjectMapper();

    private SchemaFingerprint() {
    }

    /**
     * 필드 목록을 JSON으로 직렬화 — registered_tables.schema_fields_json에 저장해 다음 감시
     * 주기의 diff 기준(직전 source 스키마 스냅샷)으로 쓴다. 지문(해시)만으로는 무엇이 바뀌었는지
     * 복원할 수 없어 별도 보관이 필요하다(2026-09-07 구현 판단, docs/internals.md).
     */
    public static String toJson(List<FieldDesc> fields) {
        ArrayNode arr = JSON.createArrayNode();
        for (FieldDesc f : fields) {
            ObjectNode n = JSON.createObjectNode();
            n.put("field", f.name());
            n.put("type", f.type());
            n.put("optional", f.optional());
            ObjectNode params = n.putObject("parameters");
            f.parameters().forEach(params::put);
            arr.add(n);
        }
        return arr.toString();
    }

    /** toJson의 역변환. 파싱 불가·null·빈 문자열이면 빈 목록(첫 감시 취급). */
    public static List<FieldDesc> fromJson(String json) {
        if (json == null || json.isBlank()) {
            return List.of();
        }
        try {
            JsonNode arr = JSON.readTree(json);
            List<FieldDesc> result = new ArrayList<>();
            for (JsonNode n : arr) {
                Map<String, String> params = new TreeMap<>();
                n.path("parameters").properties().forEach(e -> params.put(e.getKey(), e.getValue().asText()));
                result.add(new FieldDesc(n.path("field").asText(), n.path("type").asText(),
                        n.path("optional").asBoolean(true), params));
            }
            return result;
        } catch (Exception e) {
            return List.of();
        }
    }

    /** after struct 컬럼 하나의 지문 대상 속성. parameters는 정렬해 비교·해시한다. */
    public record FieldDesc(String name, String type, boolean optional, Map<String, String> parameters) {

        String canonical() {
            String params = parameters.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .reduce((a, b) -> a + "," + b).orElse("");
            return name + "|" + type + "|" + optional + "|" + params;
        }
    }

    public enum ChangeKind { ADDED, REMOVED, TYPE_CHANGED }

    public record FieldChange(ChangeKind kind, FieldDesc before, FieldDesc after) {
    }

    /**
     * Debezium JSON converter(schemas.enabled=true) 레코드 value에서 after struct의
     * 필드 목록을 뽑는다. value는 {"schema":{...},"payload":{...}} 형태여야 한다.
     * @return after struct의 필드 스키마 목록(이름순 정렬). schema.fields에 field=="after"인
     *         항목이 없으면(형식 밖) 빈 리스트.
     */
    public static List<FieldDesc> afterFields(JsonNode envelopeValue) {
        JsonNode schema = envelopeValue.path("schema");
        JsonNode afterField = null;
        for (JsonNode f : schema.path("fields")) {
            if ("after".equals(f.path("field").asText())) {
                afterField = f;
                break;
            }
        }
        if (afterField == null) {
            return List.of();
        }
        List<FieldDesc> result = new ArrayList<>();
        for (JsonNode f : afterField.path("fields")) {
            Map<String, String> params = new TreeMap<>();
            JsonNode p = f.path("parameters");
            if (p.isObject()) {
                p.properties().forEach(e -> params.put(e.getKey(), e.getValue().asText()));
            }
            // 의미 타입(name, 예: org.apache.kafka.connect.data.Decimal)도 타입 판정에 포함 —
            // 같은 raw type(bytes 등)이라도 의미가 다르면 다른 컬럼 타입이다.
            String type = f.path("type").asText("");
            if (f.hasNonNull("name")) {
                type = type + ":" + f.get("name").asText();
            }
            result.add(new FieldDesc(f.path("field").asText(), type, f.path("optional").asBoolean(true),
                    params));
        }
        result.sort(Comparator.comparing(FieldDesc::name));
        return result;
    }

    /** SHA-256(hex) — 필드 지문을 이름순 정렬 후 이어붙인 문자열의 해시. */
    public static String hash(List<FieldDesc> fields) {
        String canonical = fields.stream().map(FieldDesc::canonical).reduce((a, b) -> a + ";" + b).orElse("");
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            byte[] digest = sha256.digest(canonical.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 미지원 JVM", e);
        }
    }

    /** 이전·이후 필드 목록 비교 — 추가/삭제/타입변경. 이름 기준 매칭. */
    public static List<FieldChange> diff(List<FieldDesc> before, List<FieldDesc> after) {
        Map<String, FieldDesc> beforeByName = new LinkedHashMap<>();
        before.forEach(f -> beforeByName.put(f.name(), f));
        Map<String, FieldDesc> afterByName = new LinkedHashMap<>();
        after.forEach(f -> afterByName.put(f.name(), f));

        List<FieldChange> changes = new ArrayList<>();
        for (FieldDesc a : after) {
            FieldDesc b = beforeByName.get(a.name());
            if (b == null) {
                changes.add(new FieldChange(ChangeKind.ADDED, null, a));
            } else if (!b.canonical().equals(a.canonical())) {
                changes.add(new FieldChange(ChangeKind.TYPE_CHANGED, b, a));
            }
        }
        for (FieldDesc b : before) {
            if (!afterByName.containsKey(b.name())) {
                changes.add(new FieldChange(ChangeKind.REMOVED, b, null));
            }
        }
        return changes;
    }

    /** Debezium 스키마 타입 → 타깃 DDL 타입 소형 매핑 (2026-09-07, ADD COLUMN 초안 전용).
     * 매핑에 없는 타입(struct/array, 미분류 의미 타입 등)은 초안을 만들지 않는다 — 확인 후
     * 수동 DDL이 필요하다는 뜻으로, TODO의 "타입 변경은 초안 없음"과 같은 안전 원칙이다. */
    public static String draftDdl(String targetDbType, String targetSchema, String targetTable,
                                  List<FieldChange> changes) {
        boolean oracle = "ORACLE".equalsIgnoreCase(targetDbType);
        List<String> addClauses = new ArrayList<>();
        List<String> dropClauses = new ArrayList<>();
        for (FieldChange c : changes) {
            if (c.kind() == ChangeKind.ADDED) {
                String mapped = mapType(targetDbType, c.after().type());
                if (mapped == null) {
                    continue; // 매핑 없음 — 초안 생략
                }
                String col = oracle ? c.after().name().toUpperCase(java.util.Locale.ROOT) : c.after().name();
                addClauses.add(oracle ? "\"%s\" %s".formatted(col, mapped)
                        : "ADD COLUMN \"%s\" %s".formatted(col, mapped));
            } else if (c.kind() == ChangeKind.REMOVED) {
                String col = oracle ? c.before().name().toUpperCase(java.util.Locale.ROOT) : c.before().name();
                dropClauses.add(oracle ? "\"%s\"".formatted(col) : "DROP COLUMN \"%s\"".formatted(col));
            }
            // TYPE_CHANGED는 초안을 만들지 않는다 (TODO ②: "타입 변경은 초안 없이 확인만")
        }
        if (addClauses.isEmpty() && dropClauses.isEmpty()) {
            return null;
        }
        String qualified = "\"%s\".\"%s\"".formatted(targetSchema, targetTable);
        StringBuilder sb = new StringBuilder();
        if (!addClauses.isEmpty()) {
            sb.append(oracle
                    ? "ALTER TABLE %s ADD (%s);".formatted(qualified, String.join(", ", addClauses))
                    : "ALTER TABLE %s %s;".formatted(qualified, String.join(", ", addClauses)));
        }
        for (String drop : dropClauses) {
            if (sb.length() > 0) {
                sb.append(' ');
            }
            sb.append(oracle
                    ? "ALTER TABLE %s DROP COLUMN %s;".formatted(qualified, drop)
                    : "ALTER TABLE %s %s;".formatted(qualified, drop));
        }
        return sb.toString();
    }

    /** 사람이 읽는 diff 요약 한 줄 (ddl_events.ddl_text 앞부분에 붙인다). */
    public static String summarize(List<FieldChange> changes) {
        List<String> parts = new ArrayList<>();
        for (FieldChange c : changes) {
            switch (c.kind()) {
                case ADDED -> parts.add("추가: " + c.after().name() + "(" + c.after().type() + ")");
                case REMOVED -> parts.add("삭제: " + c.before().name());
                case TYPE_CHANGED -> parts.add("타입변경: " + c.before().name() + " "
                        + c.before().type() + "→" + c.after().type());
            }
        }
        return String.join(", ", parts);
    }

    /** Debezium 스키마 타입(+ 의미 타입) → 타깃 DbType별 소형 DDL 타입 매핑. 미지원이면 null. */
    private static String mapType(String targetDbType, String debeziumType) {
        String base = debeziumType.contains(":") ? debeziumType.substring(debeziumType.indexOf(':') + 1)
                : debeziumType;
        boolean oracle = "ORACLE".equalsIgnoreCase(targetDbType);
        return switch (base) {
            case "int8", "int16", "int32" -> oracle ? "NUMBER(10)" : "INTEGER";
            case "int64" -> oracle ? "NUMBER(19)" : "BIGINT";
            case "float32", "float64" -> oracle ? "BINARY_DOUBLE" : "DOUBLE PRECISION";
            case "boolean" -> oracle ? "NUMBER(1)" : "BOOLEAN";
            case "string" -> oracle ? "VARCHAR2(4000)" : "TEXT";
            case "bytes" -> oracle ? "BLOB" : "BYTEA";
            case "io.debezium.time.Date" -> "DATE";
            case "io.debezium.time.Timestamp", "io.debezium.time.MicroTimestamp",
                 "io.debezium.time.ZonedTimestamp" -> "TIMESTAMP";
            default -> null; // struct/array, org.apache.kafka.connect.data.Decimal(정밀도 필요) 등 — 확인 필요
        };
    }
}
