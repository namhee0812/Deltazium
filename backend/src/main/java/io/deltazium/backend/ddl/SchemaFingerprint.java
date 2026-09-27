package io.deltazium.backend.ddl;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.deltazium.backend.dictionary.TableColumn;
import io.deltazium.backend.registry.DbType;

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
 * 26. 09. 27.       | 최남희  | mapNativeType·draftCreateTable 추가 — 등록 시 "소스 스키마로
 * |                          | 새로 생성" 옵션(architecture.md 8절) 전용 CREATE TABLE 초안.
 * |                          | 위 mapType()과 입력 도메인이 다르지만(Debezium 논리 타입 vs 소스
 * |                          | 딕셔너리 원문 타입) "타입 매핑은 한 곳에서" 원칙에 따라 같은
 * |                          | 클래스에 둔다(RegistrationService.previewTargetTableDdl 전용)
 * --------------------------------------------------
 * 26. 09. 28.       | 최남희  | SingleStore 타깃 지원. (1) draftDdl·draftCreateTable의 식별자
 * |                          | 인용을 하드코딩된 큰따옴표에서 DbType.quoteIdentifier로 전환
 * |                          | (SingleStore는 백틱이어야 한다 — 인용 부호 점검, architecture.md
 * |                          | 8절). (2) mapType(ADD/DROP COLUMN 초안 타입)에 SINGLESTORE 분기
 * |                          | 추가 — 종전 오라클/비오라클 이분법으로는 SingleStore 타깃에
 * |                          | PostgreSQL 타입 키워드(BYTEA 등)가 나가 무효 DDL이 될 뻔했다.
 * |                          | (3) mapNativeType에 Oracle→SingleStore·PostgreSQL→SingleStore
 * |                          | 매핑 추가(문자 TEXT·정수 INT/BIGINT·소수 DECIMAL(65,30)·boolean
 * |                          | BOOL·날짜 DATE/DATETIME(6)·바이너리 BLOB) — 길이·정밀도 정보가
 * |                          | 없어(기존 2026-09-27 결정과 동일 제약) DECIMAL은 폭 없이 쓰면
 * |                          | MySQL 계열은 DECIMAL(10,0)으로 묵시 절삭되므로 안전한 최대치
 * |                          | DECIMAL(65,30)(MySQL/SingleStore 정밀도 상한)을 명시했다. 근거:
 * |                          | docs/internals.md "타깃 테이블 생성 옵션 — 타입 매핑" 절.
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

    /**
     * Debezium 스키마 타입 → 타깃 DDL 타입 소형 매핑 (2026-09-07, ADD/DROP COLUMN 초안 전용).
     * 매핑에 없는 타입(struct/array, 미분류 의미 타입 등)은 초안을 만들지 않는다 — 확인 후
     * 수동 DDL이 필요하다는 뜻으로, TODO의 "타입 변경은 초안 없음"과 같은 안전 원칙이다.
     *
     * <p>반환값은 **세미콜론 없는 단일 실행문**이다 — 승인 시 이 문자열을 그대로
     * {@code Statement.execute()}에 넘기므로(DdlEventService.approve, FINGERPRINT origin은
     * 이름 치환도 하지 않는다) 여러 문장을 세미콜론으로 이어붙이면 JDBC 드라이버가
     * 거부한다(실측: ORA-00900, ddl_events id=39). ADD·DROP이 함께 있으면 Oracle은 한
     * ALTER TABLE에 ADD(...)와 DROP(...) 절을 나란히(공백 구분) 쓸 수 있고, PostgreSQL은
     * ADD COLUMN·DROP COLUMN 액션을 쉼표로 나열할 수 있어 각각 단일 문장으로 합친다.
     */
    public static String draftDdl(String targetDbType, String targetSchema, String targetTable,
                                  List<FieldChange> changes) {
        DbType type = DbType.find(targetDbType).orElse(DbType.ORACLE);
        boolean oracle = type == DbType.ORACLE;
        List<String> addCols = new ArrayList<>();   // Oracle: "COL" TYPE / 그 외: 인용부호 COL TYPE
        List<String> dropCols = new ArrayList<>();  // 컬럼명(인용부호 포함)만 — 절 조립은 아래서
        for (FieldChange c : changes) {
            if (c.kind() == ChangeKind.ADDED) {
                String mapped = mapType(targetDbType, c.after().type());
                if (mapped == null) {
                    continue; // 매핑 없음 — 초안 생략
                }
                String col = oracle ? c.after().name().toUpperCase(java.util.Locale.ROOT) : c.after().name();
                addCols.add("%s %s".formatted(type.quoteIdentifier(col), mapped));
            } else if (c.kind() == ChangeKind.REMOVED) {
                String col = oracle ? c.before().name().toUpperCase(java.util.Locale.ROOT) : c.before().name();
                dropCols.add(type.quoteIdentifier(col));
            }
            // TYPE_CHANGED는 초안을 만들지 않는다 (TODO ②: "타입 변경은 초안 없이 확인만")
        }
        if (addCols.isEmpty() && dropCols.isEmpty()) {
            return null;
        }
        String qualified = type.quoteIdentifier(targetSchema) + "." + type.quoteIdentifier(targetTable);
        if (oracle) {
            List<String> clauses = new ArrayList<>();
            if (!addCols.isEmpty()) {
                clauses.add("ADD (%s)".formatted(String.join(", ", addCols)));
            }
            if (!dropCols.isEmpty()) {
                clauses.add("DROP (%s)".formatted(String.join(", ", dropCols)));
            }
            return "ALTER TABLE %s %s".formatted(qualified, String.join(" ", clauses));
        }
        // PostgreSQL·SingleStore: 액션을 쉼표로 나열 — ADD COLUMN 각각 + DROP COLUMN 각각
        List<String> actions = new ArrayList<>();
        addCols.forEach(c -> actions.add("ADD COLUMN " + c));
        dropCols.forEach(c -> actions.add("DROP COLUMN " + c));
        return "ALTER TABLE %s %s".formatted(qualified, String.join(", ", actions));
    }

    /** 사람이 읽는 diff 요약 한 줄 — ddl_events.note에 넣는다(ddl_text는 실행 문장 전용,
     * 2026-09-07 수정: 요약·초안을 한 컬럼에 섞어 넣던 것이 승인 시 그대로 실행돼 ORA-00900을 냈다). */
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

    /**
     * Debezium 스키마 타입(+ 의미 타입) → 타깃 DbType별 소형 DDL 타입 매핑. 미지원이면 null.
     * DbType 3종(Oracle·PostgreSQL·SingleStore)별로 완전히 분리한다 — 종전엔 "oracle이냐
     * 아니냐" 이분법이라 PostgreSQL 분기가 SingleStore에도 그대로 쓰였는데, PostgreSQL 전용
     * 키워드(BYTEA·BOOLEAN·DOUBLE PRECISION)는 SingleStore(MySQL 계열)에서 무효 타입이라
     * 2026-09-28 SingleStore 타깃 지원과 함께 3분기로 바꿨다.
     */
    private static String mapType(String targetDbType, String debeziumType) {
        String base = debeziumType.contains(":") ? debeziumType.substring(debeziumType.indexOf(':') + 1)
                : debeziumType;
        DbType type = DbType.find(targetDbType).orElse(DbType.ORACLE);
        return switch (type) {
            case ORACLE -> switch (base) {
                case "int8", "int16", "int32" -> "NUMBER(10)";
                case "int64" -> "NUMBER(19)";
                case "float32", "float64" -> "BINARY_DOUBLE";
                case "boolean" -> "NUMBER(1)";
                case "string" -> "VARCHAR2(4000)";
                case "bytes" -> "BLOB";
                case "io.debezium.time.Date" -> "DATE";
                case "io.debezium.time.Timestamp", "io.debezium.time.MicroTimestamp",
                     "io.debezium.time.ZonedTimestamp" -> "TIMESTAMP";
                default -> null; // struct/array, Decimal(정밀도 필요) 등 — 확인 필요
            };
            case SINGLESTORE -> switch (base) {
                case "int8", "int16", "int32" -> "INT";
                case "int64" -> "BIGINT";
                case "float32", "float64" -> "DOUBLE";
                case "boolean" -> "BOOL";
                case "string" -> "TEXT";
                case "bytes" -> "BLOB";
                case "io.debezium.time.Date" -> "DATE";
                case "io.debezium.time.Timestamp", "io.debezium.time.MicroTimestamp",
                     "io.debezium.time.ZonedTimestamp" -> "DATETIME(6)";
                default -> null;
            };
            default -> switch (base) { // PostgreSQL(그 외 비Oracle) — 기존 동작 그대로
                case "int8", "int16", "int32" -> "INTEGER";
                case "int64" -> "BIGINT";
                case "float32", "float64" -> "DOUBLE PRECISION";
                case "boolean" -> "BOOLEAN";
                case "string" -> "TEXT";
                case "bytes" -> "BYTEA";
                case "io.debezium.time.Date" -> "DATE";
                case "io.debezium.time.Timestamp", "io.debezium.time.MicroTimestamp",
                     "io.debezium.time.ZonedTimestamp" -> "TIMESTAMP";
                default -> null;
            };
        };
    }

    /**
     * 소스 네이티브 컬럼 타입(TableColumn.dataType, 딕셔너리 원문) → 타깃 DbType 컬럼 타입
     * 매핑 — 등록 시 "소스 스키마로 새로 생성" 옵션 전용(architecture.md 8절). Oracle↔PostgreSQL
     * 양방향 + Oracle·PostgreSQL→SingleStore(2026-09-28 추가, SingleStore는 타깃 전용이라
     * 반대 방향은 없음) 최소 지원(문자·정수·소수·날짜/시각·boolean·bytea/RAW). 길이·정밀도는
     * 다루지 않는다(TableColumn이 원문 타입 이름만 가지고 있어 자리수 정보가 없음 —
     * 2026-09-27 최소 구현, 필요해지면 TableColumn에 length/precision을 추가하는 확장으로
     * 다룬다). 매핑 불가·미지원 DB 조합은 null — 호출측이 초안 생성을 거부한다.
     */
    public static String mapNativeType(DbType sourceType, DbType targetType, String sourceColumnType) {
        if (sourceType == targetType) {
            return sourceColumnType; // 동종 DB — 원문 타입 그대로 유효
        }
        String base = sourceColumnType == null ? "" : sourceColumnType.trim().toUpperCase(Locale.ROOT);
        int paren = base.indexOf('(');
        if (paren >= 0) {
            base = base.substring(0, paren).trim();
        }
        if (sourceType == DbType.ORACLE && targetType == DbType.POSTGRESQL) {
            return switch (base) {
                case "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB" -> "TEXT";
                case "NUMBER", "FLOAT" -> "NUMERIC";
                case "BINARY_FLOAT", "BINARY_DOUBLE" -> "DOUBLE PRECISION";
                case "DATE", "TIMESTAMP" -> "TIMESTAMP";
                case "RAW", "LONG RAW", "BLOB" -> "BYTEA";
                default -> null;
            };
        }
        if (sourceType == DbType.POSTGRESQL && targetType == DbType.ORACLE) {
            return switch (base) {
                case "CHARACTER VARYING", "VARCHAR", "CHARACTER", "CHAR", "TEXT" -> "VARCHAR2(4000)";
                case "NUMERIC", "DECIMAL" -> "NUMBER";
                case "SMALLINT" -> "NUMBER(5)";
                case "INTEGER" -> "NUMBER(10)";
                case "BIGINT" -> "NUMBER(19)";
                case "REAL", "DOUBLE PRECISION" -> "BINARY_DOUBLE";
                case "BOOLEAN" -> "NUMBER(1)";
                case "DATE" -> "DATE";
                case "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP WITH TIME ZONE" -> "TIMESTAMP";
                case "BYTEA" -> "RAW(2000)";
                default -> null;
            };
        }
        // SingleStore는 타깃 전용(sourceCapable=false)이라 소스가 될 수 없다 — 아래 두 분기만 있으면 된다.
        if (sourceType == DbType.ORACLE && targetType == DbType.SINGLESTORE) {
            return switch (base) {
                case "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB" -> "TEXT";
                // Oracle NUMBER는 정밀도 정보가 없어 boolean/정수/소수를 구분할 수 없다(기존
                // Oracle→PostgreSQL 판단과 동일 근거, docs/internals.md). MySQL 계열 DECIMAL은
                // 길이를 안 주면 DECIMAL(10,0)으로 묵시 절삭되므로 안전한 최대치를 명시한다.
                case "NUMBER", "FLOAT" -> "DECIMAL(65,30)";
                case "BINARY_FLOAT", "BINARY_DOUBLE" -> "DOUBLE";
                // Oracle DATE는 시각 성분을 포함한다(ANSI DATE와 다름) — TIMESTAMP와 동일하게
                // DATETIME으로 매핑(Oracle→PostgreSQL의 DATE→TIMESTAMP 판단과 동일 근거).
                case "DATE", "TIMESTAMP" -> "DATETIME(6)";
                case "RAW", "LONG RAW", "BLOB" -> "BLOB";
                default -> null;
            };
        }
        if (sourceType == DbType.POSTGRESQL && targetType == DbType.SINGLESTORE) {
            return switch (base) {
                case "CHARACTER VARYING", "VARCHAR", "CHARACTER", "CHAR", "TEXT" -> "TEXT";
                case "NUMERIC", "DECIMAL" -> "DECIMAL(65,30)";
                case "SMALLINT" -> "SMALLINT";
                case "INTEGER" -> "INT";
                case "BIGINT" -> "BIGINT";
                case "REAL", "DOUBLE PRECISION" -> "DOUBLE";
                case "BOOLEAN" -> "BOOL";
                case "DATE" -> "DATE"; // PostgreSQL DATE는 시각 성분이 없다 — Oracle 방향과 다름
                case "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP WITH TIME ZONE" -> "DATETIME(6)";
                case "BYTEA" -> "BLOB";
                default -> null;
            };
        }
        return null; // Oracle·PostgreSQL·SingleStore 외 조합은 미지원(8절 지원 DB 범위)
    }

    /**
     * 등록 시 "소스 스키마로 새로 생성" 초안 — 소스 컬럼·PK를 타깃 DDL 타입으로 매핑해
     * CREATE TABLE 문 하나를 만든다(architecture.md 8절). 식별자 폴딩·인용 규칙은 draftDdl과
     * 동일(호출측이 이미 DbType.foldIdentifier로 접은 스키마·테이블명을 넘긴다는 전제 — 타깃이
     * Oracle이면 컬럼명도 대문자, 그 외는 원문; 인용 부호는 DbType.quoteIdentifier — Oracle·
     * PostgreSQL은 큰따옴표, SingleStore는 백틱). 매핑 불가 컬럼이 하나라도 있으면 부분 생성
     * 대신 초안 자체를 거부한다(8절: "매핑 불가 타입은 초안 생성 거부 + 사유") — 나중에 컬럼을
     * 못 맞추는 반쪽 테이블을 남기지 않기 위함. PK가 없으면 등록 규칙(공통 PK 필수)과 모순이라
     * 마찬가지로 거부한다.
     */
    public static String draftCreateTable(DbType sourceType, DbType targetType, String targetSchema,
                                          String targetTable, List<TableColumn> sourceColumns) {
        boolean oracle = targetType == DbType.ORACLE;
        List<String> colDefs = new ArrayList<>();
        List<String> pkCols = new ArrayList<>();
        List<String> unsupported = new ArrayList<>();
        for (TableColumn c : sourceColumns) {
            String mapped = mapNativeType(sourceType, targetType, c.dataType());
            if (mapped == null) {
                unsupported.add(c.name() + "(" + c.dataType() + ")");
                continue;
            }
            String col = oracle ? c.name().toUpperCase(Locale.ROOT) : c.name();
            colDefs.add("%s %s".formatted(targetType.quoteIdentifier(col), mapped));
            if (c.pk()) {
                pkCols.add(targetType.quoteIdentifier(col));
            }
        }
        if (!unsupported.isEmpty()) {
            throw new IllegalArgumentException(
                    "타깃 타입 매핑이 없는 컬럼이 있어 생성 초안을 만들 수 없다: " + String.join(", ", unsupported));
        }
        if (pkCols.isEmpty()) {
            throw new IllegalArgumentException("PK 컬럼이 없어 생성 초안을 만들 수 없다 (등록 규칙상 PK 필수)");
        }
        String qualified = targetType.quoteIdentifier(targetSchema) + "." + targetType.quoteIdentifier(targetTable);
        return "CREATE TABLE %s (%s, PRIMARY KEY (%s))"
                .formatted(qualified, String.join(", ", colDefs), String.join(", ", pkCols));
    }
}
