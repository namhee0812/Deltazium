package io.deltazium.backend.ddl;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.deltazium.backend.dictionary.TableColumn;
import io.deltazium.backend.registry.DbType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 파일명 : SchemaFingerprintTest.java
 * 작성일자 : 26. 09. 07.
 * 작성자 : 최남희
 * 설명 : 스키마 지문 순수 로직(after struct 추출·해시·diff·초안 DDL) 단위 테스트
 * (architecture.md 7절 개정 — PostgreSQL 등 schema change topic 없는 소스의 DDL 감지).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 최초 생성 — 다중 소스·다중 타깃 ②
 * --------------------------------------------------
 * 26. 09. 27.       | 최남희  | mapNativeType·draftCreateTable 테스트 추가 — 등록 시 "소스
 * |                          | 스키마로 새로 생성" 옵션(architecture.md 8절) 전용 CREATE TABLE 초안
 * --------------------------------------------------
 * 26. 09. 28.       | 최남희  | SingleStore 타깃 케이스 추가 — draftDdl 백틱 인용, mapNativeType
 * |                          | Oracle/PostgreSQL→SingleStore, draftCreateTable SingleStore 타깃
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | Databricks 타깃 케이스 — mapNativeType(Oracle/PG→DATABRICKS 골든, 정밀도 있을 때/
 * |                          | 없을 때), draftCreateTable(3-part 백틱·PK NOT NULL·CONSTRAINT), draftDdl(ADD COLUMNS,
 * |                          | DROP 섞이면 초안 없음)
 * --------------------------------------------------
 */
class SchemaFingerprintTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** Debezium JSON converter(schemas.enabled=true) envelope 형식 — after struct에 컬럼 2개. */
    private static final String ENVELOPE_V1 = """
            {
              "schema": {
                "type": "struct",
                "fields": [
                  {"type":"struct","optional":true,"name":"cdc_src.orders.Value","field":"before",
                   "fields":[
                     {"type":"int32","optional":false,"field":"id"},
                     {"type":"string","optional":true,"field":"status"}
                   ]},
                  {"type":"struct","optional":true,"name":"cdc_src.orders.Value","field":"after",
                   "fields":[
                     {"type":"int32","optional":false,"field":"id"},
                     {"type":"string","optional":true,"field":"status"}
                   ]},
                  {"type":"string","optional":true,"field":"op"}
                ]
              },
              "payload": {"before": null, "after": {"id": 1, "status": "OPEN"}, "op": "c"}
            }""";

    /** V1에 AMOUNT(decimal) 컬럼이 추가된 형태. */
    private static final String ENVELOPE_V2_ADD_COLUMN = """
            {
              "schema": {
                "type": "struct",
                "fields": [
                  {"type":"struct","optional":true,"name":"cdc_src.orders.Value","field":"after",
                   "fields":[
                     {"type":"int32","optional":false,"field":"id"},
                     {"type":"string","optional":true,"field":"status"},
                     {"type":"bytes","optional":true,"name":"org.apache.kafka.connect.data.Decimal",
                      "parameters":{"scale":"2"},"field":"amount"}
                   ]}
                ]
              },
              "payload": {"after": {"id": 1, "status": "OPEN", "amount": "AQ=="}, "op": "u"}
            }""";

    private static JsonNode parse(String s) throws Exception {
        return JSON.readTree(s);
    }

    @Test
    void after_struct_필드를_이름순으로_뽑는다() throws Exception {
        List<SchemaFingerprint.FieldDesc> fields = SchemaFingerprint.afterFields(parse(ENVELOPE_V1));
        assertThat(fields).extracting(SchemaFingerprint.FieldDesc::name).containsExactly("id", "status");
        assertThat(fields.get(0).type()).isEqualTo("int32");
        assertThat(fields.get(0).optional()).isFalse();
    }

    @Test
    void after_필드가_없으면_빈_목록() throws Exception {
        assertThat(SchemaFingerprint.afterFields(parse("{\"schema\":{\"fields\":[]},\"payload\":{}}")))
                .isEmpty();
    }

    @Test
    void 같은_필드목록의_지문은_순서와_무관하게_같다() {
        var a = List.of(
                new SchemaFingerprint.FieldDesc("id", "int32", false, Map.of()),
                new SchemaFingerprint.FieldDesc("status", "string", true, Map.of()));
        var b = List.of(
                new SchemaFingerprint.FieldDesc("status", "string", true, Map.of()),
                new SchemaFingerprint.FieldDesc("id", "int32", false, Map.of()));
        // afterFields가 항상 정렬해 반환하므로 여기서도 정렬 후 비교
        var aSorted = a.stream().sorted(java.util.Comparator.comparing(SchemaFingerprint.FieldDesc::name)).toList();
        var bSorted = b.stream().sorted(java.util.Comparator.comparing(SchemaFingerprint.FieldDesc::name)).toList();
        assertThat(SchemaFingerprint.hash(aSorted)).isEqualTo(SchemaFingerprint.hash(bSorted));
    }

    @Test
    void 파라미터가_다르면_다른_지문() {
        var a = List.of(new SchemaFingerprint.FieldDesc("amount", "bytes", true, Map.of("scale", "2")));
        var b = List.of(new SchemaFingerprint.FieldDesc("amount", "bytes", true, Map.of("scale", "4")));
        assertThat(SchemaFingerprint.hash(a)).isNotEqualTo(SchemaFingerprint.hash(b));
    }

    @Test
    void 지문이_실제로_바뀐다_컬럼_추가시() throws Exception {
        var v1 = SchemaFingerprint.afterFields(parse(ENVELOPE_V1));
        var v2 = SchemaFingerprint.afterFields(parse(ENVELOPE_V2_ADD_COLUMN));
        assertThat(SchemaFingerprint.hash(v1)).isNotEqualTo(SchemaFingerprint.hash(v2));
    }

    @Test
    void diff는_추가_삭제_타입변경을_구분한다() {
        var before = List.of(
                new SchemaFingerprint.FieldDesc("id", "int32", false, Map.of()),
                new SchemaFingerprint.FieldDesc("status", "string", true, Map.of()),
                new SchemaFingerprint.FieldDesc("legacy_flag", "int8", true, Map.of()));
        var after = List.of(
                new SchemaFingerprint.FieldDesc("id", "int64", false, Map.of()), // 타입 변경
                new SchemaFingerprint.FieldDesc("status", "string", true, Map.of()), // 동일
                new SchemaFingerprint.FieldDesc("amount", "string", true, Map.of())); // 신규

        var changes = SchemaFingerprint.diff(before, after);
        assertThat(changes).extracting(SchemaFingerprint.FieldChange::kind)
                .containsExactlyInAnyOrder(
                        SchemaFingerprint.ChangeKind.TYPE_CHANGED,
                        SchemaFingerprint.ChangeKind.ADDED,
                        SchemaFingerprint.ChangeKind.REMOVED);
    }

    @Test
    void 추가_삭제만_있으면_오라클_초안_DDL을_만든다() {
        var changes = List.of(
                new SchemaFingerprint.FieldChange(SchemaFingerprint.ChangeKind.ADDED, null,
                        new SchemaFingerprint.FieldDesc("amount", "string", true, Map.of())),
                new SchemaFingerprint.FieldChange(SchemaFingerprint.ChangeKind.REMOVED,
                        new SchemaFingerprint.FieldDesc("legacy_flag", "int8", true, Map.of()), null));

        String ddl = SchemaFingerprint.draftDdl("ORACLE", "TGT", "ORDERS", changes);

        // 단일 실행문(세미콜론 없음) — 승인 시 Statement.execute()에 그대로 넘겨지므로
        // 여러 문장을 세미콜론으로 이으면 안 된다(2026-09-07 수정, 실측 ORA-00900).
        assertThat(ddl).isEqualTo(
                "ALTER TABLE \"TGT\".\"ORDERS\" ADD (\"AMOUNT\" VARCHAR2(4000)) DROP (\"LEGACY_FLAG\")");
        assertThat(ddl).doesNotContain(";");
    }

    @Test
    void 추가_삭제만_있으면_postgresql_초안_DDL을_만든다() {
        var changes = List.of(
                new SchemaFingerprint.FieldChange(SchemaFingerprint.ChangeKind.ADDED, null,
                        new SchemaFingerprint.FieldDesc("amount", "int32", true, Map.of())),
                new SchemaFingerprint.FieldChange(SchemaFingerprint.ChangeKind.REMOVED,
                        new SchemaFingerprint.FieldDesc("legacy_flag", "int8", true, Map.of()), null));

        String ddl = SchemaFingerprint.draftDdl("POSTGRESQL", "tgt", "orders", changes);

        assertThat(ddl).isEqualTo(
                "ALTER TABLE \"tgt\".\"orders\" ADD COLUMN \"amount\" INTEGER, DROP COLUMN \"legacy_flag\"");
        assertThat(ddl).doesNotContain(";");
    }

    @Test
    void 추가_삭제만_있으면_singlestore_초안_DDL을_백틱으로_만든다() {
        var changes = List.of(
                new SchemaFingerprint.FieldChange(SchemaFingerprint.ChangeKind.ADDED, null,
                        new SchemaFingerprint.FieldDesc("amount", "int32", true, Map.of())),
                new SchemaFingerprint.FieldChange(SchemaFingerprint.ChangeKind.REMOVED,
                        new SchemaFingerprint.FieldDesc("legacy_flag", "int8", true, Map.of()), null));

        String ddl = SchemaFingerprint.draftDdl("SINGLESTORE", "tgt", "orders", changes);

        // SingleStore(MySQL 계열)는 큰따옴표가 아니라 백틱을 식별자 구분자로 쓴다.
        assertThat(ddl).isEqualTo(
                "ALTER TABLE `tgt`.`orders` ADD COLUMN `amount` INT, DROP COLUMN `legacy_flag`");
        assertThat(ddl).doesNotContain("\"");
    }

    @Test
    void 타입변경만_있으면_초안이_없다() {
        var changes = List.of(new SchemaFingerprint.FieldChange(SchemaFingerprint.ChangeKind.TYPE_CHANGED,
                new SchemaFingerprint.FieldDesc("id", "int32", false, Map.of()),
                new SchemaFingerprint.FieldDesc("id", "int64", false, Map.of())));

        assertThat(SchemaFingerprint.draftDdl("ORACLE", "TGT", "T1", changes)).isNull();
    }

    @Test
    void 매핑없는_타입_추가는_초안에서_제외된다() {
        var changes = List.of(new SchemaFingerprint.FieldChange(SchemaFingerprint.ChangeKind.ADDED, null,
                new SchemaFingerprint.FieldDesc("payload", "struct", true, Map.of())));

        assertThat(SchemaFingerprint.draftDdl("ORACLE", "TGT", "T1", changes)).isNull();
    }

    @Test
    void summarize는_사람이_읽는_한줄_요약을_만든다() {
        var changes = List.of(
                new SchemaFingerprint.FieldChange(SchemaFingerprint.ChangeKind.ADDED, null,
                        new SchemaFingerprint.FieldDesc("amount", "string", true, Map.of())),
                new SchemaFingerprint.FieldChange(SchemaFingerprint.ChangeKind.REMOVED,
                        new SchemaFingerprint.FieldDesc("legacy", "int8", true, Map.of()), null));

        assertThat(SchemaFingerprint.summarize(changes))
                .contains("추가: amount(string)").contains("삭제: legacy");
    }

    @Test
    void toJson_fromJson은_왕복된다() {
        var fields = List.of(
                new SchemaFingerprint.FieldDesc("id", "int32", false, Map.of()),
                new SchemaFingerprint.FieldDesc("amount", "bytes:org.apache.kafka.connect.data.Decimal",
                        true, Map.of("scale", "2")));

        String json = SchemaFingerprint.toJson(fields);
        List<SchemaFingerprint.FieldDesc> restored = SchemaFingerprint.fromJson(json);

        assertThat(restored).containsExactlyElementsOf(fields);
    }

    @Test
    void fromJson은_잘못된_입력에_빈_목록을_돌려준다() {
        assertThat(SchemaFingerprint.fromJson(null)).isEmpty();
        assertThat(SchemaFingerprint.fromJson("")).isEmpty();
        assertThat(SchemaFingerprint.fromJson("not json")).isEmpty();
    }

    @Test
    void 오라클에서_포스트그레스로_컬럼_타입을_매핑한다() {
        assertThat(SchemaFingerprint.mapNativeType(DbType.ORACLE, DbType.POSTGRESQL, "VARCHAR2"))
                .isEqualTo("TEXT");
        assertThat(SchemaFingerprint.mapNativeType(DbType.ORACLE, DbType.POSTGRESQL, "NUMBER"))
                .isEqualTo("NUMERIC");
        assertThat(SchemaFingerprint.mapNativeType(DbType.ORACLE, DbType.POSTGRESQL, "DATE"))
                .isEqualTo("TIMESTAMP");
        assertThat(SchemaFingerprint.mapNativeType(DbType.ORACLE, DbType.POSTGRESQL, "RAW"))
                .isEqualTo("BYTEA");
    }

    @Test
    void 포스트그레스에서_오라클로_컬럼_타입을_매핑한다() {
        assertThat(SchemaFingerprint.mapNativeType(DbType.POSTGRESQL, DbType.ORACLE, "character varying"))
                .isEqualTo("VARCHAR2(4000)");
        assertThat(SchemaFingerprint.mapNativeType(DbType.POSTGRESQL, DbType.ORACLE, "boolean"))
                .isEqualTo("NUMBER(1)");
        assertThat(SchemaFingerprint.mapNativeType(DbType.POSTGRESQL, DbType.ORACLE, "bytea"))
                .isEqualTo("RAW(2000)");
        assertThat(SchemaFingerprint.mapNativeType(DbType.POSTGRESQL, DbType.ORACLE, "integer"))
                .isEqualTo("NUMBER(10)");
    }

    @Test
    void 오라클에서_싱글스토어로_컬럼_타입을_매핑한다() {
        assertThat(SchemaFingerprint.mapNativeType(DbType.ORACLE, DbType.SINGLESTORE, "VARCHAR2"))
                .isEqualTo("TEXT");
        assertThat(SchemaFingerprint.mapNativeType(DbType.ORACLE, DbType.SINGLESTORE, "NUMBER"))
                .isEqualTo("DECIMAL(65,30)");
        assertThat(SchemaFingerprint.mapNativeType(DbType.ORACLE, DbType.SINGLESTORE, "DATE"))
                .isEqualTo("DATETIME(6)");
        assertThat(SchemaFingerprint.mapNativeType(DbType.ORACLE, DbType.SINGLESTORE, "RAW"))
                .isEqualTo("BLOB");
    }

    @Test
    void 포스트그레스에서_싱글스토어로_컬럼_타입을_매핑한다() {
        assertThat(SchemaFingerprint.mapNativeType(DbType.POSTGRESQL, DbType.SINGLESTORE, "character varying"))
                .isEqualTo("TEXT");
        assertThat(SchemaFingerprint.mapNativeType(DbType.POSTGRESQL, DbType.SINGLESTORE, "boolean"))
                .isEqualTo("BOOL");
        assertThat(SchemaFingerprint.mapNativeType(DbType.POSTGRESQL, DbType.SINGLESTORE, "integer"))
                .isEqualTo("INT");
        assertThat(SchemaFingerprint.mapNativeType(DbType.POSTGRESQL, DbType.SINGLESTORE, "bytea"))
                .isEqualTo("BLOB");
        assertThat(SchemaFingerprint.mapNativeType(DbType.POSTGRESQL, DbType.SINGLESTORE, "date"))
                .isEqualTo("DATE");
    }

    @Test
    void 동종_DB는_원문_타입을_그대로_돌려준다() {
        assertThat(SchemaFingerprint.mapNativeType(DbType.ORACLE, DbType.ORACLE, "NUMBER(10,2)"))
                .isEqualTo("NUMBER(10,2)");
    }

    @Test
    void 매핑_불가_타입은_null을_돌려준다() {
        assertThat(SchemaFingerprint.mapNativeType(DbType.ORACLE, DbType.POSTGRESQL, "XMLTYPE")).isNull();
    }

    @Test
    void 오라클_소스로_포스트그레스_타깃_CREATE_TABLE_초안을_만든다() {
        var columns = List.of(
                new TableColumn("ID", "NUMBER", true),
                new TableColumn("NAME", "VARCHAR2", false));

        String ddl = SchemaFingerprint.draftCreateTable(DbType.ORACLE, DbType.POSTGRESQL,
                "cdc_tmp", "orders", columns);

        assertThat(ddl).isEqualTo(
                "CREATE TABLE \"cdc_tmp\".\"orders\" (\"ID\" NUMERIC, \"NAME\" TEXT, PRIMARY KEY (\"ID\"))");
    }

    @Test
    void 포스트그레스_소스로_오라클_타깃_CREATE_TABLE_초안을_만든다() {
        var columns = List.of(
                new TableColumn("id", "integer", true),
                new TableColumn("active", "boolean", false));

        String ddl = SchemaFingerprint.draftCreateTable(DbType.POSTGRESQL, DbType.ORACLE,
                "CDC_TMP", "ORDERS", columns);

        assertThat(ddl).isEqualTo(
                "CREATE TABLE \"CDC_TMP\".\"ORDERS\" (\"ID\" NUMBER(10), \"ACTIVE\" NUMBER(1), PRIMARY KEY (\"ID\"))");
    }

    @Test
    void 오라클_소스로_싱글스토어_타깃_CREATE_TABLE_초안을_백틱으로_만든다() {
        var columns = List.of(
                new TableColumn("ID", "NUMBER", true),
                new TableColumn("NAME", "VARCHAR2", false));

        String ddl = SchemaFingerprint.draftCreateTable(DbType.ORACLE, DbType.SINGLESTORE,
                "cdc_tmp", "orders", columns);

        assertThat(ddl).isEqualTo(
                "CREATE TABLE `cdc_tmp`.`orders` (`ID` DECIMAL(65,30), `NAME` TEXT, PRIMARY KEY (`ID`))");
    }

    @Test
    void 매핑_불가_컬럼이_있으면_초안_생성을_거부한다() {
        var columns = List.of(
                new TableColumn("ID", "NUMBER", true),
                new TableColumn("DOC", "XMLTYPE", false));

        assertThatThrownBy(() -> SchemaFingerprint.draftCreateTable(DbType.ORACLE, DbType.POSTGRESQL,
                "cdc_tmp", "orders", columns))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DOC(XMLTYPE)");
    }

    @Test
    void PK가_없으면_초안_생성을_거부한다() {
        var columns = List.of(new TableColumn("NAME", "VARCHAR2", false));

        assertThatThrownBy(() -> SchemaFingerprint.draftCreateTable(DbType.ORACLE, DbType.POSTGRESQL,
                "cdc_tmp", "orders", columns))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PK");
    }

    @Test
    void Oracle_네이티브_타입을_Databricks로_매핑한다() {
        DbType o = DbType.ORACLE;
        DbType d = DbType.DATABRICKS;
        assertThat(SchemaFingerprint.mapNativeType(o, d, "NUMBER(10,2)")).isEqualTo("DECIMAL(10,2)");
        assertThat(SchemaFingerprint.mapNativeType(o, d, "NUMBER(10)")).isEqualTo("DECIMAL(10,0)");
        assertThat(SchemaFingerprint.mapNativeType(o, d, "NUMBER")).isEqualTo("DECIMAL(38,10)");
        assertThat(SchemaFingerprint.mapNativeType(o, d, "NUMBER(*,0)")).isEqualTo("DECIMAL(38,10)");
        assertThat(SchemaFingerprint.mapNativeType(o, d, "NUMBER(50,2)")).isEqualTo("DECIMAL(38,2)");
        assertThat(SchemaFingerprint.mapNativeType(o, d, "INTEGER")).isEqualTo("DECIMAL(38,0)");
        assertThat(SchemaFingerprint.mapNativeType(o, d, "FLOAT")).isEqualTo("DOUBLE");
        assertThat(SchemaFingerprint.mapNativeType(o, d, "BINARY_DOUBLE")).isEqualTo("DOUBLE");
        assertThat(SchemaFingerprint.mapNativeType(o, d, "VARCHAR2(100)")).isEqualTo("STRING");
        assertThat(SchemaFingerprint.mapNativeType(o, d, "NVARCHAR2")).isEqualTo("STRING");
        assertThat(SchemaFingerprint.mapNativeType(o, d, "CHAR")).isEqualTo("STRING");
        assertThat(SchemaFingerprint.mapNativeType(o, d, "CLOB")).isEqualTo("STRING");
        assertThat(SchemaFingerprint.mapNativeType(o, d, "DATE")).isEqualTo("TIMESTAMP");
        assertThat(SchemaFingerprint.mapNativeType(o, d, "TIMESTAMP(6)")).isEqualTo("TIMESTAMP");
        assertThat(SchemaFingerprint.mapNativeType(o, d, "TIMESTAMP(6) WITH TIME ZONE")).isEqualTo("TIMESTAMP");
        assertThat(SchemaFingerprint.mapNativeType(o, d, "RAW")).isEqualTo("BINARY");
        assertThat(SchemaFingerprint.mapNativeType(o, d, "BLOB")).isEqualTo("BINARY");
        assertThat(SchemaFingerprint.mapNativeType(o, d, "XMLTYPE")).isNull();
    }

    @Test
    void PostgreSQL_네이티브_타입을_Databricks로_매핑한다() {
        DbType p = DbType.POSTGRESQL;
        DbType d = DbType.DATABRICKS;
        assertThat(SchemaFingerprint.mapNativeType(p, d, "smallint")).isEqualTo("INT");
        assertThat(SchemaFingerprint.mapNativeType(p, d, "integer")).isEqualTo("INT");
        assertThat(SchemaFingerprint.mapNativeType(p, d, "bigint")).isEqualTo("BIGINT");
        assertThat(SchemaFingerprint.mapNativeType(p, d, "numeric(12,3)")).isEqualTo("DECIMAL(12,3)");
        assertThat(SchemaFingerprint.mapNativeType(p, d, "numeric")).isEqualTo("DECIMAL(38,10)");
        assertThat(SchemaFingerprint.mapNativeType(p, d, "real")).isEqualTo("FLOAT");
        assertThat(SchemaFingerprint.mapNativeType(p, d, "double precision")).isEqualTo("DOUBLE");
        assertThat(SchemaFingerprint.mapNativeType(p, d, "text")).isEqualTo("STRING");
        assertThat(SchemaFingerprint.mapNativeType(p, d, "character varying")).isEqualTo("STRING");
        assertThat(SchemaFingerprint.mapNativeType(p, d, "boolean")).isEqualTo("BOOLEAN");
        assertThat(SchemaFingerprint.mapNativeType(p, d, "date")).isEqualTo("DATE");
        assertThat(SchemaFingerprint.mapNativeType(p, d, "timestamp without time zone")).isEqualTo("TIMESTAMP");
        assertThat(SchemaFingerprint.mapNativeType(p, d, "timestamp with time zone")).isEqualTo("TIMESTAMP");
        assertThat(SchemaFingerprint.mapNativeType(p, d, "bytea")).isEqualTo("BINARY");
        assertThat(SchemaFingerprint.mapNativeType(p, d, "uuid")).isEqualTo("STRING");
        assertThat(SchemaFingerprint.mapNativeType(p, d, "jsonb")).isEqualTo("STRING");
        assertThat(SchemaFingerprint.mapNativeType(p, d, "point")).isNull();
    }

    @Test
    void Databricks_CREATE_TABLE_초안은_3part_백틱과_PK_NOT_NULL_제약을_쓴다() {
        var columns = List.of(
                new TableColumn("ID", "NUMBER", true),
                new TableColumn("NAME", "VARCHAR2", false),
                new TableColumn("CREATED", "DATE", false));
        assertThat(SchemaFingerprint.draftCreateTable(DbType.ORACLE, DbType.DATABRICKS, "workspace", "default",
                "orders", columns)).isEqualTo(
                "CREATE TABLE `workspace`.`default`.`orders` (`id` DECIMAL(38,10) NOT NULL, `name` STRING, "
                        + "`created` TIMESTAMP, CONSTRAINT `pk_orders` PRIMARY KEY (`id`))");
        assertThatThrownBy(() -> SchemaFingerprint.draftCreateTable(DbType.ORACLE, DbType.DATABRICKS, null,
                "default", "orders", columns)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("catalog");
    }

    @Test
    void Databricks_ADD_COLUMN_초안은_ADD_COLUMNS_이고_DROP이_섞이면_초안이_없다() {
        var add = new SchemaFingerprint.FieldChange(SchemaFingerprint.ChangeKind.ADDED, null,
                new SchemaFingerprint.FieldDesc("amount", "int64", true, Map.of()));
        var drop = new SchemaFingerprint.FieldChange(SchemaFingerprint.ChangeKind.REMOVED,
                new SchemaFingerprint.FieldDesc("legacy", "int8", true, Map.of()), null);
        assertThat(SchemaFingerprint.draftDdl("DATABRICKS", "default", "orders", List.of(add)))
                .isEqualTo("ALTER TABLE `default`.`orders` ADD COLUMNS (`amount` BIGINT)");
        assertThat(SchemaFingerprint.draftDdl("DATABRICKS", "default", "orders", List.of(add, drop))).isNull();
        assertThat(SchemaFingerprint.draftDdl("DATABRICKS", "default", "orders", List.of(drop))).isNull();
    }
}
