package io.deltazium.backend.ddl;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
}
