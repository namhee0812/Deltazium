package io.deltazium.backend.ddl;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 파일명 : DdlEventParserTest.java
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : DDL 파싱·비전파성 DDL 무시 분류 단위 테스트.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
class DdlEventParserTest {

    private static final String STREAMING_EVENT = """
            {"schema":{"type":"struct"},"payload":{
              "source":{"version":"3.6.0.Final","connector":"oracle","name":"dz",
                        "ts_ms":1753700000000,"snapshot":"false","db":"ORCL",
                        "schema":"CDC","table":"AUTO_100","txId":"tx1","scn":"31066120000"},
              "ts_ms":1753700000123,"databaseName":"ORCL",
              "ddl":"ALTER TABLE CDC.AUTO_100 ADD (PROMO VARCHAR2(10))",
              "tableChanges":[{"type":"ALTER","id":"\\"CDC\\".\\"AUTO_100\\""}]}}""";

    @Test
    void 스트리밍_DDL은_DETECTED_대상으로_파싱된다() {
        var p = DdlEventParser.parse(STREAMING_EVENT).orElseThrow();
        assertThat(p.snapshot()).isFalse();
        assertThat(p.schemaName()).isEqualTo("CDC");
        assertThat(p.tableName()).isEqualTo("AUTO_100");
        assertThat(p.scn()).isEqualTo("31066120000");
        assertThat(p.ddl()).startsWith("ALTER TABLE");
        assertThat(p.tsMs()).isEqualTo(1753700000123L);
    }

    @Test
    void 스냅샷_이벤트는_snapshot_플래그가_선다() {
        String snap = STREAMING_EVENT.replace("\"snapshot\":\"false\"", "\"snapshot\":\"true\"");
        assertThat(DdlEventParser.parse(snap).orElseThrow().snapshot()).isTrue();
    }

    @Test
    void ddl이_없는_이벤트는_건너뛴다() {
        assertThat(DdlEventParser.parse("{\"payload\":{\"source\":{}}}")).isEmpty();
    }

    @Test
    void JSON이_아니면_건너뛴다() {
        assertThat(DdlEventParser.parse("not-json")).isEmpty();
    }

    @Test
    void supplemental_logging과_비전파성_DDL은_무시_대상() {
        assertThat(DdlEventParser.ignorable(
                "ALTER TABLE CDC.T1 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS")).isTrue();
        assertThat(DdlEventParser.ignorable("GRANT SELECT ON CDC.T1 TO APP")).isTrue();
        assertThat(DdlEventParser.ignorable("ANALYZE TABLE CDC.T1 COMPUTE STATISTICS")).isTrue();
        assertThat(DdlEventParser.ignorable("COMMENT ON TABLE CDC.T1 IS 'x'")).isTrue();

        assertThat(DdlEventParser.ignorable("ALTER TABLE CDC.T1 ADD (X NUMBER)")).isFalse();
        assertThat(DdlEventParser.ignorable("TRUNCATE TABLE CDC.T1")).isFalse();
        assertThat(DdlEventParser.ignorable("CREATE TABLE CDC.T2 (ID NUMBER)")).isFalse();
    }
}
