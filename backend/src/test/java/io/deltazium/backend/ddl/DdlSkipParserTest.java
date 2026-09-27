package io.deltazium.backend.ddl;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 파일명 : DdlSkipParserTest.java
 * 작성일자 : 26. 09. 27.
 * 작성자 : 최남희
 * 설명 : DDL 건너뛰기(SKIPPED)의 ADD/DROP COLUMN 단순 인식 단위 테스트(architecture.md 7절 개정).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 27.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
class DdlSkipParserTest {

    @Test
    void 오라클_단일_ADD를_인식한다() {
        var edit = DdlSkipParser.parse("ALTER TABLE \"TGT\".\"ORDERS\" ADD (\"AMOUNT\" VARCHAR2(4000))");
        assertThat(edit).isPresent();
        assertThat(edit.get().kind()).isEqualTo(DdlSkipParser.Kind.ADD);
        assertThat(edit.get().column()).isEqualTo("AMOUNT");
    }

    @Test
    void 오라클_단일_DROP을_인식한다() {
        var edit = DdlSkipParser.parse("ALTER TABLE \"TGT\".\"ORDERS\" DROP (\"LEGACY_FLAG\")");
        assertThat(edit).isPresent();
        assertThat(edit.get().kind()).isEqualTo(DdlSkipParser.Kind.DROP);
        assertThat(edit.get().column()).isEqualTo("LEGACY_FLAG");
    }

    @Test
    void 포스트그레스_ADD_COLUMN을_인식한다() {
        var edit = DdlSkipParser.parse("ALTER TABLE \"tgt\".\"orders\" ADD COLUMN \"amount\" INTEGER");
        assertThat(edit).isPresent();
        assertThat(edit.get().kind()).isEqualTo(DdlSkipParser.Kind.ADD);
        assertThat(edit.get().column()).isEqualTo("amount");
    }

    @Test
    void 포스트그레스_DROP_COLUMN을_인식한다() {
        var edit = DdlSkipParser.parse("ALTER TABLE \"tgt\".\"orders\" DROP COLUMN \"legacy\"");
        assertThat(edit).isPresent();
        assertThat(edit.get().kind()).isEqualTo(DdlSkipParser.Kind.DROP);
        assertThat(edit.get().column()).isEqualTo("legacy");
    }

    @Test
    void 따옴표_없는_형태도_인식한다() {
        var edit = DdlSkipParser.parse("ALTER TABLE CDC.T1 ADD COL2 NUMBER");
        assertThat(edit).isPresent();
        assertThat(edit.get().column()).isEqualTo("COL2");
    }

    @Test
    void ADD와_DROP이_섞인_복합_ALTER는_인식하지_않는다() {
        assertThat(DdlSkipParser.parse(
                "ALTER TABLE \"TGT\".\"ORDERS\" ADD (\"AMOUNT\" VARCHAR2(4000)) DROP (\"LEGACY_FLAG\")"))
                .isEmpty();
    }

    @Test
    void 컬럼이_여러_개면_인식하지_않는다() {
        assertThat(DdlSkipParser.parse(
                "ALTER TABLE \"tgt\".\"orders\" ADD COLUMN \"amount\" INTEGER, ADD COLUMN \"note\" TEXT"))
                .isEmpty();
    }

    @Test
    void 타입_변경_등_ADD_DROP이_없는_문장은_인식하지_않는다() {
        assertThat(DdlSkipParser.parse("ALTER TABLE CDC.T1 MODIFY (X NUMBER)")).isEmpty();
    }

    @Test
    void 빈_문자열은_인식하지_않는다() {
        assertThat(DdlSkipParser.parse("")).isEmpty();
        assertThat(DdlSkipParser.parse(null)).isEmpty();
    }
}
