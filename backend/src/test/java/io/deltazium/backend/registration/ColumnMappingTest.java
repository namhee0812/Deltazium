package io.deltazium.backend.registration;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ColumnMappingTest {

    @Test
    void 정상_변환식은_소스_컬럼을_돌려준다() {
        assertThat(new ColumnMapping("COL2", "${COL1}", true).sourceColumn()).contains("COL1");
        assertThat(new ColumnMapping("C", "${col_1#$}", true).sourceColumn()).contains("COL_1#$");
        assertThat(new ColumnMapping("C", "  ${COL1}  ", true).sourceColumn()).contains("COL1");
    }

    @Test
    void 잘못된_변환식은_거부된다() {
        for (String bad : new String[] {"${{COL1}", "${COL1", "COL1", "${}", "${1COL}",
                "$COL1", "${COL1}}", "${COL1} ${COL2}", "", "${UPPER(COL1)}"}) {
            assertThat(new ColumnMapping("C", bad, true).sourceColumn())
                    .as("거부돼야 함: %s", bad).isEmpty();
        }
    }

    @Test
    void 동일명_매핑_판정은_대소문자를_무시한다() {
        assertThat(new ColumnMapping("COL1", "${col1}", true).isIdentity()).isTrue();
        assertThat(new ColumnMapping("COL2", "${COL1}", true).isIdentity()).isFalse();
    }

    @Test
    void 전_컬럼_동일명_활성이면_include_필터를_생략한다() {
        Map<String, String> config = RegistrationService.fieldIncludeConfig(List.of(
                new ColumnMapping("ID", "${ID}", true),
                new ColumnMapping("AMOUNT", "${AMOUNT}", true)));
        assertThat(config).isEmpty();
    }

    @Test
    void 해제_또는_리네임이_있으면_동일명_활성만_include에_들어간다() {
        Map<String, String> config = RegistrationService.fieldIncludeConfig(List.of(
                new ColumnMapping("ID", "${ID}", true),
                new ColumnMapping("AMOUNT", "${AMOUNT}", false),        // 해제
                new ColumnMapping("STATUS2", "${STATUS}", true)));      // 리네임 — 스톡 미지원, 제외
        assertThat(config).containsEntry("field.include.list", "ID");
    }
}
