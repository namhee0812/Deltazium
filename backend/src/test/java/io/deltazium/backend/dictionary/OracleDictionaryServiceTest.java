package io.deltazium.backend.dictionary;

import org.junit.jupiter.api.Test;

import static io.deltazium.backend.dictionary.OracleDictionaryService.parsePattern;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OracleDictionaryServiceTest {

    @Test
    void 스키마_전체_패턴() {
        assertThat(parsePattern("cdc.*")).containsExactly("CDC", "%");
    }

    @Test
    void 부분_와일드카드_패턴() {
        assertThat(parsePattern("CDC.TEST_*")).containsExactly("CDC", "TEST_%");
    }

    @Test
    void 단일_테이블() {
        assertThat(parsePattern("cdc.test_table_01")).containsExactly("CDC", "TEST_TABLE_01");
    }

    @Test
    void 스키마_한정자_없으면_거부() {
        assertThatThrownBy(() -> parsePattern("ORDERS"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SCHEMA.TABLE");
    }
}
