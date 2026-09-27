package io.deltazium.backend.dictionary;

import java.util.List;

import io.deltazium.backend.registry.DbConnection;
import io.deltazium.backend.registry.DbType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 파일명 : SingleStoreDictionaryServiceTest.java
 * 작성일자 : 26. 09. 28.
 * 작성자 : 최남희
 * 설명 : SingleStore 딕셔너리 단위 테스트 — 소스 전용 메서드가 명확히 실패하는지만 검증한다
 * (listColumns는 실제 information_schema 조회가 필요해 라이브 통합 범위, docs/internals.md).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 28.       | 최남희  | 최초 생성 — SingleStore 타깃 지원
 * --------------------------------------------------
 */
class SingleStoreDictionaryServiceTest {

    private final SingleStoreDictionaryService service = new SingleStoreDictionaryService();

    private static DbConnection target() {
        return new DbConnection(1L, "ss-tgt", "SINGLESTORE", "TARGET",
                "127.0.0.1", 3307, "cdc_tgt", "root", "secret");
    }

    @Test
    void dbType은_SINGLESTORE다() {
        assertThat(service.dbType()).isEqualTo(DbType.SINGLESTORE);
    }

    @Test
    void 소스_전용_메서드는_UnsupportedOperationException으로_실패한다() {
        DbConnection conn = target();
        assertThatThrownBy(() -> service.listTables(conn, "cdc.*"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("소스로 지원하지 않는다");
        assertThatThrownBy(() -> service.databaseChecks(conn))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> service.privilegeChecks(conn))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> service.captureSetupLabel())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> service.captureSetupPreview(conn, List.of("cdc.orders")))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> service.applyCaptureSetup(conn, List.of("cdc.orders")))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
