package io.deltazium.backend.iceberg;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 파일명 : ChangelogTableServiceTest.java
 * 작성일자 : 26. 07. 26.
 * 작성자 : 최남희
 * 설명 : changelog 테이블 사전 생성 로직 단위 테스트.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 26.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
class ChangelogTableServiceTest {

    private final IcebergProperties props = new IcebergProperties(
            "jdbc:postgresql://x/iceberg", "u", "p", "s3://wh/warehouse",
            "http://x:9010", "ak", "sk", "changelog");

    private final ChangelogTableService service = new ChangelogTableService(props);

    @Test
    void changelog_테이블명은_스키마_테이블_소문자() {
        assertThat(service.changelogTableName("SRC", "ORDERS")).isEqualTo("changelog.src_orders");
        assertThat(service.changelogTableName("CDC", "TEST_TABLE_01"))
                .isEqualTo("changelog.cdc_test_table_01");
    }

    @Test
    void 기본_골격은_envelope_핵심_필드를_담는다() {
        Schema s = ChangelogTableService.baseSchema();
        assertThat(s.findField("op")).isNotNull();
        assertThat(s.findField("ts_ms")).isNotNull();
        assertThat(s.findField("source.scn")).isNotNull();
        assertThat(s.findField("source.txId")).isNotNull();
        assertThat(s.findField("source.ts_ms")).isNotNull();
        assertThat(s.findField("source.table")).isNotNull();
        // 전부 optional — sink evolve union과 충돌하지 않기 위한 전제 (5.1절)
        assertThat(s.columns()).allSatisfy(f -> assertThat(f.isOptional()).isTrue());
    }

    @Test
    void 파티션은_source_ts_ms의_1일_truncate() {
        Schema s = ChangelogTableService.baseSchema();
        PartitionSpec spec = ChangelogTableService.partitionSpec(s);
        assertThat(spec.fields()).hasSize(1);
        assertThat(spec.fields().get(0).transform().toString())
                .isEqualTo("truncate[86400000]");
        assertThat(spec.fields().get(0).sourceId())
                .isEqualTo(s.findField("source.ts_ms").fieldId());
    }
}
