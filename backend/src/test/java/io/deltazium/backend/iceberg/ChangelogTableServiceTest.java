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
 * 26. 09. 05.       | 최남희  | 다중 소스·다중 타깃 ①: namespace를 topic-prefix 기반 계산값으로,
 * |                          | 기본 골격에 _pos 검증 추가 (5.1절)
 * --------------------------------------------------
 */
class ChangelogTableServiceTest {

    private final IcebergProperties props = new IcebergProperties(
            "jdbc:postgresql://x/iceberg", "u", "p", "s3://wh/warehouse",
            "http://x:9010", "ak", "sk");

    private final ChangelogTableService service = new ChangelogTableService(props, "dz");

    @Test
    void changelog_테이블명은_소스별_namespace_스키마_테이블_소문자() {
        assertThat(service.changelogTableName("SRC", "ORDERS")).isEqualTo("changelog_dz.src_orders");
        assertThat(service.changelogTableName("CDC", "TEST_TABLE_01"))
                .isEqualTo("changelog_dz.cdc_test_table_01");
    }

    @Test
    void namespace는_topic_prefix에서_계산된다() {
        ChangelogTableService other = new ChangelogTableService(props, "ANOTHER");
        assertThat(other.namespace()).isEqualTo("changelog_another");
    }

    @Test
    void 기본_골격은_envelope_핵심_필드와_pos를_담는다() {
        Schema s = ChangelogTableService.baseSchema();
        assertThat(s.findField("op")).isNotNull();
        assertThat(s.findField("ts_ms")).isNotNull();
        assertThat(s.findField("source.scn")).isNotNull();
        assertThat(s.findField("source.txId")).isNotNull();
        assertThat(s.findField("source.ts_ms")).isNotNull();
        assertThat(s.findField("source.table")).isNotNull();
        assertThat(s.findField("_pos.topic")).isNotNull();
        assertThat(s.findField("_pos.partition")).isNotNull();
        assertThat(s.findField("_pos.offset")).isNotNull();
        assertThat(s.findField("_pos.timestamp")).isNotNull();
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
