package io.deltazium.backend.connect;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 파일명 : ConnectorNamesTest.java
 * 작성일자 : 26. 09. 07.
 * 작성자 : 최남희
 * 설명 : 커넥터·컨슈머 그룹·복구 토픽 이름 규칙(architecture.md 4절) 단위 테스트.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 최초 생성 — 다중 소스·다중 타깃 ②
 * --------------------------------------------------
 */
class ConnectorNamesTest {

    @Test
    void source_이름은_소스당_1개_규칙을_따른다() {
        assertThat(ConnectorNames.source("dz")).isEqualTo("dz-source-dz");
        assertThat(ConnectorNames.source("pgsrc")).isEqualTo("dz-source-pgsrc");
    }

    @Test
    void iceberg_sink_이름은_소스당_1개_규칙을_따른다() {
        assertThat(ConnectorNames.icebergSink("dz")).isEqualTo("dz-iceberg-dz");
    }

    @Test
    void jdbc_sink_이름은_prefix와_suffix를_모두_담는다() {
        assertThat(ConnectorNames.jdbcSink("dz", "cdc_t1")).isEqualTo("dz-jdbc-sink-dz-cdc_t1");
        // 서로 다른 소스가 같은 schema.table(=같은 suffix)을 등록해도 커넥터 이름은 겹치지 않는다
        assertThat(ConnectorNames.jdbcSink("pgsrc", "cdc_t1")).isEqualTo("dz-jdbc-sink-pgsrc-cdc_t1");
    }

    @Test
    void recovery_sink_이름과_토픽도_prefix를_담는다() {
        assertThat(ConnectorNames.recoverySink("dz", "cdc_t1")).isEqualTo("dz-recovery-sink-dz-cdc_t1");
        assertThat(ConnectorNames.recoveryTopic("dz", "cdc_t1")).isEqualTo("dz-recovery.dz.cdc_t1");
    }

    @Test
    void consumer_group은_connect_접두를_붙인다() {
        assertThat(ConnectorNames.consumerGroup("dz-jdbc-sink-dz-cdc_t1"))
                .isEqualTo("connect-dz-jdbc-sink-dz-cdc_t1");
    }

    @Test
    void capture_topic과_notification_topic() {
        assertThat(ConnectorNames.captureTopic("dz", "CDC", "T1")).isEqualTo("dz.CDC.T1");
        assertThat(ConnectorNames.notificationTopic("dz")).isEqualTo("dz-notifications");
    }
}
