package io.deltazium.backend.metrics;

import java.util.Map;

import io.deltazium.backend.capture.SnapshotNotificationPoller;
import io.deltazium.backend.registration.RegisteredTableRepository;
import io.deltazium.backend.registry.DbConnectionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 파일명 : KafkaMetricsServiceTest.java
 * 작성일자 : 26. 09. 27.
 * 작성자 : 최남희
 * 설명 : /api/metrics/tables의 snapshotInProgress 산출(isSnapshotting) 단위 테스트 —
 * 소스 A가 스냅샷 진행 중, 소스 B가 완료된 상태에서 A만 true인지 검증한다.
 * tableMetrics() 전체는 Kafka AdminClient가 필요해 이 테스트 범위 밖이다(기존
 * KafkaMetricsService에도 통합 테스트가 없다 — Kafka 실제 기동이 필요한 부분은
 * smoke-test.sh 몫).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 27.       | 최남희  | 최초 생성 — 재스냅샷 중 lag 경고 제외(docs/internals.md) 산출 검증
 * --------------------------------------------------
 */
class KafkaMetricsServiceTest {

    private SnapshotNotificationPoller notifications;
    private KafkaMetricsService service;

    @BeforeEach
    void setUp() {
        notifications = mock(SnapshotNotificationPoller.class);
        service = new KafkaMetricsService(mock(RegisteredTableRepository.class),
                mock(DbConnectionService.class), notifications, "localhost:9092");
    }

    @Test
    void 스냅샷_IN_PROGRESS인_소스만_true다() {
        when(notifications.status("srcA")).thenReturn(
                new SnapshotNotificationPoller.SnapshotStatus("IN_PROGRESS", null, Map.of(), 1L, null));
        when(notifications.status("srcB")).thenReturn(
                new SnapshotNotificationPoller.SnapshotStatus("COMPLETED", null, Map.of(), 1L, 2L));

        assertThat(service.isSnapshotting("srcA")).isTrue();
        assertThat(service.isSnapshotting("srcB")).isFalse();
    }

    @Test
    void REQUESTED도_진행_중으로_본다() {
        when(notifications.status("srcA")).thenReturn(
                new SnapshotNotificationPoller.SnapshotStatus("REQUESTED", null, Map.of(), 1L, null));

        assertThat(service.isSnapshotting("srcA")).isTrue();
    }

    @Test
    void ABORTED나_NONE은_진행_중이_아니다() {
        when(notifications.status("srcA")).thenReturn(
                new SnapshotNotificationPoller.SnapshotStatus("NONE", null, Map.of(), null, null));
        when(notifications.status("srcB")).thenReturn(
                new SnapshotNotificationPoller.SnapshotStatus("ABORTED", null, Map.of(), 1L, 2L));

        assertThat(service.isSnapshotting("srcA")).isFalse();
        assertThat(service.isSnapshotting("srcB")).isFalse();
    }

    @Test
    void topicPrefix가_null이면_false다() {
        assertThat(service.isSnapshotting(null)).isFalse();
    }
}
