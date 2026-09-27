package io.deltazium.backend.capture;

import io.deltazium.backend.events.TableEventService;
import io.deltazium.backend.registry.DbConnectionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * 파일명 : SnapshotNotificationPollerTest.java
 * 작성일자 : 26. 08. 04.
 * 작성자 : 최남희
 * 설명 : Debezium notification 파싱 단위 테스트 — 공식 문서(notification.html)의
 * Initial Snapshot 예시 JSON 그대로를 입력으로 상태 전이·이벤트 적재를 검증한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 04.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ②: 생성자 인자를 topicPrefix(String)에서
 * |                          | DbConnectionService로 교체(전역 topic-prefix 제거)
 * --------------------------------------------------
 * 26. 09. 27.       | 최남희  | 상태가 소스별(topicPrefix별) Map으로 바뀌어 handle(topic, json)·
 * |                          | status(topicPrefix)로 호출부 전면 수정, 소스별 독립성 테스트 추가
 * --------------------------------------------------
 */
class SnapshotNotificationPollerTest {

    private static final String TOPIC = "dz-notifications";

    private TableEventService events;
    private SnapshotNotificationPoller poller;

    @BeforeEach
    void setUp() {
        events = mock(TableEventService.class);
        poller = new SnapshotNotificationPoller(events, mock(DbConnectionService.class), "localhost:9092");
    }

    @Test
    void STARTED_수신시_IN_PROGRESS로_전이하고_이벤트를_남긴다() {
        poller.handle(TOPIC, """
                {"id":"ff81","aggregate_type":"Initial Snapshot","type":"STARTED",
                 "additional_data":{"connector_name":"dz"},"timestamp":"1695817046353"}""");
        assertThat(poller.status("dz").phase()).isEqualTo("IN_PROGRESS");
        verify(events).info(eq("-"), eq("dz-source-dz"), eq("SNAPSHOT_STARTED"), anyString());
    }

    @Test
    void TABLE_SCAN_COMPLETED는_테이블별_행수를_누적한다() {
        poller.handle(TOPIC, """
                {"aggregate_type":"Initial Snapshot","type":"STARTED",
                 "additional_data":{},"timestamp":"1"}""");
        poller.handle(TOPIC, """
                {"aggregate_type":"Initial Snapshot","type":"TABLE_SCAN_COMPLETED",
                 "additional_data":{"scanned_collection":"ORCL.CDC.NH_MIX_TABLE_01",
                 "total_rows_scanned":"96949","status":"SUCCEEDED"},"timestamp":"2"}""");
        assertThat(poller.status("dz").tables())
                .containsEntry("ORCL.CDC.NH_MIX_TABLE_01", 96949L);
        verify(events).info(eq("-"), eq("dz-source-dz"), eq("SNAPSHOT_TABLE_COMPLETED"),
                contains("96949행"));
    }

    @Test
    void COMPLETED_수신시_완료로_전이한다() {
        poller.handle(TOPIC, """
                {"aggregate_type":"Initial Snapshot","type":"STARTED",
                 "additional_data":{},"timestamp":"1"}""");
        poller.handle(TOPIC, """
                {"aggregate_type":"Initial Snapshot","type":"COMPLETED",
                 "additional_data":{"connector_name":"dz"},"timestamp":"2"}""");
        assertThat(poller.status("dz").phase()).isEqualTo("COMPLETED");
        assertThat(poller.status("dz").completedAtMs()).isEqualTo(2L);
        verify(events).info(eq("-"), eq("dz-source-dz"), eq("SNAPSHOT_COMPLETED"), contains("go-live"));
    }

    @Test
    void Initial_Snapshot_외의_aggregate는_무시한다() {
        poller.handle(TOPIC, """
                {"aggregate_type":"Incremental Snapshot","type":"STARTED",
                 "additional_data":{},"timestamp":"1"}""");
        assertThat(poller.status("dz").phase()).isEqualTo("NONE");
    }

    @Test
    void 잘못된_JSON은_조용히_건너뛴다() {
        poller.handle(TOPIC, "not-json");
        assertThat(poller.status("dz").phase()).isEqualTo("NONE");
    }

    @Test
    void schemas_enabled_봉투에_싸인_notification도_payload를_언랩해_처리한다() {
        // 실측: 워커 JSON converter(schemas.enabled=true)가 씌우는 {schema, payload} 형식
        poller.handle(TOPIC, """
                {"schema":{"type":"struct","name":"io.debezium.connector.common.Notification"},
                 "payload":{"id":"f921","type":"STARTED","aggregate_type":"Initial Snapshot",
                 "additional_data":{"connector_name":"dz"},"timestamp":1785828000000}}""");
        assertThat(poller.status("dz").phase()).isEqualTo("IN_PROGRESS");
    }

    @Test
    void 소스별_상태는_독립적으로_유지된다() {
        // 재스냅샷 중 lag 경고 제외(KafkaMetricsService.isSnapshotting)가 소스 단위로 정확해야
        // 하므로, 한 소스의 notification이 다른 소스의 상태를 건드리지 않는지 확인한다.
        poller.handle("dzA-notifications", """
                {"aggregate_type":"Initial Snapshot","type":"STARTED",
                 "additional_data":{},"timestamp":"1"}""");
        poller.handle("dzB-notifications", """
                {"aggregate_type":"Initial Snapshot","type":"COMPLETED",
                 "additional_data":{},"timestamp":"2"}""");

        assertThat(poller.status("dzA").phase()).isEqualTo("IN_PROGRESS");
        assertThat(poller.status("dzB").phase()).isEqualTo("COMPLETED");
        assertThat(poller.byPrefix()).containsOnlyKeys("dzA", "dzB");
    }
}
