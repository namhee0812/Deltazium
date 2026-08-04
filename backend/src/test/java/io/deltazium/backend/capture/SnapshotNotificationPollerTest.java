package io.deltazium.backend.capture;

import io.deltazium.backend.events.TableEventService;
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
 */
class SnapshotNotificationPollerTest {

    private TableEventService events;
    private SnapshotNotificationPoller poller;

    @BeforeEach
    void setUp() {
        events = mock(TableEventService.class);
        poller = new SnapshotNotificationPoller(events, "localhost:9092", "dz");
    }

    @Test
    void STARTED_수신시_IN_PROGRESS로_전이하고_이벤트를_남긴다() {
        poller.handle("""
                {"id":"ff81","aggregate_type":"Initial Snapshot","type":"STARTED",
                 "additional_data":{"connector_name":"dz"},"timestamp":"1695817046353"}""");
        assertThat(poller.status().phase()).isEqualTo("IN_PROGRESS");
        verify(events).info(eq("-"), eq("dz-source"), eq("SNAPSHOT_STARTED"), anyString());
    }

    @Test
    void TABLE_SCAN_COMPLETED는_테이블별_행수를_누적한다() {
        poller.handle("""
                {"aggregate_type":"Initial Snapshot","type":"STARTED",
                 "additional_data":{},"timestamp":"1"}""");
        poller.handle("""
                {"aggregate_type":"Initial Snapshot","type":"TABLE_SCAN_COMPLETED",
                 "additional_data":{"scanned_collection":"ORCL.CDC.NH_MIX_TABLE_01",
                 "total_rows_scanned":"96949","status":"SUCCEEDED"},"timestamp":"2"}""");
        assertThat(poller.status().tables())
                .containsEntry("ORCL.CDC.NH_MIX_TABLE_01", 96949L);
        verify(events).info(eq("-"), eq("dz-source"), eq("SNAPSHOT_TABLE_COMPLETED"),
                contains("96949행"));
    }

    @Test
    void COMPLETED_수신시_완료로_전이한다() {
        poller.handle("""
                {"aggregate_type":"Initial Snapshot","type":"STARTED",
                 "additional_data":{},"timestamp":"1"}""");
        poller.handle("""
                {"aggregate_type":"Initial Snapshot","type":"COMPLETED",
                 "additional_data":{"connector_name":"dz"},"timestamp":"2"}""");
        assertThat(poller.status().phase()).isEqualTo("COMPLETED");
        assertThat(poller.status().completedAtMs()).isEqualTo(2L);
        verify(events).info(eq("-"), eq("dz-source"), eq("SNAPSHOT_COMPLETED"), contains("go-live"));
    }

    @Test
    void Initial_Snapshot_외의_aggregate는_무시한다() {
        poller.handle("""
                {"aggregate_type":"Incremental Snapshot","type":"STARTED",
                 "additional_data":{},"timestamp":"1"}""");
        assertThat(poller.status().phase()).isEqualTo("NONE");
    }

    @Test
    void 잘못된_JSON은_조용히_건너뛴다() {
        poller.handle("not-json");
        assertThat(poller.status().phase()).isEqualTo("NONE");
    }

    @Test
    void schemas_enabled_봉투에_싸인_notification도_payload를_언랩해_처리한다() {
        // 실측: 워커 JSON converter(schemas.enabled=true)가 씌우는 {schema, payload} 형식
        poller.handle("""
                {"schema":{"type":"struct","name":"io.debezium.connector.common.Notification"},
                 "payload":{"id":"f921","type":"STARTED","aggregate_type":"Initial Snapshot",
                 "additional_data":{"connector_name":"dz"},"timestamp":1785828000000}}""");
        assertThat(poller.status().phase()).isEqualTo("IN_PROGRESS");
    }
}
