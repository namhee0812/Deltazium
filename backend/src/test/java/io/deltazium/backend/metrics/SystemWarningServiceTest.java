package io.deltazium.backend.metrics;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.deltazium.backend.connect.ConnectClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 파일명 : SystemWarningServiceTest.java
 * 작성일자 : 26. 09. 23.
 * 작성자 : 최남희
 * 설명 : 경고/알림 분리 단위 테스트 — 사용자가 정지한 커넥터(PAUSED)가 WARN이 아니라
 * INFO로 노출되는지, ack 후 목록에서 빠지는지, 재정지(sinceMs 변경) 시 다시 뜨는지,
 * INFO가 아닌 항목은 ack가 거부되는지를 검증한다. ConnectClient·KafkaMetricsService는
 * mock, system_warning_acks는 메모리 fake로 대체(OverviewServiceTest와 동일한
 * "외부 의존은 전부 mock" 패턴).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 23.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
class SystemWarningServiceTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** system_warning_acks 테이블의 메모리 fake — 상태를 자연스럽게 유지해야 하는 시나리오라
     *  Mockito 순차 스텁보다 이 편이 읽기 쉽다. */
    private static class FakeAckRepository implements SystemWarningAckRepository {
        final Map<String, WarningObservation> rows = new LinkedHashMap<>();

        @Override
        public List<WarningObservation> findAll() {
            return new ArrayList<>(rows.values());
        }

        @Override
        public void insertObserved(String id, long sinceMs) {
            rows.put(id, new WarningObservation(id, sinceMs, false));
        }

        @Override
        public void ack(String id) {
            WarningObservation row = rows.get(id);
            if (row != null) {
                rows.put(id, new WarningObservation(row.id(), row.sinceMs(), true));
            }
        }

        @Override
        public void deleteIds(List<String> ids) {
            ids.forEach(rows::remove);
        }
    }

    private ConnectClient connect;
    private KafkaMetricsService metrics;
    private FakeAckRepository acks;
    private SystemWarningService service;

    @BeforeEach
    void setUp() {
        connect = mock(ConnectClient.class);
        metrics = mock(KafkaMetricsService.class);
        acks = new FakeAckRepository();
        when(metrics.reachable()).thenReturn(true);
        // diskWarnPct=101 — 테스트 환경 실제 디스크 사용률과 무관하게 디스크 경고가 섞이지 않게 한다.
        service = new SystemWarningService(metrics, connect, acks, System.getProperty("java.io.tmpdir"), 101);
    }

    private void stubConnector(String name, String state) throws Exception {
        when(connect.listConnectors()).thenReturn(JSON.readTree(
                "{\"" + name + "\":{\"status\":{\"connector\":{\"state\":\"" + state + "\"},"
                        + "\"tasks\":[{\"state\":\"" + state + "\"}]}}}"));
    }

    @Test
    void PAUSED_커넥터는_WARN이_아니라_INFO다() throws Exception {
        stubConnector("dz-jdbc-sink-dz-src_t1", "PAUSED");

        List<SystemWarningService.SystemWarning> ws = service.warnings().warnings();

        assertThat(ws).hasSize(1);
        assertThat(ws.get(0).severity()).isEqualTo("INFO");
        assertThat(ws.get(0).id()).isEqualTo("connector-paused:dz-jdbc-sink-dz-src_t1");
        assertThat(ws.get(0).sinceMs()).isNotNull();
    }

    @Test
    void FAILED_커넥터는_그대로_CRITICAL이다() throws Exception {
        stubConnector("dz-jdbc-sink-dz-src_t1", "FAILED");

        List<SystemWarningService.SystemWarning> ws = service.warnings().warnings();

        assertThat(ws).hasSize(1);
        assertThat(ws.get(0).severity()).isEqualTo("CRITICAL");
    }

    @Test
    void ack하면_목록에서_제외된다() throws Exception {
        stubConnector("dz-jdbc-sink-dz-src_t1", "PAUSED");
        String id = "connector-paused:dz-jdbc-sink-dz-src_t1";

        assertThat(service.warnings().warnings()).hasSize(1);

        service.ack(id);

        assertThat(service.warnings().warnings()).isEmpty();
    }

    @Test
    void 재개_후_다시_정지되면_sinceMs가_바뀌어_ack와_무관하게_다시_뜬다() throws Exception {
        String name = "dz-jdbc-sink-dz-src_t1";
        String id = "connector-paused:" + name;
        stubConnector(name, "PAUSED");
        long firstSince = service.warnings().warnings().get(0).sinceMs();
        service.ack(id);
        assertThat(service.warnings().warnings()).isEmpty();

        // 재개 — 관측 행이 해소되어 지워진다
        stubConnector(name, "RUNNING");
        assertThat(service.warnings().warnings()).isEmpty();

        // 재정지 — 새 sinceMs로 다시 관측되고, 이전 ack와 무관하므로 다시 노출된다
        Thread.sleep(2);
        stubConnector(name, "PAUSED");
        List<SystemWarningService.SystemWarning> ws = service.warnings().warnings();
        assertThat(ws).hasSize(1);
        assertThat(ws.get(0).sinceMs()).isNotEqualTo(firstSince);
    }

    @Test
    void 재기동_해도_ack는_유지된다_DB_기반_sinceMs() throws Exception {
        // in-memory firstSeen만 리셋되는 상황을 재현 — 동일 acks(DB)를 공유하는 새 서비스 인스턴스
        String name = "dz-jdbc-sink-dz-src_t1";
        String id = "connector-paused:" + name;
        stubConnector(name, "PAUSED");
        service.ack(id);
        assertThat(service.warnings().warnings()).isEmpty();

        SystemWarningService restarted =
                new SystemWarningService(metrics, connect, acks, System.getProperty("java.io.tmpdir"), 101);
        assertThat(restarted.warnings().warnings()).isEmpty();
    }

    @Test
    void INFO가_아닌_항목은_ack가_거부된다() throws Exception {
        stubConnector("dz-jdbc-sink-dz-src_t1", "FAILED");
        String id = "connector-failed:dz-jdbc-sink-dz-src_t1";

        assertThatThrownBy(() -> service.ack(id)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void 존재하지_않는_id는_ack가_거부된다() {
        assertThatThrownBy(() -> service.ack("connector-paused:없는거"))
                .isInstanceOf(NoSuchElementException.class);
    }
}
