package io.deltazium.backend.capture;

import java.util.List;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.deltazium.backend.connect.ConnectClient;
import io.deltazium.backend.connect.ConnectorDeployService;
import io.deltazium.backend.events.TableEventService;
import io.deltazium.backend.metrics.KafkaMetricsService;
import io.deltazium.backend.registration.RegisteredTable;
import io.deltazium.backend.registration.RegistrationService;
import io.deltazium.backend.registry.DbConnection;
import io.deltazium.backend.registry.DbConnectionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 파일명 : ResnapshotOrchestratorTest.java
 * 작성일자 : 26. 08. 05.
 * 작성자 : 최남희
 * 설명 : 재스냅샷 상태 기계 단위 테스트 — 단계 순서, 권한 불충분 홀드,
 * count=0 통과, NO_DATA+truncate 조합 거부.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 05.       | 최남희  | 최초 생성 (RegistrationServiceTest의 resnapshot 테스트 이관)
 * --------------------------------------------------
 */
class ResnapshotOrchestratorTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private RegistrationService registrations;
    private DbConnectionService connections;
    private ConnectorDeployService deploy;
    private ConnectClient connect;
    private KafkaMetricsService metrics;
    private TargetTableGate gate;
    private SnapshotNotificationPoller notifications;
    private ResnapshotOrchestrator orchestrator;

    private final RegisteredTable table = new RegisteredTable(
            1L, "CDC", "T1", 10L, 20L, "TGT", "T1", "INITIAL");
    private final DbConnection target = new DbConnection(
            20L, "tgt", "ORACLE", "TARGET", "host", 1521, "PDB", "apply", "pw");

    @BeforeEach
    void setUp() throws Exception {
        registrations = mock(RegistrationService.class);
        connections = mock(DbConnectionService.class);
        deploy = mock(ConnectorDeployService.class);
        connect = mock(ConnectClient.class);
        metrics = mock(KafkaMetricsService.class);
        gate = mock(TargetTableGate.class);
        notifications = mock(SnapshotNotificationPoller.class);
        TableEventService events = mock(TableEventService.class);

        when(registrations.list()).thenReturn(List.of(table));
        when(connections.get(20L)).thenReturn(target);
        when(metrics.groupLag(anyString(), anyString())).thenReturn(0L);
        when(notifications.status()).thenReturn(
                new SnapshotNotificationPoller.SnapshotStatus("COMPLETED", null,
                        java.util.Map.of("ORCL.CDC.T1", 5L), 1L, 2L));
        when(connect.status(anyString())).thenReturn(JSON.readTree("""
                {"status":{"connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"RUNNING"}]}}"""));

        orchestrator = new ResnapshotOrchestrator(registrations, connections, deploy,
                connect, metrics, gate, notifications, events, "dz");
    }

    private void waitForPhase(ResnapshotOrchestrator.Phase expected) {
        waitFor(() -> orchestrator.status() != null && orchestrator.status().phase() == expected,
                "phase " + expected + " (실제: "
                        + (orchestrator.status() == null ? "null" : orchestrator.status().phase()) + ")");
    }

    private void waitFor(Supplier<Boolean> cond, String what) {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            if (cond.get()) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        throw new AssertionError("시간 내 도달 실패: " + what);
    }

    @Test
    void truncate_없는_재스냅샷은_정지_리셋_재배포_재개_순서로_완주한다() {
        orchestrator.start("INITIAL", false);
        waitForPhase(ResnapshotOrchestrator.Phase.DONE);

        var order = inOrder(deploy, registrations);
        order.verify(deploy).stopAndAwait("dz-source");
        order.verify(deploy).deleteOffsets("dz-source");
        order.verify(registrations).redeployWithSnapshotMode("initial");
        order.verify(deploy).resumeConnector("dz-source");
    }

    @Test
    void SYSTEM_선택시_권한_없으면_HELD로_홀딩되고_비워지면_진행한다() {
        when(gate.canTruncate(any(), anyList())).thenReturn(false);
        when(gate.rowCount(any(), anyString())).thenReturn(100L);

        orchestrator.start("INITIAL", true);
        waitForPhase(ResnapshotOrchestrator.Phase.AWAITING_DECISION);
        orchestrator.decide("SYSTEM");
        waitForPhase(ResnapshotOrchestrator.Phase.HELD);
        assertThat(orchestrator.status().holdReason()).contains("권한 불충분");
        assertThat(orchestrator.status().truncateSql())
                .containsExactly("TRUNCATE TABLE TGT.T1;");

        // DBA가 비웠다 → count=0 → 자동 진행
        when(gate.rowCount(any(), anyString())).thenReturn(0L);
        orchestrator.recheck();
        waitForPhase(ResnapshotOrchestrator.Phase.DONE);
    }

    @Test
    void SYSTEM_선택시_권한_있으면_truncate를_실행하고_완주한다() {
        when(gate.canTruncate(any(), anyList())).thenReturn(true);

        orchestrator.start("INITIAL", true);
        waitForPhase(ResnapshotOrchestrator.Phase.AWAITING_DECISION);
        orchestrator.decide("SYSTEM");
        waitForPhase(ResnapshotOrchestrator.Phase.DONE);

        var order = inOrder(deploy, gate);
        order.verify(deploy).stopAndAwait("dz-source");
        order.verify(gate).truncate(target, "TGT.T1");
        order.verify(deploy).deleteOffsets("dz-source");
    }

    @Test
    void MANUAL_선택은_바로_HELD로_가고_취소하면_원복된다() {
        when(gate.rowCount(any(), anyString())).thenReturn(100L);

        orchestrator.start("INITIAL", true);
        waitForPhase(ResnapshotOrchestrator.Phase.AWAITING_DECISION);
        orchestrator.decide("MANUAL");
        waitForPhase(ResnapshotOrchestrator.Phase.HELD);

        orchestrator.cancel();
        waitForPhase(ResnapshotOrchestrator.Phase.CANCELLED);
        // 원복 = source resume (offset은 안 건드림)
        org.mockito.Mockito.verify(deploy).resumeConnector("dz-source");
        org.mockito.Mockito.verify(deploy, org.mockito.Mockito.never()).deleteOffsets(anyString());
    }

    @Test
    void NO_DATA와_truncate_조합은_시작_전에_거부한다() {
        assertThatThrownBy(() -> orchestrator.start("NO_DATA", true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("INITIAL");
    }
}
