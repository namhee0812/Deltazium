package io.deltazium.backend.events;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.deltazium.backend.connect.ConnectClient;
import io.deltazium.backend.registration.RegisteredTableRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 파일명 : ConnectorHealthWatcherTest.java
 * 작성일자 : 26. 08. 04.
 * 작성자 : 최남희
 * 설명 : 커넥터 상태 감시 단위 테스트 — task FAILED 집계와
 * "관측 시작 시점에 이미 FAILED" 케이스(backend 기동 전 장애) 기록 검증.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 04.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
class ConnectorHealthWatcherTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private ConnectClient connect;
    private TableEventService events;
    private ConnectorHealthWatcher watcher;

    @BeforeEach
    void setUp() {
        connect = mock(ConnectClient.class);
        events = mock(TableEventService.class);
        RegisteredTableRepository registrations = mock(RegisteredTableRepository.class);
        when(registrations.findAll()).thenReturn(List.of());
        watcher = new ConnectorHealthWatcher(connect, registrations, events);
    }

    private void connectReturns(String json) throws Exception {
        when(connect.listConnectors()).thenReturn(JSON.readTree(json));
    }

    private static String status(String connectorState, String taskState) {
        return """
                {"dz-source":{"status":{"connector":{"state":"%s"},
                 "tasks":[{"id":0,"state":"%s","trace":"boom"}]}}}"""
                .formatted(connectorState, taskState);
    }

    @Test
    void 커넥터는_RUNNING이어도_task가_FAILED면_장애로_기록한다() throws Exception {
        connectReturns(status("RUNNING", "RUNNING"));
        watcher.poll(); // 기준선: 정상
        connectReturns(status("RUNNING", "FAILED"));
        watcher.poll();
        verify(events).record(anyString(), eq("dz-source"), eq("CONNECTOR_FAILED"),
                eq("ERROR"), anyString(), eq("boom"));
    }

    @Test
    void 관측_시작_시점에_이미_FAILED면_전이가_아니어도_기록한다() throws Exception {
        connectReturns(status("RUNNING", "FAILED"));
        watcher.poll(); // 첫 관측 = backend 기동 직후
        verify(events).record(anyString(), eq("dz-source"), eq("CONNECTOR_FAILED"),
                eq("ERROR"), anyString(), eq("boom"));
    }

    @Test
    void 첫_관측이_정상이면_아무것도_기록하지_않는다() throws Exception {
        connectReturns(status("RUNNING", "RUNNING"));
        watcher.poll();
        verify(events, never()).record(anyString(), anyString(), anyString(),
                anyString(), anyString(), any());
    }

    @Test
    void FAILED_지속_중에는_중복_기록하지_않는다() throws Exception {
        connectReturns(status("RUNNING", "FAILED"));
        watcher.poll();
        watcher.poll();
        verify(events).record(anyString(), anyString(), anyString(),
                anyString(), anyString(), any());
    }
}
