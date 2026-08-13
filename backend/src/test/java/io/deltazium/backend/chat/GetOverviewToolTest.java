package io.deltazium.backend.chat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.deltazium.backend.assist.OverviewResult;
import io.deltazium.backend.assist.OverviewService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 파일명 : GetOverviewToolTest.java
 * 작성일자 : 26. 08. 12.
 * 작성자 : 최남희
 * 설명 : GetOverviewTool 단위 테스트. Claude API는 호출하지 않는다 — ToolBeans에 mock
 * OverviewService를 꽂아 get()이 유효 JSON을 내는지, 서비스가 예외를 던지면
 * {"error": ...} JSON으로 안전하게 변환하는지만 검증한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 12.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
class GetOverviewToolTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @AfterEach
    void tearDown() {
        ToolBeans.overviewService = null;
        ToolBeans.logSearchService = null;
        ToolBeans.objectMapper = null;
    }

    @Test
    void get_returnsOverviewServiceResultAsValidJson() throws Exception {
        OverviewService overviewService = mock(OverviewService.class);
        OverviewResult result = new OverviewResult(
                new OverviewResult.ConnectorsSection(1, 1, 0, java.util.List.of(),
                        0, java.util.List.of(), 0, java.util.List.of(), 0, java.util.List.of()),
                new OverviewResult.TablesSection(1, 0, java.util.List.of()),
                new OverviewResult.RecentErrorsSection(0, java.util.List.of()),
                new OverviewResult.DdlSection(0, java.util.List.of()),
                new OverviewResult.Sources("OK", "OK", "OK"));
        when(overviewService.overview()).thenReturn(result);

        ToolBeans.overviewService = overviewService;
        ToolBeans.objectMapper = JSON;

        String json = new GetOverviewTool().get();

        JsonNode node = JSON.readTree(json);
        assertThat(node.path("sources").path("connect").asText()).isEqualTo("OK");
        assertThat(node.path("connectors").path("total").asInt()).isEqualTo(1);
    }

    @Test
    void get_serviceThrows_returnsErrorJsonWithoutThrowing() throws Exception {
        OverviewService overviewService = mock(OverviewService.class);
        when(overviewService.overview()).thenThrow(new RuntimeException("Connect 도달 불가"));

        ToolBeans.overviewService = overviewService;
        ToolBeans.objectMapper = JSON;

        String json = new GetOverviewTool().get();

        JsonNode node = JSON.readTree(json);
        assertThat(node.has("error")).isTrue();
        assertThat(node.path("error").asText()).contains("Connect 도달 불가");
    }

    @Test
    void get_serviceNotInitialized_returnsErrorJson() throws Exception {
        String json = new GetOverviewTool().get();

        JsonNode node = JSON.readTree(json);
        assertThat(node.has("error")).isTrue();
    }
}
