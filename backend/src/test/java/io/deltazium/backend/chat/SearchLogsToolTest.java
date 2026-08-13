package io.deltazium.backend.chat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.deltazium.backend.assist.LogSearchResult;
import io.deltazium.backend.assist.LogSearchService;
import io.deltazium.backend.assist.LogSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 파일명 : SearchLogsToolTest.java
 * 작성일자 : 26. 08. 12.
 * 작성자 : 최남희
 * 설명 : SearchLogsTool 단위 테스트. Claude API는 호출하지 않는다 — 잘못된 source·날짜가
 * 예외 없이 {"error": ...} JSON으로 돌아오는지, 정상 입력은 LogSearchService 결과를
 * 그대로 JSON으로 내는지 검증한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 12.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
class SearchLogsToolTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @AfterEach
    void tearDown() {
        ToolBeans.overviewService = null;
        ToolBeans.logSearchService = null;
        ToolBeans.objectMapper = null;
    }

    @Test
    void get_validInput_returnsSearchResultAsJson() throws Exception {
        LogSearchService service = mock(LogSearchService.class);
        LogSearchResult result = new LogSearchResult(LogSource.BACKEND,
                java.util.List.of("backend.log"), 1, false,
                java.util.List.of(new LogSearchResult.LogLine("backend.log", 3L, "ERROR boom", true)));
        when(service.search(eq(LogSource.BACKEND), eq("boom"), any(), any(), anyInt(), anyInt()))
                .thenReturn(result);
        ToolBeans.logSearchService = service;
        ToolBeans.objectMapper = JSON;

        SearchLogsTool tool = new SearchLogsTool();
        tool.source = "BACKEND";
        tool.q = "boom";

        String json = tool.get();

        JsonNode node = JSON.readTree(json);
        assertThat(node.path("returnedLines").asInt()).isEqualTo(1);
        assertThat(node.path("lines").get(0).path("text").asText()).isEqualTo("ERROR boom");
    }

    @Test
    void get_omittedOptionalFields_appliesDefaults() throws Exception {
        LogSearchService service = mock(LogSearchService.class);
        when(service.search(eq(LogSource.KAFKA), eq(null), eq(null), eq(null),
                eq(LogSearchService.DEFAULT_LIMIT), eq(0)))
                .thenReturn(new LogSearchResult(LogSource.KAFKA, java.util.List.of(), 0, false, java.util.List.of()));
        ToolBeans.logSearchService = service;
        ToolBeans.objectMapper = JSON;

        SearchLogsTool tool = new SearchLogsTool();
        tool.source = "KAFKA";

        String json = tool.get();

        verify(service).search(LogSource.KAFKA, null, null, null, LogSearchService.DEFAULT_LIMIT, 0);
        assertThat(JSON.readTree(json).has("error")).isFalse();
    }

    @Test
    void get_invalidSource_returnsErrorJsonWithoutThrowing() throws Exception {
        ToolBeans.logSearchService = mock(LogSearchService.class);
        ToolBeans.objectMapper = JSON;

        SearchLogsTool tool = new SearchLogsTool();
        tool.source = "NOT_A_SOURCE";

        String json = tool.get();

        JsonNode node = JSON.readTree(json);
        assertThat(node.has("error")).isTrue();
    }

    @Test
    void get_invalidDate_returnsErrorJsonWithoutThrowing() throws Exception {
        ToolBeans.logSearchService = mock(LogSearchService.class);
        ToolBeans.objectMapper = JSON;

        SearchLogsTool tool = new SearchLogsTool();
        tool.source = "CONNECT";
        tool.from = "not-a-date";

        String json = tool.get();

        JsonNode node = JSON.readTree(json);
        assertThat(node.has("error")).isTrue();
        assertThat(node.path("error").asText()).contains("날짜 형식");
    }

    @Test
    void get_serviceThrowsIllegalArgument_returnsErrorJson() throws Exception {
        LogSearchService service = mock(LogSearchService.class);
        when(service.search(any(), any(), any(), any(), anyInt(), anyInt()))
                .thenThrow(new IllegalArgumentException("from이 to보다 뒤다"));
        ToolBeans.logSearchService = service;
        ToolBeans.objectMapper = JSON;

        SearchLogsTool tool = new SearchLogsTool();
        tool.source = "BACKEND";
        tool.from = "2026-08-12";
        tool.to = "2026-08-01";

        String json = tool.get();

        JsonNode node = JSON.readTree(json);
        assertThat(node.path("error").asText()).contains("from이 to보다 뒤다");
    }
}
