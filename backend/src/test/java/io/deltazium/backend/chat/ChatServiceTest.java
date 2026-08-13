package io.deltazium.backend.chat;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import io.deltazium.backend.assist.LogSearchService;
import io.deltazium.backend.assist.OverviewService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * 파일명 : ChatServiceTest.java
 * 작성일자 : 26. 08. 12.
 * 작성자 : 최남희
 * 설명 : ChatService 단위 테스트. Claude API는 실제로 호출하지 않는다 —
 * ANTHROPIC_API_KEY가 없는 환경(빌드·CI 기본값)에서 SSE error 이벤트가 나가고
 * done으로 마무리되는지만 검증한다. 키가 설정된 환경에서는 이 테스트를 건너뛴다
 * (실제 Claude 호출 여부를 이 테스트가 좌우하면 안 된다).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 12.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
class ChatServiceTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @AfterEach
    void tearDown() {
        // ChatService 생성자가 ToolBeans 정적 필드를 채운다 — 다른 테스트 클래스로 새지 않게 정리.
        ToolBeans.overviewService = null;
        ToolBeans.logSearchService = null;
        ToolBeans.objectMapper = null;
    }

    @Test
    void streamAnswer_apiKeyMissing_sendsErrorEventThenDone() throws Exception {
        String key = System.getenv("ANTHROPIC_API_KEY");
        assumeTrue(key == null || key.isBlank(),
                "ANTHROPIC_API_KEY가 설정된 환경에서는 실제 호출을 피하기 위해 이 테스트를 건너뛴다");

        ChatService service = new ChatService(mock(OverviewService.class), mock(LogSearchService.class), JSON);

        SseEmitter emitter = spy(new SseEmitter());

        service.streamAnswer("왜 CDC가 멈췄어?", emitter);

        // emitter는 실제 HTTP 요청 컨텍스트 밖이라 onCompletion 콜백이 안 걸린다 —
        // complete() 호출 자체를 폴링으로 기다린다(전용 executor 스레드에서 비동기 처리).
        verify(emitter, timeout(10_000)).complete();

        var captor = forClass(SseEmitter.SseEventBuilder.class);
        verify(emitter, atLeastOnce()).send(captor.capture());

        List<JsonNode> events = captor.getAllValues().stream()
                .map(ChatServiceTest::extractJson)
                .filter(java.util.Objects::nonNull)
                .toList();

        assertThat(events).anySatisfy(event -> {
            assertThat(event.path("type").asText()).isEqualTo("error");
            assertThat(event.path("message").asText()).contains("ANTHROPIC_API_KEY");
        });
        assertThat(events).anySatisfy(event -> assertThat(event.path("type").asText()).isEqualTo("done"));
    }

    /**
     * build()는 SSE 와이어 포맷대로 "data: " 필드명·실제 페이로드·구분용 개행을 각각 별도
     * DataWithMediaType으로 쪼갠다 — 그중 JSON 객체로 시작하는 조각만 실제 페이로드다.
     */
    private static JsonNode extractJson(SseEmitter.SseEventBuilder builder) {
        return builder.build().stream()
                .map(ResponseBodyEmitter.DataWithMediaType::getData)
                .filter(String.class::isInstance)
                .map(String.class::cast)
                .map(String::trim)
                .filter(s -> s.startsWith("{"))
                .findFirst()
                .map(raw -> {
                    try {
                        return JSON.readTree(raw);
                    } catch (Exception e) {
                        return null;
                    }
                })
                .orElse(null);
    }
}
