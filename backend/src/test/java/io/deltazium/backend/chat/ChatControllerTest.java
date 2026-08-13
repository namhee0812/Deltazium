package io.deltazium.backend.chat;

import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * 파일명 : ChatControllerTest.java
 * 작성일자 : 26. 08. 12.
 * 작성자 : 최남희
 * 설명 : ChatController 단위 테스트. Claude API는 호출하지 않는다 — question이 비어 있으면
 * 400으로 즉시 거부하고 ChatService를 아예 건드리지 않는지만 검증한다.
 * 정상 경로(SSE 스트림 자체)는 ChatServiceTest에서 다룬다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 12.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
class ChatControllerTest {

    @Test
    void chat_blankQuestion_returns400_withoutCallingChatService() {
        ChatService chatService = mock(ChatService.class);
        ChatController controller = new ChatController(chatService);

        assertThatThrownBy(() -> controller.chat(new ChatRequest("   ")))
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining("400");

        verifyNoInteractions(chatService);
    }

    @Test
    void chat_nullQuestion_returns400() {
        ChatService chatService = mock(ChatService.class);
        ChatController controller = new ChatController(chatService);

        assertThatThrownBy(() -> controller.chat(new ChatRequest(null)))
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining("400");

        verifyNoInteractions(chatService);
    }

    @Test
    void chat_validQuestion_delegatesToChatServiceAndReturnsEmitter() {
        ChatService chatService = mock(ChatService.class);
        ChatController controller = new ChatController(chatService);

        SseEmitter emitter = controller.chat(new ChatRequest("왜 멈췄어?"));

        assertThat(emitter).isNotNull();
        verify(chatService).streamAnswer(org.mockito.ArgumentMatchers.eq("왜 멈췄어?"),
                org.mockito.ArgumentMatchers.any(SseEmitter.class));
    }
}
