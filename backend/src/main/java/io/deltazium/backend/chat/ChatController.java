package io.deltazium.backend.chat;

import java.time.Duration;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * 파일명 : ChatController.java
 * 작성일자 : 26. 08. 12.
 * 작성자 : 최남희
 * 설명 : AI 진단 어시스턴트 대화 API. POST /api/chat — SSE(text/event-stream)로 진행 상황과
 * 답변을 흘린다. 이 POST는 상태를 바꾸지 않는다(DB·파일 쓰기 없음) — Claude 호출과
 * 읽기 전용 도구 실행뿐이다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 12.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@RestController
@RequestMapping("/api/chat")
public class ChatController {

    private static final long EMITTER_TIMEOUT_MS = Duration.ofMinutes(5).toMillis();

    private final ChatService chatService;

    public ChatController(ChatService chatService) {
        this.chatService = chatService;
    }

    @PostMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter chat(@RequestBody ChatRequest request) {
        if (request == null || request.question() == null || request.question().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "question이 비어 있다");
        }
        SseEmitter emitter = new SseEmitter(EMITTER_TIMEOUT_MS);
        chatService.streamAnswer(request.question(), emitter);
        return emitter;
    }
}
