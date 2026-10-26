package io.deltazium.backend.chat;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * 파일명 : ChatRoomController.java
 * 작성일자 : 26. 10. 26.
 * 작성자 : 최남희
 * 설명 : AI 진단 어시스턴트 대화방 CRUD·보관 주기 설정 API. 인증이 없어 방은 전역 공유다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 26.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@RestController
@RequestMapping("/api/chat")
public class ChatRoomController {

    /** 보관 주기 허용값(일). */
    static final Set<Integer> ALLOWED_RETENTION_DAYS = Set.of(1, 3, 7, 30);

    private final ChatRoomRepository repository;

    public ChatRoomController(ChatRoomRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/rooms")
    public List<ChatRoom> rooms() {
        return repository.findRooms();
    }

    @PostMapping("/rooms")
    public Map<String, Long> createRoom() {
        return Map.of("id", repository.createRoom(null));
    }

    @DeleteMapping("/rooms/{id}")
    public void deleteRoom(@PathVariable long id) {
        repository.deleteRoom(id);
    }

    @GetMapping("/rooms/{id}/messages")
    public List<ChatMessage> messages(@PathVariable long id) {
        return repository.findMessages(id);
    }

    @GetMapping("/settings")
    public Map<String, Integer> settings() {
        return Map.of("retentionDays", repository.retentionDays());
    }

    @PutMapping("/settings")
    public Map<String, Integer> updateSettings(@RequestBody Map<String, Integer> body) {
        Integer days = body == null ? null : body.get("retentionDays");
        if (days == null || !ALLOWED_RETENTION_DAYS.contains(days)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "retentionDays는 1·3·7·30 중 하나여야 한다");
        }
        repository.updateRetentionDays(days);
        return Map.of("retentionDays", days);
    }
}
