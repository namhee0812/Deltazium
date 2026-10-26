package io.deltazium.backend.chat;

import java.time.OffsetDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 파일명 : ChatRoomCleaner.java
 * 작성일자 : 26. 10. 26.
 * 작성자 : 최남희
 * 설명 : 대화방 보관 주기 정리. 기동 직후 1회 + 1시간마다, 마지막 활동(last_active_at)이
 * chat_settings.retention_days보다 오래된 방을 삭제한다(메시지는 FK cascade).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 26.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@Component
public class ChatRoomCleaner {

    private static final Logger log = LoggerFactory.getLogger(ChatRoomCleaner.class);

    private final ChatRoomRepository repository;

    public ChatRoomCleaner(ChatRoomRepository repository) {
        this.repository = repository;
    }

    /** 삭제 기준 시각 — 이 시각보다 strictly 이전인 방이 대상(정확히 경계면 유지). */
    static OffsetDateTime cutoff(OffsetDateTime now, int retentionDays) {
        return now.minusDays(retentionDays);
    }

    /** initialDelay 기본값 0 — 기동 시 1회 실행을 겸한다. */
    @Scheduled(fixedDelayString = "PT1H")
    public void clean() {
        clean(OffsetDateTime.now());
    }

    int clean(OffsetDateTime now) {
        try {
            int deleted = repository.deleteInactiveBefore(cutoff(now, repository.retentionDays()));
            if (deleted > 0) {
                log.info("보관 주기 경과 대화방 {}개 삭제", deleted);
            }
            return deleted;
        } catch (Exception e) {
            log.warn("대화방 정리 실패 — {}", e.getMessage());
            return 0;
        }
    }
}
