package io.deltazium.backend.events;

import java.time.LocalDateTime;

/**
 * 파일명 : TableEvent.java
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : 테이블별 운영 이벤트 한 건 (테이블 드릴다운 타임라인의 원천).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public record TableEvent(
        Long id,
        LocalDateTime occurredAt,
        String schemaName,
        String tableName,
        String eventType,
        String severity,   // INFO | WARN | ERROR
        String message,
        String detail) {
}
