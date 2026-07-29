package io.deltazium.backend.events;

import java.time.LocalDateTime;

/** 테이블별 운영 이벤트 한 건 (테이블 드릴다운 타임라인의 원천). */
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
