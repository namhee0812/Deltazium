package io.deltazium.backend.ddl;

import java.time.LocalDateTime;

/**
 * 파일명 : DdlEvent.java
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : schema change topic에서 소비해 저장한 DDL 이벤트 (7절 승인 워크플로).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public record DdlEvent(
        Long id,
        long kafkaOffset,
        long eventTsMs,
        String scn,
        String schemaName,
        String tableName,
        String ddlText,
        String state,   // SNAPSHOT(스냅샷 시 구조 덤프·정보성) | DETECTED(승인 대기) | APPROVED | REJECTED
        String note,
        LocalDateTime decidedAt) {
}
