package io.deltazium.backend.ddl;

import java.time.LocalDateTime;

/**
 * 파일명 : DdlEvent.java
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : DDL 승인 워크플로에 오른 이벤트 (7절). 감지 입구가 소스 타입별로 갈린다(2026-09-07
 * 개정) — origin으로 구분한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ②: origin 추가 — SCHEMA_TOPIC(schema change
 * |                          | topic 발행 소스, 종전 방식) | FINGERPRINT(스키마 지문 비교로 감지,
 * |                          | PostgreSQL 등 schema change topic 미발행 소스). scn은 SCHEMA_TOPIC
 * |                          | 전용 참고 문자열이라 FINGERPRINT 행은 항상 null(위치 무관, 7절)
 * --------------------------------------------------
 */
public record DdlEvent(
        Long id,
        Long kafkaOffset,   // FINGERPRINT 기원은 Kafka offset이 없어 null
        long eventTsMs,
        String scn,         // 소스 위치 참고용 nullable 문자열 — Oracle SCN 등, FINGERPRINT는 항상 null
        String schemaName,
        String tableName,
        String ddlText,
        String state,   // SNAPSHOT(스냅샷 시 구조 덤프·정보성) | DETECTED(승인 대기) | APPROVED | REJECTED
        String note,
        LocalDateTime decidedAt,
        String origin) { // SCHEMA_TOPIC | FINGERPRINT

    public DdlEvent(Long id, long kafkaOffset, long eventTsMs, String scn, String schemaName,
                    String tableName, String ddlText, String state, String note, LocalDateTime decidedAt) {
        this(id, kafkaOffset, eventTsMs, scn, schemaName, tableName, ddlText, state, note, decidedAt,
                "SCHEMA_TOPIC");
    }
}
