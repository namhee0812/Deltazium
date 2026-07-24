package io.deltazium.recovery.envelope;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Iceberg changelog 테이블 한 행 (architecture.md 5.1절 고정 스키마).
 * 이 필드들만으로 Debezium envelope을 재조립할 수 있어야 한다는 불변식을 진다.
 *
 * @param op          envelope op (c/u/d/r)
 * @param before      envelope before (nullable)
 * @param after       envelope after (nullable)
 * @param scn         source.scn (Oracle SCN)
 * @param txId        source.txId
 * @param tsMs        source.ts_ms (소스 커밋 시각, epoch millis)
 * @param sourceTable source.schema + "." + source.table
 */
public record ChangelogRecord(
        String op,
        JsonNode before,
        JsonNode after,
        long scn,
        String txId,
        long tsMs,
        String sourceTable) {
}
