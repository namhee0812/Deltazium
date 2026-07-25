package io.deltazium.backend.registration;

/** CDC 등록이 확정된 테이블 (사전 점검 통과 후 저장). */
public record RegisteredTable(
        Long id,
        String schemaName,
        String tableName,
        long sourceConnectionId,
        long targetConnectionId) {

    public String qualified() {
        return schemaName + "." + tableName;
    }
}
