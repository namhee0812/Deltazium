package io.deltazium.backend.registration;

import java.util.Locale;

/** CDC 등록이 확정된 테이블 (사전 점검 통과 후 저장). 타깃 이름이 null이면 소스와 동일. */
public record RegisteredTable(
        Long id,
        String schemaName,
        String tableName,
        long sourceConnectionId,
        long targetConnectionId,
        String targetSchemaName,
        String targetTableName) {

    public String qualified() {
        return schemaName + "." + tableName;
    }

    public String targetSchema() {
        return targetSchemaName == null || targetSchemaName.isBlank() ? schemaName : targetSchemaName;
    }

    public String targetTable() {
        return targetTableName == null || targetTableName.isBlank() ? tableName : targetTableName;
    }

    public String targetQualified() {
        return targetSchema() + "." + targetTable();
    }

    /** 테이블별 커넥터·컨슈머 그룹 이름에 쓰는 접미사 (예: cdc_auto_100). */
    public String suffix() {
        return (schemaName + "_" + tableName).toLowerCase(Locale.ROOT);
    }
}
