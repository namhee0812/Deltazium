package io.deltazium.backend.registration;

import java.util.Locale;

/**
 * 파일명 : RegisteredTable.java
 * 작성일자 : 26. 07. 25.
 * 작성자 : 최남희
 * 설명 : CDC 등록이 확정된 테이블 (사전 점검 통과 후 저장). 타깃 이름이 null이면 소스와 동일.
 * 식별 키는 (source_connection_id, schema_name, table_name) — 다중 소스·다중 타깃 ②
 * (2026-09-07): 소스가 여러 개면 스키마.테이블 이름이 같아도 다른 소스면 별개 테이블이다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 25.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | schemaFingerprint 필드 추가(스키마 지문 비교 감지, architecture.md
 * |                          | 7절 개정) — 기존 생성자는 유지해 null로 위임(하위 호환)
 * --------------------------------------------------
 */
public record RegisteredTable(
        Long id,
        String schemaName,
        String tableName,
        long sourceConnectionId,
        long targetConnectionId,
        String targetSchemaName,
        String targetTableName,
        String snapshotMode,
        String schemaFingerprint) {

    public RegisteredTable(Long id, String schemaName, String tableName,
                           long sourceConnectionId, long targetConnectionId,
                           String targetSchemaName, String targetTableName) {
        this(id, schemaName, tableName, sourceConnectionId, targetConnectionId,
                targetSchemaName, targetTableName, "INITIAL", null);
    }

    public RegisteredTable(Long id, String schemaName, String tableName,
                           long sourceConnectionId, long targetConnectionId,
                           String targetSchemaName, String targetTableName, String snapshotMode) {
        this(id, schemaName, tableName, sourceConnectionId, targetConnectionId,
                targetSchemaName, targetTableName, snapshotMode, null);
    }

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

    /** 테이블별 커넥터·컨슈머 그룹 이름에 쓰는 접미사 (예: cdc_auto_100). 소스 식별자는 별도
     * (ConnectorNames가 접두로 붙인다) — 같은 접미사가 다른 소스에도 있을 수 있다. */
    public String suffix() {
        return (schemaName + "_" + tableName).toLowerCase(Locale.ROOT);
    }
}
