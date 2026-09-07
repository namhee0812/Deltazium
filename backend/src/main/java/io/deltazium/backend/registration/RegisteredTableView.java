package io.deltazium.backend.registration;

/**
 * 파일명 : RegisteredTableView.java
 * 작성일자 : 26. 09. 07.
 * 작성자 : 최남희
 * 설명 : GET /api/registrations 응답 전용 뷰 — RegisteredTable(도메인)에 소스 topicPrefix를
 * 얹는다. UI가 커넥터·토픽 이름(dz-jdbc-sink-&lt;prefix&gt;-&lt;suffix&gt; 등)을 backend와 같은
 * 규칙(ConnectorNames)으로 조립하려면 소스 식별자가 필요하다 (다중 소스·다중 타깃 ②).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public record RegisteredTableView(
        Long id,
        String schemaName,
        String tableName,
        long sourceConnectionId,
        long targetConnectionId,
        String targetSchemaName,
        String targetTableName,
        String snapshotMode,
        String sourceTopicPrefix) {

    public static RegisteredTableView of(RegisteredTable t, String sourceTopicPrefix) {
        return new RegisteredTableView(t.id(), t.schemaName(), t.tableName(), t.sourceConnectionId(),
                t.targetConnectionId(), t.targetSchemaName(), t.targetTableName(), t.snapshotMode(),
                sourceTopicPrefix);
    }
}
