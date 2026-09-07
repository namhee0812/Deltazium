package io.deltazium.backend.registration;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/**
 * 파일명 : RegisteredTableRepository.java
 * 작성일자 : 26. 07. 25.
 * 작성자 : 최남희
 * 설명 : SQL은 resources/mappers/registered-table.xml
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 25.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 09. 05.       | 최남희  | countTableNameInOtherSchema 제거 — route-field가 토픽 이름
 * |                          | 기준(_pos.topic)으로 바뀌어 동명 테이블 제약이 해소됨(5.1절)
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ②: 중복 판정을 (source_connection_id, schema,
 * |                          | table) 기준으로 전환 — 소스가 다르면 동일 schema.table도 별개.
 * |                          | findBySource·updateFingerprint 추가(스키마 지문 감지용)
 * --------------------------------------------------
 */
@Mapper
public interface RegisteredTableRepository {

    List<RegisteredTable> findAll();

    /** 스키마 지문 감지(SchemaFingerprintService) 대상 조회 — 특정 소스 커넥션에 속한 등록 테이블. */
    List<RegisteredTable> findBySource(@Param("sourceConnectionId") long sourceConnectionId);

    int countBySourceSchemaAndTable(@Param("sourceConnectionId") long sourceConnectionId,
                                    @Param("schema") String schema, @Param("table") String table);

    default boolean exists(long sourceConnectionId, String schema, String table) {
        return countBySourceSchemaAndTable(sourceConnectionId, schema, table) > 0;
    }

    class InsertRow {
        public Long id;
        public String schemaName;
        public String tableName;
        public long sourceConnectionId;
        public long targetConnectionId;
        public String targetSchemaName;
        public String targetTableName;
        public String snapshotMode;
    }

    void insertRow(InsertRow row);

    default long insert(String schema, String table, long sourceConnId, long targetConnId,
                        String targetSchema, String targetTable) {
        return insert(schema, table, sourceConnId, targetConnId, targetSchema, targetTable, "INITIAL");
    }

    default long insert(String schema, String table, long sourceConnId, long targetConnId,
                        String targetSchema, String targetTable, String snapshotMode) {
        InsertRow row = new InsertRow();
        row.schemaName = schema;
        row.tableName = table;
        row.sourceConnectionId = sourceConnId;
        row.targetConnectionId = targetConnId;
        row.targetSchemaName = targetSchema;
        row.targetTableName = targetTable;
        row.snapshotMode = snapshotMode;
        insertRow(row);
        return row.id;
    }

    int deleteRow(long id);

    default boolean delete(long id) {
        return deleteRow(id) == 1;
    }

    void updateFingerprint(@Param("id") long id, @Param("fingerprint") String fingerprint);
}
