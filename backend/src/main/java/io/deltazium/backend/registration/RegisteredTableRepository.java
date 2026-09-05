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
 */
@Mapper
public interface RegisteredTableRepository {

    List<RegisteredTable> findAll();

    int countBySchemaAndTable(@Param("schema") String schema, @Param("table") String table);

    default boolean exists(String schema, String table) {
        return countBySchemaAndTable(schema, table) > 0;
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
}
