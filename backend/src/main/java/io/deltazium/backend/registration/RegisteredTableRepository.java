package io.deltazium.backend.registration;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/** SQL은 resources/mappers/registered-table.xml */
@Mapper
public interface RegisteredTableRepository {

    List<RegisteredTable> findAll();

    int countBySchemaAndTable(@Param("schema") String schema, @Param("table") String table);

    default boolean exists(String schema, String table) {
        return countBySchemaAndTable(schema, table) > 0;
    }

    /** 다른 스키마에 같은 이름의 테이블이 있는지 — iceberg route-field(source.table) 충돌 검사용 */
    int countTableNameInOtherSchema(@Param("schema") String schema, @Param("table") String table);

    default boolean existsTableNameInOtherSchema(String schema, String table) {
        return countTableNameInOtherSchema(schema, table) > 0;
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
