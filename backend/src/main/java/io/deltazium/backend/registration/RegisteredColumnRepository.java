package io.deltazium.backend.registration;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/** SQL은 resources/mappers/registered-column.xml */
@Mapper
public interface RegisteredColumnRepository {

    List<ColumnMapping> findByTable(long registeredTableId);

    void deleteByTable(long registeredTableId);

    void insertOne(@Param("tableId") long registeredTableId, @Param("m") ColumnMapping mapping);

    default void insertAll(long registeredTableId, List<ColumnMapping> mappings) {
        for (ColumnMapping m : mappings) {
            insertOne(registeredTableId, m);
        }
    }
}
