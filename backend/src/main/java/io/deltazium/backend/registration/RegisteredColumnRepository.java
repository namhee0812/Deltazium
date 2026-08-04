package io.deltazium.backend.registration;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/**
 * 파일명 : RegisteredColumnRepository.java
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : SQL은 resources/mappers/registered-column.xml
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
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
