package io.deltazium.backend.events;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/**
 * 파일명 : TableEventRepository.java
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : SQL은 resources/mappers/table-event.xml
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
public interface TableEventRepository {

    List<TableEvent> findRecent(@Param("limit") int limit);

    List<TableEvent> findByTable(@Param("schema") String schema, @Param("table") String table,
                                 @Param("limit") int limit);

    void insertEvent(@Param("schema") String schema, @Param("table") String table,
                     @Param("eventType") String eventType, @Param("severity") String severity,
                     @Param("message") String message, @Param("detail") String detail);
}
