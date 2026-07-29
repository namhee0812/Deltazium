package io.deltazium.backend.events;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/** SQL은 resources/mappers/table-event.xml */
@Mapper
public interface TableEventRepository {

    List<TableEvent> findRecent(@Param("limit") int limit);

    List<TableEvent> findByTable(@Param("schema") String schema, @Param("table") String table,
                                 @Param("limit") int limit);

    void insertEvent(@Param("schema") String schema, @Param("table") String table,
                     @Param("eventType") String eventType, @Param("severity") String severity,
                     @Param("message") String message, @Param("detail") String detail);
}
