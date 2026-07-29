package io.deltazium.backend.ddl;

import java.util.List;
import java.util.Optional;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/** SQL은 resources/mappers/ddl-event.xml */
@Mapper
public interface DdlEventRepository {

    List<DdlEvent> findAll();

    Optional<DdlEvent> findById(long id);

    int countByOffset(long kafkaOffset);

    void insertEvent(@Param("kafkaOffset") long kafkaOffset,
                     @Param("tsMs") long tsMs,
                     @Param("scn") String scn,
                     @Param("schema") String schema,
                     @Param("table") String table,
                     @Param("ddl") String ddl,
                     @Param("state") String state);

    /** kafka_offset 기준 멱등 삽입 — 이미 있으면 false. */
    default boolean insertIfAbsent(long kafkaOffset, long tsMs, String scn,
                                   String schema, String table, String ddl, String state) {
        if (countByOffset(kafkaOffset) > 0) {
            return false;
        }
        insertEvent(kafkaOffset, tsMs, scn, schema, table, ddl, state);
        return true;
    }

    void decide(@Param("id") long id, @Param("state") String state, @Param("note") String note);
}
