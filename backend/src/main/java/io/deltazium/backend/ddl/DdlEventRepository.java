package io.deltazium.backend.ddl;

import java.util.List;
import java.util.Optional;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/**
 * 파일명 : DdlEventRepository.java
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : SQL은 resources/mappers/ddl-event.xml
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ②: insertFingerprintEvent 추가 —
 * |                          | schema change topic이 없는 소스의 스키마 지문 diff 기록용
 * |                          | (Kafka offset이 없어 insertIfAbsent 경로를 쓰지 않는다)
 * --------------------------------------------------
 */
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

    /** 스키마 지문 비교(FINGERPRINT)로 감지한 DDL 초안 — 호출측(SchemaFingerprintService)이
     * 지문이 실제로 바뀐 시점에만 부르므로 멱등 삽입이 필요 없다. */
    class InsertFingerprintRow {
        public Long id;
        public long tsMs;
        public String schema;
        public String table;
        public String ddl;
        public String state;
    }

    void insertFingerprintEventRow(InsertFingerprintRow row);

    default long insertFingerprintEvent(long tsMs, String schema, String table, String ddl, String state) {
        InsertFingerprintRow row = new InsertFingerprintRow();
        row.tsMs = tsMs;
        row.schema = schema;
        row.table = table;
        row.ddl = ddl;
        row.state = state;
        insertFingerprintEventRow(row);
        return row.id;
    }

    void decide(@Param("id") long id, @Param("state") String state, @Param("note") String note);
}
