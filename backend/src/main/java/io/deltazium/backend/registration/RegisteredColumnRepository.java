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
 * 26. 09. 27.       | 최남희  | updateEnabled 추가 — DDL 건너뛰기(SKIPPED, architecture.md 7절)의
 * |                          | DROP COLUMN 처리에서 기존 매핑을 비활성화할 때 쓴다
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

    /** 대소문자 무관 컬럼명 매칭(소스 DB에 따라 대문자·소문자가 섞일 수 있다). */
    void updateEnabled(@Param("tableId") long registeredTableId, @Param("targetColumn") String targetColumn,
                       @Param("enabled") boolean enabled);
}
