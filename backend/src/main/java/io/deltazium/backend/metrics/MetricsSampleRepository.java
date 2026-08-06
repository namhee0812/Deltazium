package io.deltazium.backend.metrics;

import java.time.LocalDateTime;
import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/**
 * 파일명 : MetricsSampleRepository.java
 * 작성일자 : 26. 08. 06.
 * 작성자 : 최남희
 * 설명 : 대시보드 시계열 샘플 저장소 (매퍼: mappers/metrics-sample.xml).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@Mapper
public interface MetricsSampleRepository {

    void insert(@Param("sampledAt") LocalDateTime sampledAt,
                @Param("metric") String metric,
                @Param("name") String name,
                @Param("value") long value,
                @Param("value2") Long value2);

    List<MetricsSample> findSince(@Param("from") LocalDateTime from);

    int deleteBefore(@Param("before") LocalDateTime before);
}
