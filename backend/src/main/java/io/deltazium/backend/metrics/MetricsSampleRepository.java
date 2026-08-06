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
 * 시계열 조회는 SQL에서 집계(GROUP BY)한다 — 수백 테이블 × 분 단위 원본을
 * 앱으로 끌어오지 않기 위함. 롤업(MIN→HOUR→DAY)은 NOT EXISTS로 멱등.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | resolution·SQL 집계 조회·롤업·해상도별 보존으로 개편
 * --------------------------------------------------
 */
@Mapper
public interface MetricsSampleRepository {

    void insert(@Param("sampledAt") LocalDateTime sampledAt,
                @Param("metric") String metric,
                @Param("name") String name,
                @Param("value") long value,
                @Param("value2") Long value2,
                @Param("resolution") String resolution);

    /** 시점·지표별 집계 시계열. name이 null이면 전 테이블 SUM, 지정하면 해당 토픽만. */
    List<SeriesRow> series(@Param("resolution") String resolution,
                           @Param("from") LocalDateTime from,
                           @Param("metrics") List<String> metrics,
                           @Param("name") String name);

    /** 컴포넌트 자원 최신값 조회용 — RESOURCE(MIN)만. */
    List<MetricsSample> recentResources(@Param("from") LocalDateTime from);

    /** MIN → HOUR 롤업 (완결된 시간만, 이미 롤업된 조합은 NOT EXISTS로 제외). */
    int rollupHourSum(@Param("cutoff") LocalDateTime cutoff);

    int rollupHourMax(@Param("cutoff") LocalDateTime cutoff);

    int rollupHourAvg(@Param("cutoff") LocalDateTime cutoff);

    /** HOUR → DAY 롤업. */
    int rollupDaySum(@Param("cutoff") LocalDateTime cutoff);

    int rollupDayMax(@Param("cutoff") LocalDateTime cutoff);

    int rollupDayAvg(@Param("cutoff") LocalDateTime cutoff);

    int deleteResolutionBefore(@Param("resolution") String resolution,
                               @Param("before") LocalDateTime before);
}
