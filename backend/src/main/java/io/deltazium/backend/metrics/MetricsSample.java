package io.deltazium.backend.metrics;

import java.time.LocalDateTime;

/**
 * 파일명 : MetricsSample.java
 * 작성일자 : 26. 08. 06.
 * 작성자 : 최남희
 * 설명 : 대시보드 시계열 샘플 한 건 (1분 주기).
 * metric: PUBLISH·APPLY_JDBC·APPLY_ICEBERG(분당 이벤트, name=topic) /
 * LAG_JDBC·LAG_ICEBERG(시점 lag) / RESOURCE(name=컴포넌트, value=RSS kB, value2=CPU%×10).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public record MetricsSample(
        Long id,
        LocalDateTime sampledAt,
        String metric,
        String name,
        long value,
        Long value2) {
}
