package io.deltazium.backend.metrics;

import java.time.LocalDateTime;

/**
 * 파일명 : SeriesRow.java
 * 작성일자 : 26. 08. 06.
 * 작성자 : 최남희
 * 설명 : 대시보드 시계열 조회 행 — 시점·지표별 집계값 (SQL GROUP BY 결과).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public record SeriesRow(LocalDateTime ts, String metric, long agg) {
}
