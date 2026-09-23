package io.deltazium.backend.metrics;

/**
 * 파일명 : WarningObservation.java
 * 작성일자 : 26. 09. 23.
 * 작성자 : 최남희
 * 설명 : system_warning_acks 한 행 — 경고 id의 최초 관측 시각(sinceMs)과 확인(ack) 여부.
 * INFO 등급 경고(PAUSED 등)만 대상. SystemWarningService가 재기동 후에도 sinceMs를
 * 유지하기 위해(ack 유효성 보존) 이 테이블에 최초 관측 시각을 저장한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 23.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public record WarningObservation(String id, long sinceMs, boolean acked) {
}
