package io.deltazium.dwapply;

/**
 * 파일명 : FlatRow.java
 * 작성일자 : 26. 10. 19.
 * 작성자 : 최남희
 * 설명 : envelope을 평탄화한 한 행 — JSON Lines 한 줄(line)과 출처 Kafka 위치.
 * bytes는 배치 크기 판정용(UTF-8 길이 + 개행).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public record FlatRow(String topic, int partition, long offset, String line, int bytes) {
}
