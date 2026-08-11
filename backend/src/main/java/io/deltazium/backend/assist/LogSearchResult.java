package io.deltazium.backend.assist;

import java.util.List;

/**
 * 파일명 : LogSearchResult.java
 * 작성일자 : 26. 08. 10.
 * 작성자 : 최남희
 * 설명 : 로그 검색 응답 DTO. AI가 답변에 출처를 붙일 수 있도록 근거 위치(파일·줄번호)를 함께 싣는다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 10.       | 최남희  | 최초 생성
 * 26. 08. 10.       | 최남희  | searchedFiles·lines 정렬 규약 명시 (시간 오름차순)
 * --------------------------------------------------
 *
 * @param source        검색 대상 컴포넌트
 * @param searchedFiles 실제로 열어 읽은 파일 목록 (로그 루트 기준 상대경로, 시간 오름차순).
 *                      최신 파일부터 고르므로, limit이 차면 더 오래된 파일은 열지 않고 여기서 빠진다.
 * @param returnedLines lines의 개수
 * @param truncated     조건에 맞는 매칭이 더 있었으나 limit 때문에 잘렸는지 여부
 *                      (파일 내에서 밀려났거나, 더 오래된 파일을 열지 않았거나)
 * @param lines         매칭 줄 + contextAfter로 딸려온 줄. 최신 구간을 고르되 <b>시간 오름차순</b>으로 반환
 */
public record LogSearchResult(
        LogSource source,
        List<String> searchedFiles,
        int returnedLines,
        boolean truncated,
        List<LogLine> lines) {

    /**
     * @param file    로그 루트 기준 상대경로
     * @param lineNo  파일 내 줄 번호 (1-based)
     * @param text    줄 원문
     * @param matched true면 키워드에 직접 매칭된 줄, false면 contextAfter로 딸려온 뒷줄
     */
    public record LogLine(String file, long lineNo, String text, boolean matched) {
    }
}
