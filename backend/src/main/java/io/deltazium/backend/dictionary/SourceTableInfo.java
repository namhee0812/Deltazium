package io.deltazium.backend.dictionary;

/**
 * 파일명 : SourceTableInfo.java
 * 작성일자 : 26. 07. 25.
 * 작성자 : 최남희
 * 설명 : 소스 딕셔너리에서 조회한 테이블 정보 + 등록 사전 점검 결과.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 25.       | 최남희  | 최초 생성
 * --------------------------------------------------
 *
 * @param hasPk      PK 존재 (없으면 등록 거부 — 멱등 upsert 전제)
 * @param suppLogAll 테이블 레벨 supplemental logging (ALL) COLUMNS 여부
 * @param numRows    통계상 행 수 (ALL_TABLES.NUM_ROWS, 통계 미수집이면 null)
 */
public record SourceTableInfo(
        String schema,
        String table,
        boolean hasPk,
        boolean suppLogAll,
        Long numRows) {

    public String qualified() {
        return schema + "." + table;
    }
}
