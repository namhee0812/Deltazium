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
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ②: suppLogAll → captureReady로 일반화
 * |                          | (Oracle은 supp.log ALL COLUMNS, PostgreSQL은 REPLICA IDENTITY
 * |                          | FULL — 소스별로 의미는 다르지만 "캡처 사전조건 충족" 여부는 공통)
 * --------------------------------------------------
 *
 * @param hasPk        PK 존재 (없으면 등록 거부 — 멱등 upsert 전제)
 * @param captureReady 테이블 레벨 캡처 사전조건 충족 여부 (Oracle: supp.log ALL COLUMNS,
 *                     PostgreSQL: REPLICA IDENTITY FULL)
 * @param numRows      통계상 행 수 (통계 미수집이면 null)
 */
public record SourceTableInfo(
        String schema,
        String table,
        boolean hasPk,
        boolean captureReady,
        Long numRows) {

    public String qualified() {
        return schema + "." + table;
    }
}
