package io.deltazium.dwapply;

import java.util.List;

/**
 * 파일명 : TableSpec.java
 * 작성일자 : 26. 10. 19.
 * 작성자 : 최남희
 * 설명 : 워커가 담당하는 타깃 테이블 한 건 — 입력 토픽, 타깃 schema/table, 키 컬럼(타깃 컬럼명), 컬럼 매핑.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public record TableSpec(List<String> topics, String schema, String table,
                        List<String> keyColumns, List<ColumnMapping> columns) {

    /** 상태 파일·로그에 쓰는 식별자 (`schema.table`). */
    public String key() {
        return schema + "." + table;
    }

    /** Volume 아래 이 테이블 전용 디렉터리 이름 — 기동 시 sweep 대상이 되는 워커 소유 영역. */
    public String dirName() {
        return schema + "__" + table;
    }

    /** 타깃 컬럼명 중 키가 아닌 것들 (MERGE UPDATE SET 대상). */
    public List<String> valueColumns() {
        return columns.stream().map(ColumnMapping::target)
                .filter(c -> keyColumns.stream().noneMatch(k -> k.equalsIgnoreCase(c))).toList();
    }
}
