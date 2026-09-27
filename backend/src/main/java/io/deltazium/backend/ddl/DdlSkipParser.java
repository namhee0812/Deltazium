package io.deltazium.backend.ddl;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 파일명 : DdlSkipParser.java
 * 작성일자 : 26. 09. 27.
 * 작성자 : 최남희
 * 설명 : DDL 건너뛰기(SKIPPED, architecture.md 7절) 시 컬럼 매핑을 갱신하기 위해 ddl_text에서
 * 단일 ADD/DROP COLUMN을 추출한다. 지원 형태(단순 단일 컬럼 ALTER)만 인식하고, 그 외(다중
 * 컬럼·ADD와 DROP이 섞인 복합 ALTER·타입 변경 등)는 빈 Optional — 호출측(DdlEventService)이
 * "재배포 없이 재개만" 폴백으로 처리한다("DDL 문장에서 컬럼명을 얻는 방법: ... ADD/DROP COLUMN
 * 단순 형태만 정규식으로, 그 외는 재개만" — 지시 원문). 정확한 해법은 파서(ANTLR 등)이고 현재
 * 범위 밖(docs/internals.md "DDL 치환의 한계"와 같은 제약).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 27.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public final class DdlSkipParser {

    public enum Kind { ADD, DROP }

    public record ColumnEdit(Kind kind, String column) {
    }

    private static final Pattern ADD_WORD = Pattern.compile("\\bADD\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern DROP_WORD = Pattern.compile("\\bDROP\\b", Pattern.CASE_INSENSITIVE);

    // Oracle: ADD ("COL" TYPE) / ADD "COL" TYPE / ADD (COL TYPE)
    // PostgreSQL: ADD COLUMN "col" TYPE
    private static final Pattern ADD_SINGLE = Pattern.compile(
            "\\bADD\\s*\\(?\\s*(?:COLUMN\\s+)?\"?([A-Za-z_][A-Za-z0-9_$#]*)\"?\\s+[A-Za-z]",
            Pattern.CASE_INSENSITIVE);
    // Oracle: DROP ("COL") / DROP "COL" / DROP COLUMN "COL"
    // PostgreSQL: DROP COLUMN "col"
    private static final Pattern DROP_SINGLE = Pattern.compile(
            "\\bDROP\\s*\\(?\\s*(?:COLUMN\\s+)?\"?([A-Za-z_][A-Za-z0-9_$#]*)\"?\\s*\\)?\\s*$",
            Pattern.CASE_INSENSITIVE);

    private DdlSkipParser() {
    }

    /**
     * @return 단일 ADD 또는 단일 DROP으로 인식되면 그 컬럼명·종류, 그 외(복합·다중 컬럼·인식
     *         불가)는 빈 Optional.
     */
    public static Optional<ColumnEdit> parse(String ddlText) {
        if (ddlText == null || ddlText.isBlank()) {
            return Optional.empty();
        }
        String ddl = ddlText.strip();
        boolean hasAdd = ADD_WORD.matcher(ddl).find();
        boolean hasDrop = DROP_WORD.matcher(ddl).find();
        if (hasAdd == hasDrop) {
            return Optional.empty(); // 둘 다 없거나(인식 불가) 둘 다 있음(ADD+DROP 복합) — 단순 형태 아님
        }
        if (ddl.contains(",")) {
            return Optional.empty(); // 컬럼 여러 개 — 단순 형태 아님
        }
        if (hasAdd) {
            Matcher m = ADD_SINGLE.matcher(ddl);
            return m.find() ? Optional.of(new ColumnEdit(Kind.ADD, m.group(1))) : Optional.empty();
        }
        Matcher m = DROP_SINGLE.matcher(ddl);
        return m.find() ? Optional.of(new ColumnEdit(Kind.DROP, m.group(1))) : Optional.empty();
    }
}
