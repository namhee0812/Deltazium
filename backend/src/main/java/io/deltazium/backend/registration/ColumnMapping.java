package io.deltazium.backend.registration;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 파일명 : ColumnMapping.java
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : 타깃 컬럼 하나의 매핑. sourceExpr는 현재 '${소스컬럼}' 형식만 지원한다
 * (함수·치환식은 추후 확장 — 구문 검증이 이 확장 지점을 지킨다).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public record ColumnMapping(String targetColumn, String sourceExpr, boolean enabled) {

    /** ${COL} — Oracle 식별자 문자만 허용. `${{COL}` 같은 변형은 전부 거부된다. */
    private static final Pattern EXPR = Pattern.compile("^\\$\\{([A-Za-z][A-Za-z0-9_#$]*)}$");

    /** 구문이 유효하면 참조하는 소스 컬럼명을 돌려준다. */
    public Optional<String> sourceColumn() {
        if (sourceExpr == null) {
            return Optional.empty();
        }
        Matcher m = EXPR.matcher(sourceExpr.trim());
        return m.matches() ? Optional.of(m.group(1).toUpperCase()) : Optional.empty();
    }

    /** 동일명 매핑(리네임 아님) 여부 — 스톡 sink로 실반영 가능한 형태. */
    public boolean isIdentity() {
        return sourceColumn().map(c -> c.equalsIgnoreCase(targetColumn)).orElse(false);
    }
}
