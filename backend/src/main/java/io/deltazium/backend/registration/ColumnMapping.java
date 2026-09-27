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
 * 26. 09. 27.       | 최남희  | sourceColumnRaw() 추가 — sourceColumn()의 대문자 접기는 비교용으로만
 * |                          | 두고, sink field.include.list처럼 카탈로그 대소문자가 필요한 곳은 원문
 * --------------------------------------------------
 */
public record ColumnMapping(String targetColumn, String sourceExpr, boolean enabled) {

    /** ${COL} — Oracle 식별자 문자만 허용. `${{COL}` 같은 변형은 전부 거부된다. */
    private static final Pattern EXPR = Pattern.compile("^\\$\\{([A-Za-z][A-Za-z0-9_#$]*)}$");

    /** 구문이 유효하면 참조하는 소스 컬럼명을 **대문자로** 돌려준다 — 대소문자 무시 비교용(isIdentity 등).
     * 실제 카탈로그 대소문자가 필요한 곳(JDBC sink field.include.list, 체크섬 SQL)은 {@link #sourceColumnRaw()}. */
    public Optional<String> sourceColumn() {
        return sourceColumnRaw().map(String::toUpperCase);
    }

    /** 변환식 `${col}` 안의 소스 컬럼명 원문 — PostgreSQL 소스는 소문자, Oracle은 대문자가 그대로 온다.
     * Debezium JDBC sink의 field.include.list는 레코드 필드명과 대소문자까지 일치해야 한다
     * (2026-09-27 실측: 대문자 목록이 PG 소문자 필드와 안 맞아 키 필드까지 걸러져 "no key fields" 실패). */
    public Optional<String> sourceColumnRaw() {
        if (sourceExpr == null) {
            return Optional.empty();
        }
        Matcher m = EXPR.matcher(sourceExpr.trim());
        return m.matches() ? Optional.of(m.group(1)) : Optional.empty();
    }

    /** 동일명 매핑(리네임 아님) 여부 — 스톡 sink로 실반영 가능한 형태. */
    public boolean isIdentity() {
        return sourceColumn().map(c -> c.equalsIgnoreCase(targetColumn)).orElse(false);
    }
}
