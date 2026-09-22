package io.deltazium.backend.registry;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * 파일명 : DbType.java
 * 작성일자 : 26. 07. 25.
 * 작성자 : 최남희
 * 설명 : 지원 DB 종류. 확장 시 여기에 추가하고 supported 플래그를 켠다 —
 * UI 선택 목록은 GET /api/connections/db-types로 이 목록을 내려받는다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 25.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ② PostgreSQL 활성화(supported=true).
 * |                          | hasSchemaChangeTopic(architecture.md 7절 — Oracle/MySQL은
 * |                          | schema change topic 발행, PostgreSQL은 미발행이라 스키마 지문
 * |                          | 비교로 감지) · normalizeIdentifier(8절 — Oracle 대문자,
 * |                          | PostgreSQL 원문 유지) 추가
 * --------------------------------------------------
 * 26. 09. 22.       | 최남희  | PG 타깃 DDL 승인 502 결함(D1) 수정 — foldIdentifier 추가.
 * |                          | normalizeIdentifier(소스 딕셔너리 조회용)와 분리 — 타깃 저장값은
 * |                          | 항상 타깃 DbType의 unquoted 폴딩(Oracle 대문자·PG 소문자)이어야
 * |                          | JDBC sink unquoted 실행·DDL 승인 초안 따옴표 식별자가 일치한다.
 * --------------------------------------------------
 */
public enum DbType {
    ORACLE("Oracle", true, true),
    POSTGRESQL("PostgreSQL", true, false),
    MYSQL("MySQL", false, true);

    private final String label;
    private final boolean supported;
    private final boolean hasSchemaChangeTopic;

    DbType(String label, boolean supported, boolean hasSchemaChangeTopic) {
        this.label = label;
        this.supported = supported;
        this.hasSchemaChangeTopic = hasSchemaChangeTopic;
    }

    public String label() {
        return label;
    }

    public boolean supported() {
        return supported;
    }

    /**
     * true면 Debezium이 schema change topic을 발행해 DdlEventPoller(7절 1번)로 감지한다.
     * false면 스키마 지문 비교(SchemaFingerprintService, 7절 개정)로 감지한다.
     */
    public boolean hasSchemaChangeTopic() {
        return hasSchemaChangeTopic;
    }

    /** 식별자 대소문자 정규화 — Oracle은 대문자, PostgreSQL은 원문 그대로 (architecture.md 8절). */
    public String normalizeIdentifier(String raw) {
        return this == ORACLE ? raw.toUpperCase(Locale.ROOT) : raw;
    }

    /**
     * 타깃 식별자 폴딩 — 등록 시 타깃 스키마·테이블명을 저장할 때 쓴다(architecture.md 8절,
     * 2026-09-22 결정: PG 타깃 DDL 승인 502 결함 D1 수정). 저장값은 **그 DB가 unquoted
     * 식별자를 접는 형태**로 고정한다 — Oracle은 대문자, PostgreSQL은 소문자. 이 값 하나가
     * {@code registered_tables}·JDBC sink {@code collection.name.format}·DDL 승인 초안의
     * 따옴표 식별자({@code "schema"."table"})에 그대로 쓰이므로, 폴딩이 다르면 따옴표 식별자가
     * 실제 카탈로그 값과 어긋난다(JDBC sink는 unquoted라 우연히 동작하지만 DDL 승인은 실패).
     *
     * <p>{@link #normalizeIdentifier(String)}와 목적이 다르다 — 그쪽은 **소스** 딕셔너리
     * 조회용으로, mixed-case 소스 테이블을 실제 카탈로그 값과 맞추기 위해 PostgreSQL은
     * 원문을 유지한다. foldIdentifier는 반대로 타깃 저장값을 항상 하나의 폴딩 규칙으로
     * 고정한다 — 사용자가 따옴표로 만든 mixed-case 타깃 테이블은 범위 밖이다(제약,
     * docs/operations.md).
     */
    public String foldIdentifier(String raw) {
        if (raw == null) {
            return null;
        }
        return this == POSTGRESQL ? raw.toLowerCase(Locale.ROOT) : raw.toUpperCase(Locale.ROOT);
    }

    public static List<DbType> supportedTypes() {
        return Arrays.stream(values()).filter(DbType::supported).toList();
    }

    public static Optional<DbType> find(String code) {
        return Arrays.stream(values())
                .filter(t -> t.name().equalsIgnoreCase(code))
                .findFirst();
    }
}
