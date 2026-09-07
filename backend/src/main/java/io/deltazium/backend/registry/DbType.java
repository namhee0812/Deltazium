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

    public static List<DbType> supportedTypes() {
        return Arrays.stream(values()).filter(DbType::supported).toList();
    }

    public static Optional<DbType> find(String code) {
        return Arrays.stream(values())
                .filter(t -> t.name().equalsIgnoreCase(code))
                .findFirst();
    }
}
