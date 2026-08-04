package io.deltazium.backend.registry;

import java.util.Arrays;
import java.util.List;
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
 */
public enum DbType {
    ORACLE("Oracle", true),
    POSTGRESQL("PostgreSQL", false),
    MYSQL("MySQL", false);

    private final String label;
    private final boolean supported;

    DbType(String label, boolean supported) {
        this.label = label;
        this.supported = supported;
    }

    public String label() {
        return label;
    }

    public boolean supported() {
        return supported;
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
