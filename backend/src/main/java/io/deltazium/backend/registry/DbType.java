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
 * 26. 09. 28.       | 최남희  | SingleStore 타깃 지원 추가 — sourceCapable(SOURCE 역할 가용 여부)
 * |                          | 필드 신설, SingleStore는 supported=true·sourceCapable=false(타깃
 * |                          | 전용, architecture.md 8절). foldIdentifier에 SingleStore 분기
 * |                          | 추가(원문 유지 — 대소문자를 보존하는 DB라 폴딩 자체가 없음, 실측
 * |                          | 2026-09-28: CREATE TABLE MixedCase 후 select mixedcase는 오류).
 * |                          | quoteIdentifier 추가(Oracle·PG는 큰따옴표, SingleStore는 백틱) —
 * |                          | SchemaFingerprint·ChecksumSql의 식별자 인용에 공용으로 쓴다.
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | Databricks 타깃 추가 — dwTarget 속성 신설(DW 계열 타깃은 JDBC sink
 * |                          | 대신 DW apply 워커로 적재, architecture.md 6.5절). DATABRICKS는
 * |                          | supported=true·hasSchemaChangeTopic=false·sourceCapable=false·
 * |                          | dwTarget=true. foldIdentifier는 소문자(UC는 식별자를 소문자로 저장),
 * |                          | quoteIdentifier는 백틱.
 * --------------------------------------------------
 */
public enum DbType {
    ORACLE("Oracle", true, true, true, false),
    POSTGRESQL("PostgreSQL", true, false, true, false),
    MYSQL("MySQL", false, true, true, false),
    SINGLESTORE("SingleStore", true, false, false, false),
    DATABRICKS("Databricks", true, false, false, true);

    private final String label;
    private final boolean supported;
    private final boolean hasSchemaChangeTopic;
    private final boolean sourceCapable;
    private final boolean dwTarget;

    DbType(String label, boolean supported, boolean hasSchemaChangeTopic, boolean sourceCapable,
           boolean dwTarget) {
        this.label = label;
        this.supported = supported;
        this.hasSchemaChangeTopic = hasSchemaChangeTopic;
        this.sourceCapable = sourceCapable;
        this.dwTarget = dwTarget;
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

    /**
     * true면 SOURCE 역할로 쓸 수 있다 — false인 DB는 등록 화면에서 소스로 선택할 수 없고
     * {@link io.deltazium.backend.registry.DbConnectionService}가 SOURCE 생성을 거부한다.
     * SingleStore는 타깃 전용(architecture.md 8절, 2026-09-28) — Debezium source 커넥터가
     * 없어 캡처 원본이 될 수 없다.
     */
    public boolean sourceCapable() {
        return sourceCapable;
    }

    /**
     * true면 DW 계열 타깃 — JDBC sink 대신 DW apply 워커(stage-and-merge)가 적재한다
     * (architecture.md 6.5절). 등록 시 jdbc-sink를 배포하지 않고 DwApplyService가 워커를 관리한다.
     */
    public boolean dwTarget() {
        return dwTarget;
    }

    /**
     * 식별자 대소문자 정규화 — 소스 딕셔너리 조회 전용(mixed-case 소스 테이블을 실제 카탈로그
     * 값과 맞추기 위함). Oracle은 대문자, 그 외(PostgreSQL·SingleStore)는 원문 그대로
     * (architecture.md 8절). SingleStore는 sourceCapable=false라 이 경로에 실질적으로
     * 도달하지 않지만, 도달하더라도 원문 유지가 안전한 기본값이라 별도 분기를 두지 않는다.
     */
    public String normalizeIdentifier(String raw) {
        return this == ORACLE ? raw.toUpperCase(Locale.ROOT) : raw;
    }

    /**
     * 타깃 식별자 폴딩 — 등록 시 타깃 스키마·테이블명을 저장할 때 쓴다(architecture.md 8절,
     * 2026-09-22 결정: PG 타깃 DDL 승인 502 결함 D1 수정). 저장값은 **그 DB가 unquoted
     * 식별자를 접는 형태**로 고정한다 — Oracle은 대문자, PostgreSQL은 소문자,
     * **SingleStore는 원문 유지**(2026-09-28 추가 — SingleStore는 식별자 대소문자를 그대로
     * 보존하는 DB라 "접는 형태" 자체가 없다: 실측(9.0.44) `CREATE TABLE MixedCase` 후
     * `SELECT * FROM mixedcase`가 1146(테이블 없음) 오류. 그래서 원문을 그대로 저장값으로
     * 쓰는 것이 유일하게 항상 일치하는 선택이다). 이 값 하나가 {@code registered_tables}·JDBC
     * sink {@code collection.name.format}·DDL 승인 초안의 따옴표 식별자에 그대로 쓰이므로,
     * 폴딩이 다르면 따옴표 식별자가 실제 카탈로그 값과 어긋난다(JDBC sink는 unquoted라
     * 우연히 동작하지만 DDL 승인은 실패).
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
        return switch (this) {
            case POSTGRESQL -> raw.toLowerCase(Locale.ROOT);
            case SINGLESTORE -> raw;
            case DATABRICKS -> raw.toLowerCase(Locale.ROOT); // UC는 식별자를 소문자로 저장
            default -> raw.toUpperCase(Locale.ROOT); // ORACLE·MYSQL
        };
    }

    /**
     * 식별자 인용 부호 — DDL 초안 조립(SchemaFingerprint)·체크섬 SQL(ChecksumSql)이 따옴표
     * 식별자를 만들 때 공용으로 쓴다(2026-09-28, SingleStore 타깃 지원). Oracle·PostgreSQL은
     * 큰따옴표, SingleStore(MySQL 계열)는 백틱 — 표준 SQL 큰따옴표를 식별자 구분자로 쓰지
     * 않는다(공식 문법: {@code `identifier`}).
     */
    public String quoteIdentifier(String raw) {
        String q = (this == SINGLESTORE || this == DATABRICKS) ? "`" : "\"";
        return q + raw + q;
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
