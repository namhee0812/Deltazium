package io.deltazium.backend.registry;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 파일명 : DbConnection.java
 * 작성일자 : 26. 07. 25.
 * 작성자 : 최남희
 * 설명 : DB 연결 등록 정보. password는 응답 직렬화에서 제외 (WRITE_ONLY).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 25.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ②: topicPrefix 필드 추가(SOURCE 전용,
 * |                          | architecture.md 2.2·4절 — Debezium topic.prefix), jdbcUrl()을
 * |                          | dbType별로 분기(PostgreSQL 지원). 기존 9-인자 생성자는 유지해
 * |                          | topicPrefix=null(TARGET 등)로 호출부 하위 호환
 * --------------------------------------------------
 * 26. 09. 28.       | 최남희  | SingleStore 타깃 지원 — jdbcUrl()에 SINGLESTORE 분기 추가
 * |                          | (mysql-connector-j, useSSL=false&allowPublicKeyRetrieval=true —
 * |                          | 개발 환경 실측 기준, architecture.md 8절)
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | Databricks 타깃 지원 — httpPath·stagingVolume 필드 추가(DATABRICKS
 * |                          | 전용, 다른 DB는 null), jdbcUrl()에 DATABRICKS 분기(PWD는 URL이
 * |                          | 아니라 Properties로 — DatabricksSupport). 기존 9·10-인자 생성자는
 * |                          | 유지해 호출부 하위 호환. DATABRICKS 필드 의미: host=Server hostname,
 * |                          | port=443 고정, databaseName=UC catalog, username="token" 고정,
 * |                          | password=PAT(비면 환경변수 DZ_DATABRICKS_TOKEN)
 * --------------------------------------------------
 */
public record DbConnection(
        Long id,
        String name,
        String dbType,
        String role,          // SOURCE | TARGET
        String host,
        int port,
        String databaseName,  // Oracle service name/SID 또는 PostgreSQL 데이터베이스명
        String username,
        @JsonProperty(access = JsonProperty.Access.WRITE_ONLY) String password,
        String topicPrefix, // SOURCE 전용 — Debezium topic.prefix (소스 식별자, 2.2절)
        String httpPath,       // DATABRICKS 전용 — SQL warehouse HTTP path
        String stagingVolume) { // DATABRICKS 전용 — "<schema>.<volume>" (카탈로그 내 UC Volume)

    /** 하위 호환 — httpPath·stagingVolume 없이 생성(Databricks 외 모든 호출부). */
    public DbConnection(Long id, String name, String dbType, String role, String host, int port,
                        String databaseName, String username, String password, String topicPrefix) {
        this(id, name, dbType, role, host, port, databaseName, username, password, topicPrefix, null, null);
    }

    /** 하위 호환 — topicPrefix 없이 생성(주로 TARGET 연결·기존 호출부). */
    public DbConnection(Long id, String name, String dbType, String role, String host, int port,
                        String databaseName, String username, String password) {
        this(id, name, dbType, role, host, port, databaseName, username, password, null, null, null);
    }

    public DbConnection withId(long newId) {
        return new DbConnection(newId, name, dbType, role, host, port,
                databaseName, username, password, topicPrefix, httpPath, stagingVolume);
    }

    public String jdbcUrl() {
        DbType type = DbType.find(dbType).orElse(DbType.ORACLE);
        return switch (type) {
            case POSTGRESQL -> "jdbc:postgresql://%s:%d/%s".formatted(host, port, databaseName);
            case SINGLESTORE -> "jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true"
                    .formatted(host, port, databaseName);
            // PWD(토큰)는 URL에 넣지 않는다 — DatabricksSupport.props()가 Properties로 넘긴다
            case DATABRICKS -> "jdbc:databricks://%s:%d;httpPath=%s;AuthMech=3;UID=token"
                    .formatted(host, port, httpPath);
            default -> "jdbc:oracle:thin:@//%s:%d/%s".formatted(host, port, databaseName);
        };
    }
}
