package io.deltazium.backend.registry;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * DB 연결 등록 정보. 현재 Oracle만 지원 (dbType 고정 검증은 서비스에서).
 * password는 응답 직렬화에서 제외 (WRITE_ONLY).
 */
public record DbConnection(
        Long id,
        String name,
        String dbType,
        String role,          // SOURCE | TARGET
        String host,
        int port,
        String databaseName,  // Oracle service name 또는 SID
        String username,
        @JsonProperty(access = JsonProperty.Access.WRITE_ONLY) String password) {

    public DbConnection withId(long newId) {
        return new DbConnection(newId, name, dbType, role, host, port,
                databaseName, username, password);
    }

    public String jdbcUrl() {
        return "jdbc:oracle:thin:@//%s:%d/%s".formatted(host, port, databaseName);
    }
}
