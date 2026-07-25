package io.deltazium.backend.registry;

import java.util.List;
import java.util.Set;

import org.springframework.stereotype.Service;

@Service
public class DbConnectionService {

    private static final Set<String> ROLES = Set.of("SOURCE", "TARGET");

    private final DbConnectionRepository repository;
    private final OracleConnectionTester tester;

    public DbConnectionService(DbConnectionRepository repository, OracleConnectionTester tester) {
        this.repository = repository;
        this.tester = tester;
    }

    public List<DbConnection> list() {
        return repository.findAll();
    }

    public DbConnection get(long id) {
        return repository.findById(id)
                .orElseThrow(() -> new NotFoundException("연결 없음: id=" + id));
    }

    public DbConnection create(DbConnection c) {
        validate(c);
        repository.findByName(c.name()).ifPresent(dup -> {
            throw new IllegalArgumentException("이미 존재하는 이름: " + c.name());
        });
        return repository.insert(c);
    }

    public DbConnection update(long id, DbConnection c) {
        validate(c);
        DbConnection existing = get(id);
        // password 빈 값이면 기존 비밀번호 유지 (UI에서 미변경 수정 지원)
        String password = (c.password() == null || c.password().isBlank())
                ? existing.password() : c.password();
        DbConnection merged = new DbConnection(id, c.name(), "ORACLE", c.role(),
                c.host(), c.port(), c.databaseName(), c.username(), password);
        repository.update(merged);
        return merged;
    }

    public void delete(long id) {
        if (!repository.delete(id)) {
            throw new NotFoundException("연결 없음: id=" + id);
        }
    }

    /** 저장 없이 입력값으로 연결 확인 (등록 전 테스트), 또는 id 지정 시 저장된 값으로. */
    public OracleConnectionTester.Result test(DbConnection c) {
        validate(c);
        return tester.test(c);
    }

    public OracleConnectionTester.Result testSaved(long id) {
        return tester.test(get(id));
    }

    private void validate(DbConnection c) {
        if (c.dbType() != null) {
            DbType type = DbType.find(c.dbType())
                    .orElseThrow(() -> new IllegalArgumentException("알 수 없는 DB 종류: " + c.dbType()));
            if (!type.supported()) {
                throw new IllegalArgumentException("아직 지원하지 않는 DB 종류: " + type.label());
            }
        }
        if (c.name() == null || c.name().isBlank()) {
            throw new IllegalArgumentException("name은 필수다");
        }
        if (c.role() == null || !ROLES.contains(c.role())) {
            throw new IllegalArgumentException("role은 SOURCE 또는 TARGET이어야 한다");
        }
        if (c.host() == null || c.host().isBlank()) {
            throw new IllegalArgumentException("host는 필수다");
        }
        if (c.port() <= 0 || c.port() > 65535) {
            throw new IllegalArgumentException("port 범위 오류: " + c.port());
        }
        if (c.databaseName() == null || c.databaseName().isBlank()) {
            throw new IllegalArgumentException("databaseName(service name/SID)은 필수다");
        }
        if (c.username() == null || c.username().isBlank()) {
            throw new IllegalArgumentException("username은 필수다");
        }
    }

    public static class NotFoundException extends RuntimeException {
        public NotFoundException(String message) {
            super(message);
        }
    }
}
