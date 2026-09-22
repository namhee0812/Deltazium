package io.deltazium.backend.registry;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

import org.springframework.stereotype.Service;

/**
 * 파일명 : DbConnectionService.java
 * 작성일자 : 26. 07. 25.
 * 작성자 : 최남희
 * 설명 : DB 연결 저장소 서비스 — 저장·조회·연결 테스트.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 25.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ②(architecture.md 2.2·4절): topicPrefix를
 * |                          | SOURCE 연결 필수·유일 값으로 검증(정규식 `^[a-z][a-z0-9_]*$`,
 * |                          | 미입력 시 이름 슬러그로 기본값). update()가 dbType을 "ORACLE"로
 * |                          | 고정하던 버그 수정 — PostgreSQL 연결이 수정 후에도 유지되도록.
 * --------------------------------------------------
 * 26. 09. 23.       | 최남희  | topicPrefix 요청값을 받지 않는다(4절 확정) — 생성 시 이름 슬러그로
 * |                          | 고정, 수정 시 기존 값 유지. 소스마다 손으로 준 prefix(dz·pg·nhtest)가
 * |                          | 규칙 없이 흩어져 토픽 이름 기준이 안 보이던 문제. 검증 정규식은 유지.
 * --------------------------------------------------
 */
@Service
public class DbConnectionService {

    private static final Set<String> ROLES = Set.of("SOURCE", "TARGET");
    private static final Pattern TOPIC_PREFIX = Pattern.compile("^[a-z][a-z0-9_]*$");

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
        DbConnection normalized = normalizeTopicPrefix(c, null);
        // 이름 중복을 topicPrefix 검증보다 먼저 본다 — 이름이 같으면 기본 슬러그도 같아져
        // topicPrefix 충돌 메시지가 먼저 뜨는 것보다 "이미 존재하는 이름"이 더 정확한 원인이다.
        repository.findByName(normalized.name()).ifPresent(dup -> {
            throw new IllegalArgumentException("이미 존재하는 이름: " + normalized.name());
        });
        validate(normalized);
        return repository.insert(normalized);
    }

    public DbConnection update(long id, DbConnection c) {
        DbConnection existing = get(id);
        DbConnection normalized = normalizeTopicPrefix(c, existing);
        validate(normalized, id);
        // password 빈 값이면 기존 비밀번호 유지 (UI에서 미변경 수정 지원)
        String password = (normalized.password() == null || normalized.password().isBlank())
                ? existing.password() : normalized.password();
        // dbType 미지정이면 기존 값 유지 — 이전엔 항상 "ORACLE"로 고정돼 PostgreSQL 연결이
        // 수정 후 Oracle로 되돌아가는 결함이 있었다.
        String dbType = normalized.dbType() == null ? existing.dbType() : normalized.dbType();
        DbConnection merged = new DbConnection(id, normalized.name(), dbType, normalized.role(),
                normalized.host(), normalized.port(), normalized.databaseName(), normalized.username(),
                password, normalized.topicPrefix());
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
        validate(normalizeTopicPrefix(c, null));
        return tester.test(c);
    }

    public OracleConnectionTester.Result testSaved(long id) {
        return tester.test(get(id));
    }

    /**
     * TARGET 연결은 topicPrefix를 강제로 비운다(값이 있어도 의미 없음 — SOURCE 전용, 2.2절).
     * SOURCE 연결의 topicPrefix는 요청값을 받지 않는다(4절, 2026-09-23 확정): 생성 시 이름
     * 슬러그로 고정하고, 이후 수정에서는 기존 값을 유지한다 — 토픽·changelog namespace·복제
     * 슬롯 이름이 이 값에 묶여 있어 생성 후 바뀌면 안 되고, 이름은 표시용이라 바뀔 수 있다.
     * 그래서 "이름 ≠ prefix"는 이름 변경 후에만 생기며, 화면은 그 경우에만 prefix를 보여준다.
     */
    private DbConnection normalizeTopicPrefix(DbConnection c, DbConnection existing) {
        if (c.role() == null || !"SOURCE".equals(c.role())) {
            return c.topicPrefix() == null ? c
                    : new DbConnection(c.id(), c.name(), c.dbType(), c.role(), c.host(), c.port(),
                            c.databaseName(), c.username(), c.password(), null);
        }
        String prefix = existing != null && existing.topicPrefix() != null
                ? existing.topicPrefix() : slug(c.name());
        return new DbConnection(c.id(), c.name(), c.dbType(), c.role(), c.host(), c.port(),
                c.databaseName(), c.username(), c.password(), prefix);
    }

    /** 이름 → topic_prefix 기본값 슬러그. 숫자로 시작하면 접두를 붙여 정규식을 만족시킨다. */
    static String slug(String name) {
        String s = (name == null ? "" : name).toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9]+", "_").replaceAll("^_+|_+$", "");
        if (s.isEmpty()) {
            s = "src";
        }
        if (!Character.isLetter(s.charAt(0))) {
            s = "s_" + s;
        }
        return s;
    }

    private void validate(DbConnection c) {
        validate(c, null);
    }

    private void validate(DbConnection c, Long selfId) {
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
        if ("SOURCE".equals(c.role())) {
            if (c.topicPrefix() == null || !TOPIC_PREFIX.matcher(c.topicPrefix()).matches()) {
                throw new IllegalArgumentException(
                        "topicPrefix는 소문자로 시작하는 [a-z][a-z0-9_]* 형식이어야 한다: " + c.topicPrefix());
            }
            repository.findByTopicPrefix(c.topicPrefix()).ifPresent(dup -> {
                if (selfId == null || !dup.id().equals(selfId)) {
                    throw new IllegalArgumentException("이미 사용 중인 topicPrefix: " + c.topicPrefix());
                }
            });
        }
    }

    public static class NotFoundException extends RuntimeException {
        public NotFoundException(String message) {
            super(message);
        }
    }
}
