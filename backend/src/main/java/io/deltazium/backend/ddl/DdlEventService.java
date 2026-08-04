package io.deltazium.backend.ddl;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.deltazium.backend.connect.ConnectClient;
import io.deltazium.backend.events.TableEventService;
import io.deltazium.backend.registration.RegisteredTable;
import io.deltazium.backend.registration.RegisteredTableRepository;
import io.deltazium.backend.registry.DbConnectionService;
import org.springframework.stereotype.Service;

/**
 * 파일명 : DdlEventService.java
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : DDL 승인 워크플로 (architecture.md 7절).
 * 승인: 타깃에 DDL 적용 → 해당 테이블 CDC 계속.
 * 거부: jdbc-sink 구독에서 해당 테이블 토픽 제외 → apply만 정지 (changelog는 계속 축적).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@Service
public class DdlEventService {

    private final DdlEventRepository repository;
    private final RegisteredTableRepository registrations;
    private final DbConnectionService connections;
    private final TargetDdlExecutor executor;
    private final ConnectClient connect;
    private final TableEventService events;

    public DdlEventService(DdlEventRepository repository,
                           RegisteredTableRepository registrations,
                           DbConnectionService connections,
                           TargetDdlExecutor executor,
                           ConnectClient connect,
                           TableEventService events) {
        this.repository = repository;
        this.registrations = registrations;
        this.connections = connections;
        this.executor = executor;
        this.connect = connect;
        this.events = events;
    }

    public List<DdlEvent> list() {
        return repository.findAll();
    }

    /** 승인 — 등록 테이블의 DDL만 가능. 타깃 이름이 다르면 DDL을 치환 후 실행, 성공 시에만 상태 변경. */
    public DdlEvent approve(long id) {
        DdlEvent event = pending(id);
        RegisteredTable registered = requireRegistered(event);
        String ddl = rewriteForTarget(event.ddlText(), registered);
        executor.execute(connections.get(registered.targetConnectionId()), ddl);
        String note = ddl.equals(event.ddlText())
                ? "타깃에 DDL 적용 완료"
                : "타깃 이름으로 치환 적용: " + registered.targetQualified();
        repository.decide(id, "APPROVED", note);
        events.info(registered.schemaName(), registered.tableName(), "DDL_APPROVED",
                "DDL 승인·타깃 적용: " + firstLine(ddl));
        return repository.findById(id).orElseThrow();
    }

    /** 거부 — 해당 테이블의 jdbc-sink 커넥터를 pause (apply만 정지, changelog는 계속). */
    public DdlEvent reject(long id) {
        DdlEvent event = pending(id);
        RegisteredTable registered = requireRegistered(event);
        String connector = "dz-jdbc-sink-" + registered.suffix();
        connect.pause(connector);
        repository.decide(id, "REJECTED",
                "apply 정지 — " + connector + " pause. changelog는 계속 축적됨");
        events.record(registered.schemaName(), registered.tableName(), "DDL_REJECTED", "WARN",
                "DDL 거부 — apply 정지: " + firstLine(event.ddlText()), null);
        return repository.findById(id).orElseThrow();
    }

    /**
     * 소스 스키마.테이블 참조를 타깃 이름으로 치환 (매핑이 동일하면 원문 그대로).
     * 다루는 형태: "S"."T" · S.T · 스키마 없이 T (따옴표 유무 조합, 대소문자 무관).
     */
    static String rewriteForTarget(String ddl, RegisteredTable t) {
        if (t.qualified().equalsIgnoreCase(t.targetQualified())) {
            return ddl;
        }
        String replacement = Matcher.quoteReplacement(
                "\"" + t.targetSchema() + "\".\"" + t.targetTable() + "\"");
        // 스키마 한정 참조 (S.T / "S"."T" / S."T" / "S".T)
        Pattern qualifiedRef = Pattern.compile(
                "\"?" + Pattern.quote(t.schemaName()) + "\"?\\.\"?" + Pattern.quote(t.tableName()) + "\"?",
                Pattern.CASE_INSENSITIVE);
        String out = qualifiedRef.matcher(ddl).replaceAll(replacement);
        // 비한정 참조 (TABLE T ...) — 단어 경계로만 매칭해 컬럼명 등 오치환 방지
        Pattern bareRef = Pattern.compile(
                "(?<![\\w.\"])\"?" + Pattern.quote(t.tableName()) + "\"?(?![\\w\"])",
                Pattern.CASE_INSENSITIVE);
        return bareRef.matcher(out).replaceAll(replacement);
    }

    private static String firstLine(String ddl) {
        String s = ddl.strip();
        int nl = s.indexOf('\n');
        return nl > 0 ? s.substring(0, nl) : s.length() > 120 ? s.substring(0, 120) + "…" : s;
    }

    private DdlEvent pending(long id) {
        DdlEvent event = repository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("DDL 이벤트 없음: id=" + id));
        if (!"DETECTED".equals(event.state())) {
            throw new IllegalArgumentException("승인 대기 상태가 아니다: " + event.state());
        }
        return event;
    }

    private RegisteredTable requireRegistered(DdlEvent event) {
        return registrations.findAll().stream()
                .filter(t -> t.schemaName().equalsIgnoreCase(event.schemaName())
                        && t.tableName().equalsIgnoreCase(event.tableName()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "등록되지 않은 테이블의 DDL이다: " + event.schemaName() + "." + event.tableName()));
    }
}
