package io.deltazium.backend.ddl;

import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.deltazium.backend.connect.ConnectClient;
import io.deltazium.backend.connect.ConnectorNames;
import io.deltazium.backend.events.TableEventService;
import io.deltazium.backend.registration.RegisteredTable;
import io.deltazium.backend.registration.RegisteredTableRepository;
import io.deltazium.backend.registration.RegistrationService;
import io.deltazium.backend.registry.DbConnectionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ②: jdbc-sink 커넥터명에 소스 topicPrefix
 * |                          | 반영(ConnectorNames.jdbcSink) — DbConnectionService 의존 추가
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | PG 소스 실 배선 스모크 수정: origin=FINGERPRINT는 ddl_text가
 * |                          | 이미 타깃 이름으로 조립돼 있어(SchemaFingerprint.draftDdl) 이름
 * |                          | 치환 없이 그대로 실행 — rewriteForTarget은 SCHEMA_TOPIC 전용
 * --------------------------------------------------
 * 26. 09. 27.       | 최남희  | 테이블별 DDL 반영 정책(architecture.md 7절 개정) 구현.
 * |                          | handleNewEvent — 감지 직후 두 poller(DdlEventPoller·
 * |                          | SchemaFingerprintPoller)가 부르는 진입점: AUTO 정책 테이블만
 * |                          | 골라 타깃 DDL을 자동 실행하고 sink를 재개하며(AUTO_APPLIED),
 * |                          | 초안이 없거나 실행에 실패하면 MANUAL로 폴백(DETECTED 유지 + WARN
 * |                          | 이벤트). approve에 sink 재개(ConnectClient.resumeAfterDdl —
 * |                          | FAILED면 restartFailed, PAUSED면 resume)를 통합. skip(신규,
 * |                          | SKIPPED) 추가 — DdlSkipParser로 ddl_text에서 단순 ADD/DROP
 * |                          | COLUMN을 인식해 RegistrationService의 컬럼 매핑·재배포로
 * |                          | field.include.list에서 뺀 뒤 재개, 인식 못 하면 재배포 없이
 * |                          | 재개만 한다
 * --------------------------------------------------
 */
@Service
public class DdlEventService {

    private static final Logger log = LoggerFactory.getLogger(DdlEventService.class);

    private final DdlEventRepository repository;
    private final RegisteredTableRepository registrations;
    private final DbConnectionService connections;
    private final TargetDdlExecutor executor;
    private final ConnectClient connect;
    private final TableEventService events;
    private final RegistrationService registrationService;

    public DdlEventService(DdlEventRepository repository,
                           RegisteredTableRepository registrations,
                           DbConnectionService connections,
                           TargetDdlExecutor executor,
                           ConnectClient connect,
                           TableEventService events,
                           RegistrationService registrationService) {
        this.repository = repository;
        this.registrations = registrations;
        this.connections = connections;
        this.executor = executor;
        this.connect = connect;
        this.events = events;
        this.registrationService = registrationService;
    }

    public List<DdlEvent> list() {
        return repository.findAll();
    }

    /**
     * 감지 직후 정책 분기 진입점 — DdlEventPoller(schema change topic)·SchemaFingerprintPoller
     * (스키마 지문)가 새 이벤트를 적재한 직후 이 id로 호출한다. 미등록 테이블·MANUAL 정책
     * 테이블은 그대로 둔다(현행 동작). AUTO 정책이고 실행 가능한 초안이 있으면(state=DETECTED)
     * 즉시 타깃에 적용하고 sink를 재개한다. 지문 경로의 타입 변경(초안 없음, state=SNAPSHOT)은
     * AUTO 정책이어도 자동 적용할 방법이 없어 DETECTED로 끌어올려 사용자가 보게 한다(그 외
     * 정책 무관 SNAPSHOT·IGNORED는 손대지 않는다).
     */
    public void handleNewEvent(long eventId) {
        DdlEvent event = repository.findById(eventId).orElse(null);
        if (event == null) {
            return;
        }
        RegisteredTable registered = findRegisteredOrNull(event);
        if (registered == null || !"AUTO".equals(registered.ddlPolicy())) {
            return;
        }
        boolean noDraftFingerprint = "FINGERPRINT".equals(event.origin()) && "SNAPSHOT".equals(event.state())
                && (event.ddlText() == null || event.ddlText().isBlank());
        if (!"DETECTED".equals(event.state()) && !noDraftFingerprint) {
            return; // SCHEMA_TOPIC의 SNAPSHOT(구조 덤프)·IGNORED — 정책 무관, 그대로 둔다
        }
        if (noDraftFingerprint) {
            repository.decide(eventId, "DETECTED",
                    "DDL 반영 정책=자동이나 타입 변경은 초안이 없어 자동 적용 불가 — 확인 후 처리 필요");
            events.record(event.schemaName(), event.tableName(), "DDL_AUTO_FALLBACK", "WARN",
                    "자동 적용 대상이나 초안 없음(타입 변경) — 확인 후 처리 필요", null);
            return;
        }
        autoApply(eventId, event, registered);
    }

    private void autoApply(long eventId, DdlEvent event, RegisteredTable registered) {
        boolean fingerprintOrigin = "FINGERPRINT".equals(event.origin());
        String ddl = fingerprintOrigin ? event.ddlText() : rewriteForTarget(event.ddlText(), registered);
        try {
            executor.execute(connections.get(registered.targetConnectionId()), ddl);
        } catch (Exception e) {
            String reason = e.getMessage() == null ? e.toString() : e.getMessage();
            repository.decide(eventId, "DETECTED", "자동 적용 실패(" + reason + ") — 확인 후 처리 필요");
            events.record(registered.schemaName(), registered.tableName(), "DDL_AUTO_FALLBACK", "WARN",
                    "DDL 자동 적용 실패 — 확인 후 처리 필요: " + firstLine(ddl), reason);
            log.warn("DDL 자동 적용 실패 ({}.{}, event={}): {}", registered.schemaName(), registered.tableName(),
                    eventId, reason);
            return;
        }
        String prefix = connections.get(registered.sourceConnectionId()).topicPrefix();
        connect.resumeAfterDdl(ConnectorNames.jdbcSink(prefix, registered.suffix()));
        String note = "DDL 자동 적용 완료: " + firstLine(ddl);
        repository.decide(eventId, "AUTO_APPLIED", note);
        events.info(registered.schemaName(), registered.tableName(), "DDL_AUTO_APPLIED", note);
    }

    /**
     * 승인 — 등록 테이블의 DDL만 가능. origin에 따라 실행 문장을 다르게 구한다:
     * SCHEMA_TOPIC은 소스 이름으로 온 DDL이라 타깃 이름으로 치환 후 실행하고,
     * FINGERPRINT는 SchemaFingerprint.draftDdl이 이미 타깃 이름으로 조립해 저장했으므로
     * 치환 없이 그대로 실행한다(치환을 또 거치면 타깃 이름 문자열을 오매칭할 위험만 있다).
     * 적용 후 해당 테이블 jdbc-sink를 재개까지 한다(FAILED면 restartFailed, PAUSED면 resume —
     * MANUAL 정책에서 DETECTED로 멈춰 있던 sink를 승인 한 번으로 되살리기 위함, 7절 개정).
     */
    public DdlEvent approve(long id) {
        DdlEvent event = pending(id);
        RegisteredTable registered = requireRegistered(event);
        boolean fingerprintOrigin = "FINGERPRINT".equals(event.origin());
        String ddl = fingerprintOrigin ? event.ddlText() : rewriteForTarget(event.ddlText(), registered);
        executor.execute(connections.get(registered.targetConnectionId()), ddl);
        String prefix = connections.get(registered.sourceConnectionId()).topicPrefix();
        connect.resumeAfterDdl(ConnectorNames.jdbcSink(prefix, registered.suffix()));
        String note = fingerprintOrigin ? "타깃에 DDL 적용 완료(스키마 지문 초안)"
                : ddl.equals(event.ddlText())
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
        String prefix = connections.get(registered.sourceConnectionId()).topicPrefix();
        String connector = ConnectorNames.jdbcSink(prefix, registered.suffix());
        connect.pause(connector);
        repository.decide(id, "REJECTED",
                "apply 정지 — " + connector + " pause. changelog는 계속 축적됨");
        events.record(registered.schemaName(), registered.tableName(), "DDL_REJECTED", "WARN",
                "DDL 거부 — apply 정지: " + firstLine(event.ddlText()), null);
        return repository.findById(id).orElseThrow();
    }

    /**
     * 건너뛰기(SKIPPED, 신규) — 타깃엔 DDL을 적용하지 않고 sink를 이어간다. ddl_text에서
     * 단순 단일 ADD/DROP COLUMN을 인식하면(DdlSkipParser) 컬럼 매핑을 조정해 재배포하고,
     * 그 외(다중 컬럼·타입 변경 등 인식 불가)는 재배포 없이 재개만 한다("타깃 타입 불일치
     * 가능" 경고를 note에 남긴다).
     */
    public DdlEvent skip(long id) {
        DdlEvent event = pending(id);
        RegisteredTable registered = requireRegistered(event);
        String note = applySkipMapping(event, registered);
        String prefix = connections.get(registered.sourceConnectionId()).topicPrefix();
        connect.resumeAfterDdl(ConnectorNames.jdbcSink(prefix, registered.suffix()));
        repository.decide(id, "SKIPPED", note);
        events.info(registered.schemaName(), registered.tableName(), "DDL_SKIPPED", note);
        return repository.findById(id).orElseThrow();
    }

    private String applySkipMapping(DdlEvent event, RegisteredTable registered) {
        Optional<DdlSkipParser.ColumnEdit> edit = DdlSkipParser.parse(event.ddlText());
        if (edit.isEmpty()) {
            return "건너뜀 — 재개만(단순 ADD/DROP COLUMN 형태가 아니라 매핑 변경 없음, 타깃 타입 불일치 가능)";
        }
        DdlSkipParser.ColumnEdit e = edit.get();
        if (e.kind() == DdlSkipParser.Kind.ADD) {
            registrationService.addDisabledColumn(registered.id(), e.column());
            return "건너뜀 — 새 컬럼 " + e.column() + "은 비활성 매핑으로 추가돼 적재에서 제외됨"
                    + "(changelog는 그대로 받으므로 나중에 활성화 가능)";
        }
        registrationService.disableColumn(registered.id(), e.column());
        return "건너뜀 — 컬럼 " + e.column() + " 매핑 비활성화(타깃 컬럼은 유지)";
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
        RegisteredTable t = findRegisteredOrNull(event);
        if (t == null) {
            throw new IllegalArgumentException(
                    "등록되지 않은 테이블의 DDL이다: " + event.schemaName() + "." + event.tableName());
        }
        return t;
    }

    /** handleNewEvent 전용 — 미등록 테이블의 이벤트는 예외 없이 조용히 건너뛴다(정책 판단 불가). */
    private RegisteredTable findRegisteredOrNull(DdlEvent event) {
        if (event.schemaName() == null || event.tableName() == null) {
            return null;
        }
        return registrations.findAll().stream()
                .filter(t -> t.schemaName().equalsIgnoreCase(event.schemaName())
                        && t.tableName().equalsIgnoreCase(event.tableName()))
                .findFirst().orElse(null);
    }
}
