package io.deltazium.backend.ddl;

import java.util.List;

import io.deltazium.backend.connect.ConnectClient;
import io.deltazium.backend.registration.RegisteredTable;
import io.deltazium.backend.registration.RegisteredTableRepository;
import io.deltazium.backend.registry.DbConnectionService;
import org.springframework.stereotype.Service;

/**
 * DDL 승인 워크플로 (architecture.md 7절).
 * 승인: 타깃에 DDL 적용 → 해당 테이블 CDC 계속.
 * 거부: jdbc-sink 구독에서 해당 테이블 토픽 제외 → apply만 정지 (changelog는 계속 축적).
 */
@Service
public class DdlEventService {

    private final DdlEventRepository repository;
    private final RegisteredTableRepository registrations;
    private final DbConnectionService connections;
    private final TargetDdlExecutor executor;
    private final ConnectClient connect;

    public DdlEventService(DdlEventRepository repository,
                           RegisteredTableRepository registrations,
                           DbConnectionService connections,
                           TargetDdlExecutor executor,
                           ConnectClient connect) {
        this.repository = repository;
        this.registrations = registrations;
        this.connections = connections;
        this.executor = executor;
        this.connect = connect;
    }

    public List<DdlEvent> list() {
        return repository.findAll();
    }

    /** 승인 — 등록 테이블의 DDL만 가능. 타깃 실행 성공 시에만 상태 변경. */
    public DdlEvent approve(long id) {
        DdlEvent event = pending(id);
        RegisteredTable registered = requireRegistered(event);
        executor.execute(connections.get(registered.targetConnectionId()), event.ddlText());
        repository.decide(id, "APPROVED", "타깃에 DDL 적용 완료");
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
        return repository.findById(id).orElseThrow();
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
