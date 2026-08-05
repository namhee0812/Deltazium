package io.deltazium.backend.capture;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import io.deltazium.backend.connect.ConnectClient;
import io.deltazium.backend.connect.ConnectorDeployService;
import io.deltazium.backend.events.TableEventService;
import io.deltazium.backend.metrics.KafkaMetricsService;
import io.deltazium.backend.registration.RegisteredTable;
import io.deltazium.backend.registration.RegistrationService;
import io.deltazium.backend.registry.DbConnection;
import io.deltazium.backend.registry.DbConnectionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 파일명 : ResnapshotOrchestrator.java
 * 작성일자 : 26. 08. 05.
 * 작성자 : 최남희
 * 설명 : 재스냅샷 상태 기계 — 등록 위저드처럼 단계를 노출하고 승인·홀드로 진행한다.
 * ① 유입 차단(source stop, offset 보존) ② sink 잔량 소진 ③ [truncate 시] 실행 주체
 * 승인 → 시스템 실행(권한 점검) 또는 직접/DBA 실행 홀드(테이블별 count=0 폴링으로 통과)
 * ④ offset 리셋 ⑤ 초기 스냅샷(notification 실측) ⑥ go-live.
 * 홀드·취소는 offset을 건드리기 전이므로 source resume만으로 원복된다.
 * 홀드가 길어져도 안전한 근거: 어차피 ④에서 offset을 버리고 전체 스냅샷을 뜨므로
 * redo/archive 보존 기간과 무관하다 (단, 홀드 기간의 changelog 공백은 감수).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 05.       | 최남희  | 최초 생성 (기존 RegistrationService.resnapshot 이관·확장)
 * --------------------------------------------------
 */
@Component
public class ResnapshotOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(ResnapshotOrchestrator.class);
    private static final String SOURCE = "dz-source";

    public enum Phase {
        STOPPING_SOURCE, DRAINING, AWAITING_DECISION, HELD, TRUNCATING,
        RESETTING, SNAPSHOTTING, DONE, FAILED, CANCELLED
    }

    /** UI 폴링용 상태 스냅샷. tableCounts: 홀드 중 타깃 테이블별 잔여 행 수. */
    public record RunStatus(Phase phase, String mode, boolean truncateTarget,
                            long remainingLag, String decision, String holdReason,
                            Map<String, Long> tableCounts, List<String> truncateSql,
                            String error, long startedAtMs, Long finishedAtMs,
                            SnapshotNotificationPoller.SnapshotStatus snapshot) {
    }

    private static class Run {
        volatile Phase phase = Phase.STOPPING_SOURCE;
        final String mode;
        final boolean truncateTarget;
        volatile long remainingLag = -1;
        volatile String decision;          // SYSTEM | MANUAL
        volatile String holdReason;
        volatile Map<String, Long> tableCounts = Map.of();
        volatile String error;
        final long startedAtMs = System.currentTimeMillis();
        volatile Long finishedAtMs;
        final AtomicBoolean cancelRequested = new AtomicBoolean(false);
        final AtomicBoolean recheckRequested = new AtomicBoolean(false);
        final List<RegisteredTable> tables;
        final DbConnection target;

        Run(String mode, boolean truncateTarget, List<RegisteredTable> tables, DbConnection target) {
            this.mode = mode;
            this.truncateTarget = truncateTarget;
            this.tables = tables;
            this.target = target;
        }

        boolean active() {
            return phase != Phase.DONE && phase != Phase.FAILED && phase != Phase.CANCELLED;
        }
    }

    private final RegistrationService registrations;
    private final DbConnectionService connections;
    private final ConnectorDeployService deploy;
    private final ConnectClient connect;
    private final KafkaMetricsService metrics;
    private final TargetTableGate gate;
    private final SnapshotNotificationPoller notifications;
    private final TableEventService events;
    private final String topicPrefix;
    private final AtomicReference<Run> current = new AtomicReference<>();

    public ResnapshotOrchestrator(RegistrationService registrations,
                                  DbConnectionService connections,
                                  ConnectorDeployService deploy,
                                  ConnectClient connect,
                                  KafkaMetricsService metrics,
                                  TargetTableGate gate,
                                  SnapshotNotificationPoller notifications,
                                  TableEventService events,
                                  @Value("${deltazium.topic-prefix}") String topicPrefix) {
        this.registrations = registrations;
        this.connections = connections;
        this.deploy = deploy;
        this.connect = connect;
        this.metrics = metrics;
        this.gate = gate;
        this.notifications = notifications;
        this.events = events;
        this.topicPrefix = topicPrefix;
    }

    /** 실행 시작 — 활성 run이 있으면 거부. */
    public synchronized void start(String mode, boolean truncateTarget) {
        Run running = current.get();
        if (running != null && running.active()) {
            throw new IllegalStateException("이미 진행 중인 재스냅샷이 있다 (단계: " + running.phase + ")");
        }
        String m = mode == null ? "INITIAL" : mode.toUpperCase(Locale.ROOT);
        if (!m.equals("INITIAL") && !m.equals("NO_DATA")) {
            throw new IllegalArgumentException("mode는 INITIAL 또는 NO_DATA여야 한다: " + mode);
        }
        if (truncateTarget && m.equals("NO_DATA")) {
            throw new IllegalArgumentException("truncate 재구축은 INITIAL에서만 의미가 있다 — "
                    + "비우고 no_data면 과거 데이터가 영영 없다");
        }
        List<RegisteredTable> tables = registrations.list();
        if (tables.isEmpty()) {
            throw new IllegalStateException("등록된 테이블이 없다 — 재스냅샷 대상 없음");
        }
        DbConnection target = connections.get(tables.get(0).targetConnectionId());
        Run run = new Run(m, truncateTarget, tables, target);
        current.set(run);
        notifications.markRequested();
        events.record("-", SOURCE, "RESNAPSHOT_REQUESTED", "INFO",
                ("INITIAL".equals(m) ? "초기 스냅샷부터 재기동" : "현재 시점(no_data)부터 재기동")
                        + (truncateTarget ? " + 타깃 truncate 재구축" : ""), null);
        Thread t = new Thread(() -> drive(run), "resnapshot-orchestrator");
        t.setDaemon(true);
        t.start();
    }

    public RunStatus status() {
        Run r = current.get();
        if (r == null) {
            return null;
        }
        return new RunStatus(r.phase, r.mode, r.truncateTarget, r.remainingLag,
                r.decision, r.holdReason, r.tableCounts,
                r.tables.stream().map(t -> "TRUNCATE TABLE " + t.targetQualified() + ";")
                        .collect(Collectors.toList()),
                r.error, r.startedAtMs, r.finishedAtMs, notifications.status());
    }

    /** ③ 실행 주체 선택 — SYSTEM(시스템이 실행) | MANUAL(직접/DBA 실행 대기). */
    public void decide(String choice) {
        Run r = requireActive();
        if (r.phase != Phase.AWAITING_DECISION) {
            throw new IllegalStateException("승인 대기 단계가 아니다 (현재: " + r.phase + ")");
        }
        String c = choice == null ? "" : choice.toUpperCase(Locale.ROOT);
        if (!c.equals("SYSTEM") && !c.equals("MANUAL")) {
            throw new IllegalArgumentException("choice는 SYSTEM 또는 MANUAL이어야 한다: " + choice);
        }
        r.decision = c;
    }

    /** 홀드 중 즉시 재검사 (기본은 10초 주기 자동). */
    public void recheck() {
        requireActive().recheckRequested.set(true);
    }

    /** 취소 — offset 리셋 전(②③)까지만 허용, source resume으로 원복. */
    public void cancel() {
        Run r = requireActive();
        if (r.phase == Phase.RESETTING || r.phase == Phase.SNAPSHOTTING) {
            throw new IllegalStateException("offset 리셋 이후에는 취소할 수 없다 — 스냅샷 완료를 기다려라");
        }
        r.cancelRequested.set(true);
    }

    private Run requireActive() {
        Run r = current.get();
        if (r == null || !r.active()) {
            throw new IllegalStateException("진행 중인 재스냅샷이 없다");
        }
        return r;
    }

    // ── 상태 기계 본체 ──────────────────────────────────────────────

    void drive(Run run) {
        try {
            // ① 유입 차단 (offset 보존 — 이 시점 이후 실패·취소는 resume으로 원복)
            deploy.stopAndAwait(SOURCE);
            events.info("-", SOURCE, "RESNAPSHOT_STEP", "① 유입 차단 — source 정지 (offset 보존)");

            if (run.truncateTarget) {
                // ② 잔량 소진 — 정지된 sink는 깨운다 (truncate 후 옛 이벤트 도착 방지)
                run.phase = Phase.DRAINING;
                for (RegisteredTable t : run.tables) {
                    deploy.resumeConnector("dz-jdbc-sink-" + t.suffix());
                }
                drain(run);
                events.info("-", SOURCE, "RESNAPSHOT_STEP", "② 파이프 잔량 소진 완료");

                // ③ 실행 주체 승인 → 실행 또는 홀드
                run.phase = Phase.AWAITING_DECISION;
                awaitDecision(run);
                if ("SYSTEM".equals(run.decision)) {
                    systemTruncate(run);
                } else {
                    run.phase = Phase.HELD;
                    run.holdReason = "직접/DBA 실행 대기 — 아래 SQL 실행 후 비워지면 자동 진행";
                }
                if (run.phase == Phase.HELD) {
                    awaitEmpty(run);
                }
                events.info("-", SOURCE, "RESNAPSHOT_STEP", "③ 타깃 비움 확인 (전 테이블 0행)");
            }

            // ④ offset 리셋 → 재배포 → 재개 (여기부터 되돌릴 수 없다)
            run.phase = Phase.RESETTING;
            deploy.deleteOffsets(SOURCE);
            registrations.redeployWithSnapshotMode(
                    "NO_DATA".equals(run.mode) ? "no_data" : "initial");
            deploy.resumeConnector(SOURCE);
            events.info("-", SOURCE, "RESNAPSHOT_STEP", "④ offset 리셋 · 재배포 · 재개");

            // ⑤ 스냅샷 → ⑥ go-live
            run.phase = Phase.SNAPSHOTTING;
            awaitSnapshotDone(run);
            run.phase = Phase.DONE;
            run.finishedAtMs = System.currentTimeMillis();
        } catch (CancelledException e) {
            rollback(run, "사용자 취소");
            run.phase = Phase.CANCELLED;
            run.finishedAtMs = System.currentTimeMillis();
        } catch (Exception e) {
            log.error("재스냅샷 실패: {}", e.getMessage(), e);
            run.error = e.getMessage();
            if (run.phase == Phase.DRAINING || run.phase == Phase.AWAITING_DECISION
                    || run.phase == Phase.HELD || run.phase == Phase.TRUNCATING
                    || run.phase == Phase.STOPPING_SOURCE) {
                rollback(run, "실패 원복: " + e.getMessage());
            } else {
                events.record("-", SOURCE, "RESNAPSHOT_FAILED", "ERROR",
                        "offset 리셋 이후 실패 — 수동 확인 필요: " + e.getMessage(), null);
            }
            run.phase = Phase.FAILED;
            run.finishedAtMs = System.currentTimeMillis();
        }
    }

    private void rollback(Run run, String why) {
        try {
            deploy.resumeConnector(SOURCE); // offset 미변경 — 이어서 스트리밍
            events.record("-", SOURCE, "RESNAPSHOT_ROLLBACK", "WARN",
                    why + " — source 재개(offset 보존)", null);
        } catch (Exception e) {
            log.error("원복 중 resume 실패: {}", e.getMessage());
        }
    }

    private void drain(Run run) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 600_000;
        while (true) {
            checkCancelled(run);
            long remaining = 0;
            for (RegisteredTable t : run.tables) {
                remaining += Math.max(0, metrics.groupLag(
                        "connect-dz-jdbc-sink-" + t.suffix(), topicPrefix + "." + t.qualified()));
            }
            run.remainingLag = remaining;
            if (remaining == 0) {
                return;
            }
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("sink 잔량 소진 대기 초과 (잔량 " + remaining
                        + "건) — sink 상태를 확인하라");
            }
            Thread.sleep(2000);
        }
    }

    private void awaitDecision(Run run) throws InterruptedException {
        while (run.decision == null) {
            checkCancelled(run);
            Thread.sleep(500);
        }
    }

    /** SYSTEM 선택: 권한 사전 점검 → 전 테이블 truncate → 검증. 권한 없으면 홀드로 전환. */
    private void systemTruncate(Run run) {
        List<String> schemas = run.tables.stream()
                .map(t -> t.targetQualified().split("\\.")[0]).distinct().toList();
        if (!gate.canTruncate(run.target, schemas)) {
            run.phase = Phase.HELD;
            run.holdReason = "권한 불충분 — 계정 " + run.target.username()
                    + "에 DROP ANY TABLE이 없고 스키마 소유자도 아니다. "
                    + "DBA에게 아래 SQL 실행을 요청하라 (비워지면 자동 진행)";
            events.record("-", SOURCE, "RESNAPSHOT_HELD", "WARN", run.holdReason, null);
            return;
        }
        run.phase = Phase.TRUNCATING;
        for (RegisteredTable t : run.tables) {
            gate.truncate(run.target, t.targetQualified());
            events.record(t.schemaName(), t.tableName(), "TARGET_TRUNCATED", "WARN",
                    t.targetQualified() + " TRUNCATE (재구축 준비)", null);
        }
    }

    /** 홀드: 테이블별 count 폴링 — 전부 0이면 통과. */
    private void awaitEmpty(Run run) throws InterruptedException {
        while (true) {
            checkCancelled(run);
            Map<String, Long> counts = new LinkedHashMap<>();
            long total = 0;
            for (RegisteredTable t : run.tables) {
                long c = gate.rowCount(run.target, t.targetQualified());
                counts.put(t.targetQualified(), c);
                total += c;
            }
            run.tableCounts = Map.copyOf(counts);
            if (total == 0) {
                return;
            }
            // 10초 주기 자동 재검사 + 즉시 재검사 요청 반영
            for (int i = 0; i < 20; i++) {
                checkCancelled(run);
                if (run.recheckRequested.getAndSet(false)) {
                    break;
                }
                Thread.sleep(500);
            }
        }
    }

    private void awaitSnapshotDone(Run run) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 1_800_000;
        while (System.currentTimeMillis() < deadline) {
            String snapPhase = notifications.status().phase();
            if ("NO_DATA".equals(run.mode)) {
                // no_data는 스냅샷 notification이 없다 — source RUNNING이면 곧 go-live
                if (isSourceHealthy()) {
                    return;
                }
            } else if ("COMPLETED".equals(snapPhase)) {
                return;
            } else if ("ABORTED".equals(snapPhase)) {
                throw new IllegalStateException("초기 스냅샷 중단(ABORTED) — 이벤트 탭 확인");
            }
            if (sourceTaskFailed()) {
                throw new IllegalStateException("재기동 후 source task FAILED — 커넥터 trace 확인");
            }
            Thread.sleep(3000);
        }
        throw new IllegalStateException("스냅샷 완료 확인 시간 초과 — 진행은 계속 중일 수 있다 (이벤트 탭 확인)");
    }

    private boolean isSourceHealthy() {
        try {
            var st = connect.status(SOURCE).path("status");
            var node = st.isMissingNode() ? connect.status(SOURCE) : st;
            if (!"RUNNING".equals(node.path("connector").path("state").asText())) {
                return false;
            }
            for (var task : node.path("tasks")) {
                if (!"RUNNING".equals(task.path("state").asText())) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean sourceTaskFailed() {
        try {
            var st = connect.status(SOURCE).path("status");
            var node = st.isMissingNode() ? connect.status(SOURCE) : st;
            for (var task : node.path("tasks")) {
                if ("FAILED".equals(task.path("state").asText())) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            return false;
        }
    }

    private void checkCancelled(Run run) {
        if (run.cancelRequested.get()) {
            throw new CancelledException();
        }
    }

    private static class CancelledException extends RuntimeException {
    }
}
