package io.deltazium.backend.capture;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 파일명 : CaptureController.java
 * 작성일자 : 26. 08. 04.
 * 작성자 : 최남희
 * 설명 : 캡처(dz-source) 운영 REST API — 재스냅샷 상태 기계의 시작·상태·승인·재검사·취소,
 * 스냅샷 진행 상태 조회(notification 기반).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 04.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 08. 04.       | 최남희  | truncateTarget 옵션 — 타깃 비우고 완전 재구축
 * --------------------------------------------------
 * 26. 08. 05.       | 최남희  | 오케스트레이터 개편 — run/decision/recheck/cancel 추가,
 * |                          | 동기 resnapshot 호출을 상태 기계 시작으로 교체
 * --------------------------------------------------
 */
@RestController
@RequestMapping("/api/capture")
public class CaptureController {

    /** mode: INITIAL | NO_DATA. truncateTarget: 타깃 비우고 완전 재구축 (INITIAL 전용). */
    public record ResnapshotRequest(String mode, Boolean truncateTarget) {
    }

    /** choice: SYSTEM(시스템이 truncate 실행) | MANUAL(직접/DBA 실행 대기). */
    public record DecisionRequest(String choice) {
    }

    private final ResnapshotOrchestrator orchestrator;
    private final SnapshotNotificationPoller notifications;

    public CaptureController(ResnapshotOrchestrator orchestrator,
                             SnapshotNotificationPoller notifications) {
        this.orchestrator = orchestrator;
        this.notifications = notifications;
    }

    /** 재스냅샷 시작 — 이후 진행은 GET /resnapshot/run 폴링으로 관찰. */
    @PostMapping("/resnapshot")
    public ResponseEntity<Void> resnapshot(@RequestBody ResnapshotRequest req) {
        orchestrator.start(req.mode(), Boolean.TRUE.equals(req.truncateTarget()));
        return ResponseEntity.accepted().build();
    }

    /** 현재(또는 마지막) 재스냅샷 run 상태 — 없으면 204. */
    @GetMapping("/resnapshot/run")
    public ResponseEntity<ResnapshotOrchestrator.RunStatus> run() {
        ResnapshotOrchestrator.RunStatus s = orchestrator.status();
        return s == null ? ResponseEntity.noContent().build() : ResponseEntity.ok(s);
    }

    /** ③단계 승인 — truncate 실행 주체 선택. */
    @PostMapping("/resnapshot/decision")
    public ResponseEntity<Void> decision(@RequestBody DecisionRequest req) {
        orchestrator.decide(req.choice());
        return ResponseEntity.accepted().build();
    }

    /** 홀드 중 즉시 재검사. */
    @PostMapping("/resnapshot/recheck")
    public ResponseEntity<Void> recheck() {
        orchestrator.recheck();
        return ResponseEntity.accepted().build();
    }

    /** 취소 — offset 리셋 전까지만 (source resume으로 원복). */
    @PostMapping("/resnapshot/cancel")
    public ResponseEntity<Void> cancel() {
        orchestrator.cancel();
        return ResponseEntity.accepted().build();
    }

    /** 스냅샷 진행 상태 (Debezium notification 실측). */
    @GetMapping("/snapshot")
    public SnapshotNotificationPoller.SnapshotStatus snapshot() {
        return notifications.status();
    }
}
