package io.deltazium.backend.capture;

import io.deltazium.backend.registration.RegistrationService;
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
 * 설명 : 캡처(dz-source) 운영 REST API — 재스냅샷 트리거(상시 운영 액션 + 장애 복구 진입점 겸용),
 * 스냅샷 진행 상태 조회(notification 기반, UI 배너 폴링용).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 04.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@RestController
@RequestMapping("/api/capture")
public class CaptureController {

    public record ResnapshotRequest(String mode) { // INITIAL | NO_DATA
    }

    private final RegistrationService registrations;
    private final SnapshotNotificationPoller notifications;

    public CaptureController(RegistrationService registrations,
                             SnapshotNotificationPoller notifications) {
        this.registrations = registrations;
        this.notifications = notifications;
    }

    /** 재스냅샷 — stop → offset 리셋 → snapshot.mode 재배포 → 재개. 멱등 upsert 전제라 비파괴. */
    @PostMapping("/resnapshot")
    public ResponseEntity<Void> resnapshot(@RequestBody ResnapshotRequest req) {
        notifications.markRequested();
        registrations.resnapshot(req.mode());
        return ResponseEntity.accepted().build();
    }

    /** 스냅샷 진행 상태 (Debezium notification 실측). */
    @GetMapping("/snapshot")
    public SnapshotNotificationPoller.SnapshotStatus snapshot() {
        return notifications.status();
    }
}
