package io.deltazium.backend.recovery;

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * 파일명 : RecoveryController.java
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : 복구 재발행 트리거·상태 조회·정합 검증 REST API.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 09. 05.       | 최남희  | 복구 진입점을 SCN에서 시각(epoch millis)으로 전환 —
 * |                          | 다중 소스·다중 타깃 ① changelog 중립 계약(architecture.md 6.2절)
 * --------------------------------------------------
 */
@RestController
@RequestMapping("/api/recovery")
public class RecoveryController {

    /** @param fromTimeMs 복구 진입 시각 (epoch millis) — ts_ms 파티션 한 칸 앞부터 재생(5.2·6.2절) */
    public record TriggerRequest(long registeredTableId, long fromTimeMs, Boolean autoResume) {
    }

    public record VerifyRequest(long registeredTableId) {
    }

    private final RecoveryService service;

    public RecoveryController(RecoveryService service) {
        this.service = service;
    }

    @GetMapping
    public List<RecoveryService.RecoveryRun> list() {
        return service.list();
    }

    @PostMapping
    public RecoveryService.RecoveryRun trigger(@RequestBody TriggerRequest req) {
        if (req.fromTimeMs() <= 0) {
            throw new IllegalArgumentException("fromTimeMs는 양수 epoch millis여야 한다: " + req.fromTimeMs());
        }
        return service.trigger(req.registeredTableId(), req.fromTimeMs(),
                Boolean.TRUE.equals(req.autoResume()));
    }

    @PostMapping("/verify")
    public RecoveryService.VerifyResult verify(@RequestBody VerifyRequest req) {
        return service.verify(req.registeredTableId());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> badRequest(IllegalArgumentException e) {
        return Map.of("error", e.getMessage());
    }

    @ExceptionHandler(IllegalStateException.class)
    @ResponseStatus(HttpStatus.BAD_GATEWAY)
    public Map<String, String> gatewayError(IllegalStateException e) {
        return Map.of("error", e.getMessage());
    }
}
