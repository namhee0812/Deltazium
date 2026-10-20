package io.deltazium.backend.dwapply;

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * 파일명 : DwApplyController.java
 * 작성일자 : 26. 10. 19.
 * 작성자 : 최남희
 * 설명 : DW apply 워커 상태 조회·재시작 API. GET /api/dw-apply는 DW 타깃 연결별
 * {targetId, targetName, running, pid, startedAt, restarts, error, status(워커 statusFile 내용)}.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@RestController
@RequestMapping("/api/dw-apply")
public class DwApplyController {

    private final DwApplyService service;

    public DwApplyController(DwApplyService service) {
        this.service = service;
    }

    @GetMapping
    public List<DwApplyService.WorkerInfo> list() {
        return service.list();
    }

    @PostMapping("/{targetId}/restart")
    public Map<String, String> restart(@PathVariable long targetId) {
        service.restart(targetId);
        return Map.of("status", "restarted");
    }

    @ExceptionHandler({IllegalArgumentException.class, IllegalStateException.class})
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> badRequest(RuntimeException e) {
        return Map.of("error", e.getMessage());
    }
}
