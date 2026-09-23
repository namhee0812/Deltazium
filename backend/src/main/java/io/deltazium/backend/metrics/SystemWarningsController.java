package io.deltazium.backend.metrics;

import java.util.Map;
import java.util.NoSuchElementException;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * 파일명 : SystemWarningsController.java
 * 작성일자 : 26. 08. 24.
 * 작성자 : 최남희
 * 설명 : 전역 경고 센터 REST API — GET /api/system/warnings.
 * UI 헤더 경고 칩이 30초 주기로 폴링한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 24.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 09. 23.       | 최남희  | POST /api/system/warnings/{id}/ack 추가 (INFO 알림 확인) —
 * |                          | INFO 아니면 400, 존재하지 않으면 404
 * --------------------------------------------------
 */
@RestController
@RequestMapping("/api/system")
public class SystemWarningsController {

    private final SystemWarningService warnings;

    public SystemWarningsController(SystemWarningService warnings) {
        this.warnings = warnings;
    }

    @GetMapping("/warnings")
    public SystemWarningService.SystemWarningsResponse warnings() {
        return warnings.warnings();
    }

    @PostMapping("/warnings/{id}/ack")
    public void ack(@PathVariable String id) {
        warnings.ack(id);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> badRequest(IllegalArgumentException e) {
        return Map.of("error", e.getMessage());
    }

    @ExceptionHandler(NoSuchElementException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Map<String, String> notFound(NoSuchElementException e) {
        return Map.of("error", e.getMessage());
    }
}
