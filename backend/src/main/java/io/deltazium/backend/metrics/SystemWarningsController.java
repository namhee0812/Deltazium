package io.deltazium.backend.metrics;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
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
}
