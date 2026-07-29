package io.deltazium.backend.iceberg;

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/changelog")
public class ChangelogBrowserController {

    private final ChangelogBrowserService service;

    public ChangelogBrowserController(ChangelogBrowserService service) {
        this.service = service;
    }

    @GetMapping
    public List<ChangelogBrowserService.ChangelogInfo> list() {
        return service.list();
    }

    @ExceptionHandler(RuntimeException.class)
    @ResponseStatus(HttpStatus.BAD_GATEWAY)
    public Map<String, String> catalogError(RuntimeException e) {
        return Map.of("error", "changelog 카탈로그 조회 실패: " + e.getMessage());
    }
}
