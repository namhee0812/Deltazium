package io.deltazium.backend.ddl;

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

@RestController
@RequestMapping("/api/ddl-events")
public class DdlEventController {

    private final DdlEventService service;

    public DdlEventController(DdlEventService service) {
        this.service = service;
    }

    @GetMapping
    public List<DdlEvent> list() {
        return service.list();
    }

    @PostMapping("/{id}/approve")
    public DdlEvent approve(@PathVariable long id) {
        return service.approve(id);
    }

    @PostMapping("/{id}/reject")
    public DdlEvent reject(@PathVariable long id) {
        return service.reject(id);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> badRequest(IllegalArgumentException e) {
        return Map.of("error", e.getMessage());
    }

    @ExceptionHandler(IllegalStateException.class)
    @ResponseStatus(HttpStatus.BAD_GATEWAY)
    public Map<String, String> executionError(IllegalStateException e) {
        return Map.of("error", e.getMessage());
    }
}
