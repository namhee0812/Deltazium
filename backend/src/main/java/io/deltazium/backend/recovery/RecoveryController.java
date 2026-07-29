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

@RestController
@RequestMapping("/api/recovery")
public class RecoveryController {

    public record TriggerRequest(long registeredTableId, long fromScn) {
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
        return service.trigger(req.registeredTableId(), req.fromScn());
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
