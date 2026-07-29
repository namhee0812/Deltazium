package io.deltazium.backend.registration;

import java.util.List;
import java.util.Map;

import io.deltazium.backend.dictionary.OracleDictionaryService;
import io.deltazium.backend.dictionary.SourceTableInfo;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/registrations")
public class RegistrationController {

    public record TablesRequest(long sourceConnectionId, List<String> tables) {
    }

    public record RegisterRequest(long sourceConnectionId, long targetConnectionId,
                                  List<RegistrationService.TableSpec> tables) {
    }

    private final RegistrationService service;

    public RegistrationController(RegistrationService service) {
        this.service = service;
    }

    @GetMapping
    public List<RegisteredTable> list() {
        return service.list();
    }

    /** 소스 딕셔너리 조회 — pattern 예: CDC.* / CDC.TEST_% / CDC.T1 */
    @GetMapping("/discover/{sourceConnectionId}")
    public List<SourceTableInfo> discover(@PathVariable long sourceConnectionId,
                                          @RequestParam String pattern) {
        return service.discover(sourceConnectionId, pattern);
    }

    /** 컬럼 목록 (소스/타깃 연결 공용) — 매핑 화면용. table=SCHEMA.TABLE */
    @GetMapping("/columns/{connectionId}")
    public List<io.deltazium.backend.dictionary.TableColumn> columns(
            @PathVariable long connectionId, @RequestParam String table) {
        return service.columns(connectionId, table);
    }

    /** DB 레벨 사전 점검 (ARCHIVELOG · DB supplemental logging). */
    @GetMapping("/db-checks/{sourceConnectionId}")
    public Map<String, String> databaseChecks(@PathVariable long sourceConnectionId) {
        return service.databaseChecks(sourceConnectionId);
    }

    /** LogMiner 권한 점검 — 권한명 → 보유 여부. */
    @GetMapping("/privilege-checks/{sourceConnectionId}")
    public Map<String, Boolean> privilegeChecks(@PathVariable long sourceConnectionId) {
        return service.privilegeChecks(sourceConnectionId);
    }

    /** supp.log(ALL) 적용 — UI에서 "적용하겠습니까?" YES를 받은 뒤에만 호출된다. */
    @PostMapping("/supplemental-logging")
    public Map<String, String> applySupplementalLogging(@RequestBody TablesRequest req) {
        return service.applySupplementalLogging(req.sourceConnectionId(), req.tables());
    }

    /** 등록 확정 + source·jdbc-sink 배포. */
    @PostMapping
    public List<RegisteredTable> register(@RequestBody RegisterRequest req) {
        return service.register(req.sourceConnectionId(), req.targetConnectionId(), req.tables());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> badRequest(IllegalArgumentException e) {
        return Map.of("error", e.getMessage());
    }

    @ExceptionHandler(OracleDictionaryService.DictionaryException.class)
    @ResponseStatus(HttpStatus.BAD_GATEWAY)
    public Map<String, String> dictionaryError(OracleDictionaryService.DictionaryException e) {
        return Map.of("error", e.getMessage());
    }
}
