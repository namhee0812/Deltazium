package io.deltazium.backend.registry;

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * 파일명 : DbConnectionController.java
 * 작성일자 : 26. 07. 25.
 * 작성자 : 최남희
 * 설명 : DB 연결 저장소 REST API — 등록·연결 테스트·지원 DB 목록.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 25.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@RestController
@RequestMapping("/api/connections")
public class DbConnectionController {

    private final DbConnectionService service;

    public DbConnectionController(DbConnectionService service) {
        this.service = service;
    }

    @GetMapping
    public List<DbConnection> list() {
        return service.list();
    }

    /** UI 선택 목록용 — 지원되는 DB 종류만 (현재 Oracle뿐, 확장 시 DbType에 추가). */
    @GetMapping("/db-types")
    public List<Map<String, String>> dbTypes() {
        return DbType.supportedTypes().stream()
                .map(t -> Map.of("code", t.name(), "label", t.label()))
                .toList();
    }

    @GetMapping("/{id}")
    public DbConnection get(@PathVariable long id) {
        return service.get(id);
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public DbConnection create(@RequestBody DbConnection c) {
        return service.create(c);
    }

    @PutMapping("/{id}")
    public DbConnection update(@PathVariable long id, @RequestBody DbConnection c) {
        return service.update(id, c);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable long id) {
        service.delete(id);
        return ResponseEntity.noContent().build();
    }

    /** 등록 전 연결 테스트 (저장 안 함). */
    @PostMapping("/test")
    public OracleConnectionTester.Result test(@RequestBody DbConnection c) {
        return service.test(c);
    }

    /** 저장된 연결로 테스트. */
    @PostMapping("/{id}/test")
    public OracleConnectionTester.Result testSaved(@PathVariable long id) {
        return service.testSaved(id);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> badRequest(IllegalArgumentException e) {
        return Map.of("error", e.getMessage());
    }

    @ExceptionHandler(DbConnectionService.NotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Map<String, String> notFound(DbConnectionService.NotFoundException e) {
        return Map.of("error", e.getMessage());
    }
}
