package io.deltazium.backend.events;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 파일명 : TableEventController.java
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : 테이블별 운영 이벤트 이력 조회 REST API.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@RestController
@RequestMapping("/api/events")
public class TableEventController {

    private final TableEventService service;

    public TableEventController(TableEventService service) {
        this.service = service;
    }

    /** 전체 최근 이벤트, 또는 schema+table 지정 시 해당 테이블만. */
    @GetMapping
    public List<TableEvent> list(@RequestParam(required = false) String schema,
                                 @RequestParam(required = false) String table,
                                 @RequestParam(defaultValue = "100") int limit) {
        if (schema != null && table != null) {
            return service.byTable(schema, table, limit);
        }
        return service.recent(limit);
    }
}
