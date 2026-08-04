package io.deltazium.backend.events;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 파일명 : TableEventService.java
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : 테이블별 운영 이벤트 기록·조회. record()는 절대 호출측 흐름을 깨지 않는다 —
 * 이벤트 기록 실패가 본 작업(등록·승인·복구)을 실패시키면 안 된다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@Service
public class TableEventService {

    private static final Logger log = LoggerFactory.getLogger(TableEventService.class);

    private final TableEventRepository repository;

    public TableEventService(TableEventRepository repository) {
        this.repository = repository;
    }

    public void record(String schema, String table, String eventType, String severity,
                       String message, String detail) {
        try {
            repository.insertEvent(schema, table, eventType, severity, message, detail);
        } catch (Exception e) {
            log.warn("테이블 이벤트 기록 실패 ({} {}.{}): {}", eventType, schema, table, e.getMessage());
        }
    }

    public void info(String schema, String table, String eventType, String message) {
        record(schema, table, eventType, "INFO", message, null);
    }

    public List<TableEvent> recent(int limit) {
        return repository.findRecent(limit);
    }

    public List<TableEvent> byTable(String schema, String table, int limit) {
        return repository.findByTable(schema, table, limit);
    }
}
