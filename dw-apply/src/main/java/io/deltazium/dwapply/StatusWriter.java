package io.deltazium.dwapply;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 파일명 : StatusWriter.java
 * 작성일자 : 26. 10. 19.
 * 작성자 : 최남희
 * 설명 : 워커 상태 파일(statusFile) 기록 — backend가 읽어 GET /api/dw-apply로 노출한다.
 * 임시 파일에 쓰고 원자적으로 교체한다. 쓰기 실패는 WARN만 남기고 워커 동작에 영향 없음.
 * {"startedAt","lastBatchAt","lastBatchRows","lastBatchMs","lastError","lastErrorAt",
 *  "tables":{"sch.tbl":{"rows","mergeMs"}}}
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public final class StatusWriter {

    private static final Logger log = LoggerFactory.getLogger(StatusWriter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path file;
    private final String startedAt = Instant.now().toString();
    private String lastBatchAt;
    private long lastBatchRows;
    private long lastBatchMs;
    private String lastError;
    private String lastErrorAt;
    private Map<String, long[]> tables = new LinkedHashMap<>();

    public StatusWriter(Path file) {
        this.file = file;
    }

    /** @param perTable 테이블키 → {rows, mergeMs} */
    public void batchDone(long rows, long ms, Map<String, long[]> perTable) {
        lastBatchAt = Instant.now().toString();
        lastBatchRows = rows;
        lastBatchMs = ms;
        tables.putAll(perTable);
        write();
    }

    public void error(String message) {
        lastError = message;
        lastErrorAt = Instant.now().toString();
        write();
    }

    public void start() {
        write();
    }

    private void write() {
        if (file == null) {
            return;
        }
        try {
            ObjectNode n = MAPPER.createObjectNode();
            n.put("startedAt", startedAt);
            n.put("lastBatchAt", lastBatchAt);
            n.put("lastBatchRows", lastBatchRows);
            n.put("lastBatchMs", lastBatchMs);
            n.put("lastError", lastError);
            n.put("lastErrorAt", lastErrorAt);
            ObjectNode t = n.putObject("tables");
            tables.forEach((k, v) -> t.putObject(k).put("rows", v[0]).put("mergeMs", v[1]));
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            Files.createDirectories(file.toAbsolutePath().getParent());
            Files.writeString(tmp, MAPPER.writeValueAsString(n));
            Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            log.warn("상태 파일 기록 실패: {}", e.toString());
        }
    }
}
