package io.deltazium.dwapply;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 파일명 : WorkerConfig.java
 * 작성일자 : 26. 10. 19.
 * 작성자 : 최남희
 * 설명 : 워커 설정 JSON(기동 인자 config=경로) 파싱. 토큰은 여기에 없다 — 환경변수
 * DZ_DATABRICKS_TOKEN으로만 받는다(ps 노출 방지). 검증 실패는 IllegalArgumentException이며
 * 워커는 exit 2로 종료한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public record WorkerConfig(long targetId, String bootstrap, String groupId, String host,
                           String httpPath, String catalog, String volume, long batchMs,
                           long batchMaxBytes, int parallelism, Path statusFile,
                           List<TableSpec> tables) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static WorkerConfig load(Path file) throws IOException {
        return parse(MAPPER.readTree(Files.readString(file)));
    }

    public static WorkerConfig parse(JsonNode n) {
        List<TableSpec> tables = new ArrayList<>();
        for (JsonNode t : n.path("tables")) {
            List<String> topics = strings(t.path("topics"));
            List<String> keys = strings(t.path("keyColumns"));
            List<ColumnMapping> cols = new ArrayList<>();
            for (JsonNode c : t.path("columns")) {
                cols.add(new ColumnMapping(req(c, "source"), req(c, "target")));
            }
            TableSpec spec = new TableSpec(topics, req(t, "schema"), req(t, "table"), keys, cols);
            if (topics.isEmpty() || keys.isEmpty() || cols.isEmpty()) {
                throw new IllegalArgumentException("테이블 설정 불완전 (topics/keyColumns/columns): " + spec.key());
            }
            for (String k : keys) {
                if (cols.stream().noneMatch(c -> c.target().equalsIgnoreCase(k))) {
                    throw new IllegalArgumentException("키 컬럼이 컬럼 매핑에 없음: " + spec.key() + "." + k);
                }
            }
            tables.add(spec);
        }
        if (tables.isEmpty()) {
            throw new IllegalArgumentException("tables 비어 있음");
        }
        String volume = req(n, "volume");
        return new WorkerConfig(n.path("targetId").asLong(), req(n, "bootstrap"), req(n, "groupId"),
                req(n, "host"), req(n, "httpPath"), req(n, "catalog"),
                volume.endsWith("/") ? volume.substring(0, volume.length() - 1) : volume,
                n.path("batchMs").asLong(10_000), n.path("batchMaxBytes").asLong(64L * 1024 * 1024),
                Math.max(1, n.path("parallelism").asInt(4)),
                n.hasNonNull("statusFile") ? Path.of(n.get("statusFile").asText()) : null,
                List.copyOf(tables));
    }

    /** 토픽 → 테이블 매핑. 한 토픽이 두 테이블에 걸리면 설정 오류. */
    public Map<String, TableSpec> topicIndex() {
        Map<String, TableSpec> m = new HashMap<>();
        for (TableSpec t : tables) {
            for (String topic : t.topics()) {
                if (m.put(topic, t) != null) {
                    throw new IllegalArgumentException("토픽이 둘 이상의 테이블에 매핑됨: " + topic);
                }
            }
        }
        return m;
    }

    public List<String> allTopics() {
        return tables.stream().flatMap(t -> t.topics().stream()).toList();
    }

    private static String req(JsonNode n, String f) {
        if (!n.hasNonNull(f) || n.get(f).asText().isBlank()) {
            throw new IllegalArgumentException("필수 설정 누락: " + f);
        }
        return n.get(f).asText();
    }

    private static List<String> strings(JsonNode arr) {
        List<String> r = new ArrayList<>();
        arr.forEach(x -> r.add(x.asText()));
        return r;
    }
}
