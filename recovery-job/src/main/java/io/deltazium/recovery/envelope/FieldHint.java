package io.deltazium.recovery.envelope;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 파일명 : FieldHint.java
 * 작성일자 : 26. 09. 27.
 * 작성자 : 최남희
 * 설명 : 결함 R1(재조립 envelope 논리 타입 소실, docs/experiments/2026-09-27-pg2pg-recovery-
 * rehearsal.md) 수정 — backend가 복구 트리거 시점에 캡처 토픽(또는 등록 시점 스냅샷)에서 읽은
 * 컬럼별 Debezium 논리 타입 힌트(--field-schema-file). Iceberg changelog는 이 논리 타입명을
 * 보존하지 못해(docs/experiments/2026-07-24-iceberg-sink-schema.md) 재조립만으로는 복원할 수
 * 없는 컬럼(예: PostgreSQL timestamptz → io.debezium.time.ZonedTimestamp)이 있다 — 이 힌트가
 * 있으면 ConnectJsonAssembler가 스키마 name·값 표현을 힌트대로 낸다(before/after 공통 적용,
 * source 등 다른 struct에는 적용하지 않는다). 힌트가 없는 필드는 기존 Iceberg 타입 기반 동작
 * 그대로 유지한다. JSON 형태는 backend FieldSchemaHint.toJson과의 계약 — recovery-job은
 * backend에 의존하지 않는 플레인 Java 모듈이라(CLAUDE.md) 필드 이름을 바꾸면 양쪽을 같이
 * 바꿔야 한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 27.       | 최남희  | 최초 생성 — 결함 R1 수정(feature/recovery-type-hints)
 * --------------------------------------------------
 */
public record FieldHint(String type, String name, boolean optional, Map<String, String> parameters) {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** 컬럼명 → FieldHint 맵. 파일이 없거나 형식이 깨졌으면 호출측(RecoveryJob)이 예외를 잡아
     * 힌트 없이 진행한다 — 힌트는 보조 정보일 뿐 복구 자체를 막을 이유가 아니다. */
    public static Map<String, FieldHint> load(Path file) throws IOException {
        JsonNode root = JSON.readTree(Files.readString(file));
        Map<String, FieldHint> result = new HashMap<>();
        root.properties().forEach(e -> {
            JsonNode n = e.getValue();
            Map<String, String> params = new TreeMap<>();
            n.path("parameters").properties().forEach(p -> params.put(p.getKey(), p.getValue().asText()));
            result.put(e.getKey(), new FieldHint(
                    n.path("type").asText(),
                    n.hasNonNull("name") ? n.get("name").asText() : null,
                    n.path("optional").asBoolean(true),
                    params));
        });
        return result;
    }
}
