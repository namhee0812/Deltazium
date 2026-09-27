package io.deltazium.backend.recovery;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.deltazium.backend.ddl.SchemaFingerprint;

/**
 * 파일명 : FieldSchemaHint.java
 * 작성일자 : 26. 09. 27.
 * 작성자 : 최남희
 * 설명 : 결함 R1(재조립 envelope 논리 타입 소실, docs/experiments/2026-09-27-pg2pg-recovery-
 * rehearsal.md) 수정 — 복구 트리거 시점에 얻은 컬럼 스키마(SchemaFingerprint.FieldDesc, 캡처
 * 토픽 마지막 레코드 또는 registered_tables.schema_fields_json 폴백)를 recovery-job
 * (--field-schema-file)에 넘길 JSON으로 직렬화한다. FieldDesc.type()은 "rawType" 또는
 * "rawType:logicalName" 형태(SchemaFingerprint.afterFields)라 콜론으로 나눠 recovery-job이
 * 바로 쓸 수 있는 {type, name, optional, parameters} 구조로 편다.
 * recovery-job은 backend에 의존하지 않는 별도 모듈이라(플레인 Java, CLAUDE.md) 이 JSON이 두
 * 모듈 사이의 유일한 계약이다 — 필드 이름·구조를 바꾸면 recovery-job의
 * io.deltazium.recovery.envelope.FieldHint 로더도 함께 바꿔야 한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 27.       | 최남희  | 최초 생성 — 결함 R1 수정(feature/recovery-type-hints)
 * --------------------------------------------------
 */
public final class FieldSchemaHint {

    private static final ObjectMapper JSON = new ObjectMapper();

    private FieldSchemaHint() {
    }

    /** 컬럼명 → {type, name?, optional, parameters} 맵을 JSON 문자열로. before/after 공통 적용
     * (recovery-job의 ConnectJsonAssembler가 두 struct 모두에 같은 힌트를 쓴다). */
    public static String toJson(List<SchemaFingerprint.FieldDesc> fields) {
        ObjectNode root = JSON.createObjectNode();
        for (SchemaFingerprint.FieldDesc f : fields) {
            ObjectNode n = root.putObject(f.name());
            String rawType = f.type();
            String logicalName = null;
            int colon = f.type().indexOf(':');
            if (colon >= 0) {
                rawType = f.type().substring(0, colon);
                logicalName = f.type().substring(colon + 1);
            }
            n.put("type", rawType);
            if (logicalName != null) {
                n.put("name", logicalName);
            }
            n.put("optional", f.optional());
            ObjectNode params = n.putObject("parameters");
            Map<String, String> sorted = new TreeMap<>(f.parameters());
            sorted.forEach(params::put);
        }
        return root.toString();
    }
}
