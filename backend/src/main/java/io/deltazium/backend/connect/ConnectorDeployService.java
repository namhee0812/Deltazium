package io.deltazium.backend.connect;

import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.deltazium.backend.template.TemplateRenderer;
import org.springframework.stereotype.Service;

/**
 * 커넥터 템플릿 렌더링 → Connect 배포. connectors/*.json.tmpl만 배포 가능 —
 * 임의 설정 JSON을 받는 경로를 두지 않아 커넥터 구성이 항상 템플릿을 거치게 강제한다.
 */
@Service
public class ConnectorDeployService {

    /** 배포 허용 템플릿 (connectors/ 디렉터리와 1:1). */
    private static final Set<String> TEMPLATES = Set.of(
            "source", "jdbc-sink", "iceberg-sink", "recovery-sink");

    private final TemplateRenderer renderer;
    private final ConnectClient connect;
    private final ObjectMapper json = new ObjectMapper();

    public ConnectorDeployService(TemplateRenderer renderer, ConnectClient connect) {
        this.renderer = renderer;
        this.connect = connect;
    }

    /**
     * @param template TEMPLATES 중 하나
     * @param vars     템플릿 placeholder 값 (connector_name 필수)
     * @return Connect 응답 (배포된 커넥터 정보)
     */
    public JsonNode deploy(String template, Map<String, String> vars) {
        return deploy(template, vars, Map.of());
    }

    /**
     * extraConfig: 템플릿의 고정 placeholder로 표현할 수 없는 동적 키
     * (예: iceberg-sink의 테이블별 route-regex). 렌더링된 config 위에 병합된다.
     */
    public JsonNode deploy(String template, Map<String, String> vars, Map<String, String> extraConfig) {
        if (!TEMPLATES.contains(template)) {
            throw new IllegalArgumentException("알 수 없는 템플릿: " + template);
        }
        String name = vars.get("connector_name");
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("connector_name은 필수다");
        }
        String rendered = renderer.render(template, vars);
        ObjectNode config = (ObjectNode) parse(rendered).get("config");
        extraConfig.forEach(config::put);
        return connect.upsert(name, config);
    }

    private JsonNode parse(String rendered) {
        try {
            return json.readTree(rendered);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("렌더링 결과가 JSON이 아니다", e);
        }
    }
}
