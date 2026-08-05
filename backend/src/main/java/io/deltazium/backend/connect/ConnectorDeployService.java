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
 * 파일명 : ConnectorDeployService.java
 * 작성일자 : 26. 07. 24.
 * 작성자 : 최남희
 * 설명 : 커넥터 템플릿 렌더링 → Connect 배포. connectors/*.json.tmpl만 배포 가능 —
 * 임의 설정 JSON을 받는 경로를 두지 않아 커넥터 구성이 항상 템플릿을 거치게 강제한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 24.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 08. 04.       | 최남희  | stopAndAwait·deleteOffsets 분리 — truncate 재구축이
 * |                          | 정지와 offset 삭제 사이에 lag 소진·truncate를 끼울 수 있게
 * --------------------------------------------------
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

    /** 커넥터 삭제 (데이터는 무관 — Connect 설정만). 없으면 예외가 나므로 호출측이 무시 처리. */
    public void deleteConnector(String name) {
        connect.delete(name);
    }

    public void pauseConnector(String name) {
        connect.pause(name);
    }

    /**
     * stop → offset 삭제 (STOPPED 전이 대기 포함). 삭제 직전의 커넥터에 사용 —
     * offset이 남으면 같은 이름 재생성 시 스냅샷이 SKIP된다.
     */
    public void stopAndResetOffsets(String name) {
        stopAndAwait(name);
        connect.deleteOffsets(name);
    }

    /** stop 후 STOPPED 전이 대기 — offset은 건드리지 않는다 (실패 시 resume으로 원복 가능). */
    public void stopAndAwait(String name) {
        connect.stop(name);
        for (int i = 0; i < 15; i++) {
            try {
                String state = connect.status(name).path("connector").path("state").asText();
                if ("STOPPED".equals(state)) {
                    break;
                }
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    public void deleteOffsets(String name) {
        connect.deleteOffsets(name);
    }

    public void resumeConnector(String name) {
        connect.resume(name);
    }

    private JsonNode parse(String rendered) {
        try {
            return json.readTree(rendered);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("렌더링 결과가 JSON이 아니다", e);
        }
    }
}
