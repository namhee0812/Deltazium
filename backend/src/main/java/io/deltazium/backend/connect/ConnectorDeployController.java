package io.deltazium.backend.connect;

import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * 파일명 : ConnectorDeployController.java
 * 작성일자 : 26. 07. 24.
 * 작성자 : 최남희
 * 설명 : 커넥터 템플릿 렌더링·배포 REST API.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 24.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@RestController
public class ConnectorDeployController {

    private final ConnectorDeployService deployService;

    public ConnectorDeployController(ConnectorDeployService deployService) {
        this.deployService = deployService;
    }

    /** body = 템플릿 placeholder 값 맵. 예: POST /api/deploy/source {"connector_name": "dz-source", ...} */
    @PostMapping("/api/deploy/{template}")
    public JsonNode deploy(@PathVariable String template, @RequestBody Map<String, String> vars) {
        return deployService.deploy(template, vars);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> badRequest(IllegalArgumentException e) {
        return Map.of("error", e.getMessage());
    }
}
