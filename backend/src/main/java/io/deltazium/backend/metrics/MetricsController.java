package io.deltazium.backend.metrics;

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * 파일명 : MetricsController.java
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : 토픽 offset·consumer lag 실측 메트릭 REST API.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | /dashboard 추가 — 시계열 집계(DashboardService)
 * --------------------------------------------------
 */
@RestController
@RequestMapping("/api/metrics")
public class MetricsController {

    private final KafkaMetricsService metrics;
    private final DashboardService dashboard;

    public MetricsController(KafkaMetricsService metrics, DashboardService dashboard) {
        this.metrics = metrics;
        this.dashboard = dashboard;
    }

    @GetMapping("/tables")
    public List<KafkaMetricsService.TableMetrics> tables() {
        return metrics.tableMetrics();
    }

    /** 대시보드 시계열 — 처리량(발행 vs apply)·lag 추이·컴포넌트 자원(최신). */
    @GetMapping("/dashboard")
    public DashboardService.Dashboard dashboard(
            @RequestParam(defaultValue = "24") int hours) {
        return dashboard.load(hours);
    }

    @ExceptionHandler(KafkaMetricsService.MetricsException.class)
    @ResponseStatus(HttpStatus.BAD_GATEWAY)
    public Map<String, String> metricsError(KafkaMetricsService.MetricsException e) {
        return Map.of("error", e.getMessage());
    }
}
