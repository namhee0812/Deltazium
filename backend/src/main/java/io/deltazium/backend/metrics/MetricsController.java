package io.deltazium.backend.metrics;

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/metrics")
public class MetricsController {

    private final KafkaMetricsService metrics;

    public MetricsController(KafkaMetricsService metrics) {
        this.metrics = metrics;
    }

    @GetMapping("/tables")
    public List<KafkaMetricsService.TableMetrics> tables() {
        return metrics.tableMetrics();
    }

    @ExceptionHandler(KafkaMetricsService.MetricsException.class)
    @ResponseStatus(HttpStatus.BAD_GATEWAY)
    public Map<String, String> metricsError(KafkaMetricsService.MetricsException e) {
        return Map.of("error", e.getMessage());
    }
}
