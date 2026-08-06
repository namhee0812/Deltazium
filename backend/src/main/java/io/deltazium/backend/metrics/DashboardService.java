package io.deltazium.backend.metrics;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

/**
 * 파일명 : DashboardService.java
 * 작성일자 : 26. 08. 06.
 * 작성자 : 최남희
 * 설명 : 대시보드 시계열 조회 — metrics_samples를 분 단위 시점별로 집계한다.
 * 처리량(발행 vs jdbc apply, 전 테이블 합)·lag 추이·컴포넌트 자원(최신값).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@Service
public class DashboardService {

    public record ThroughputPoint(LocalDateTime ts, long publish, long apply) {
    }

    public record LagPoint(LocalDateTime ts, long jdbc, long iceberg) {
    }

    public record ResourceNow(String component, double cpuPct, long rssMb) {
    }

    public record Dashboard(List<ThroughputPoint> throughput, List<LagPoint> lag,
                            List<ResourceNow> resources) {
    }

    private final MetricsSampleRepository repository;

    public DashboardService(MetricsSampleRepository repository) {
        this.repository = repository;
    }

    public Dashboard load(int hours) {
        List<MetricsSample> rows = repository.findSince(
                LocalDateTime.now().minusHours(Math.min(Math.max(hours, 1), 168)));

        Map<LocalDateTime, long[]> thr = new LinkedHashMap<>(); // [publish, apply]
        Map<LocalDateTime, long[]> lag = new LinkedHashMap<>(); // [jdbc, iceberg]
        Map<String, MetricsSample> latestResource = new LinkedHashMap<>();

        for (MetricsSample s : rows) {
            switch (s.metric()) {
                case "PUBLISH" -> bucket(thr, s.sampledAt())[0] += s.value();
                case "APPLY_JDBC" -> bucket(thr, s.sampledAt())[1] += s.value();
                case "LAG_JDBC" -> bucket(lag, s.sampledAt())[0] += s.value();
                case "LAG_ICEBERG" -> bucket(lag, s.sampledAt())[1] += s.value();
                case "RESOURCE" -> latestResource.put(s.name(), s); // findSince가 시간순 — 마지막이 최신
                default -> {
                    // APPLY_ICEBERG 등은 현재 차트 미사용 — 보존만
                }
            }
        }

        List<ThroughputPoint> throughput = new ArrayList<>();
        thr.forEach((ts, v) -> throughput.add(new ThroughputPoint(ts, v[0], v[1])));
        List<LagPoint> lags = new ArrayList<>();
        lag.forEach((ts, v) -> lags.add(new LagPoint(ts, v[0], v[1])));
        List<ResourceNow> resources = latestResource.values().stream()
                .map(s -> new ResourceNow(s.name(),
                        (s.value2() == null ? 0 : s.value2()) / 10.0, s.value() / 1024))
                .toList();
        return new Dashboard(throughput, lags, resources);
    }

    private static long[] bucket(Map<LocalDateTime, long[]> map, LocalDateTime ts) {
        return map.computeIfAbsent(ts, k -> new long[2]);
    }
}
