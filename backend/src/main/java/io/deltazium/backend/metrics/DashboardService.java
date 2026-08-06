package io.deltazium.backend.metrics;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Service;

/**
 * 파일명 : DashboardService.java
 * 작성일자 : 26. 08. 06.
 * 작성자 : 최남희
 * 설명 : 대시보드 시계열 조회 — 해상도(MIN/HOUR/DAY)·테이블(전체 합 또는 특정 토픽)
 * 파라미터를 받아 SQL 집계 결과를 화면용 시리즈로 조립한다.
 * 자원(RESOURCE)은 해상도와 무관하게 최근 MIN 샘플의 컴포넌트별 최신값.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | 해상도·테이블 파라미터 + SQL 집계(series) 기반으로 개편
 * --------------------------------------------------
 */
@Service
public class DashboardService {

    private static final Set<String> RESOLUTIONS = Set.of("MIN", "HOUR", "DAY");
    /** 해상도별 기본 조회 폭(시간) 상한 — MIN 48h 보존, HOUR 60일, DAY 1년 */
    private static final Map<String, Integer> MAX_HOURS =
            Map.of("MIN", 48, "HOUR", 24 * 60, "DAY", 24 * 365);

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

    /** @param table null 또는 "all"이면 전 테이블 합, 아니면 해당 토픽만. */
    public Dashboard load(String resolution, int hours, String table) {
        String res = resolution == null ? "MIN" : resolution.toUpperCase();
        if (!RESOLUTIONS.contains(res)) {
            throw new IllegalArgumentException("resolution은 MIN|HOUR|DAY: " + resolution);
        }
        String name = (table == null || table.isBlank() || table.equalsIgnoreCase("all"))
                ? null : table;
        LocalDateTime from = LocalDateTime.now()
                .minusHours(Math.min(Math.max(hours, 1), MAX_HOURS.get(res)));

        List<SeriesRow> rows = repository.series(res, from,
                List.of("PUBLISH", "APPLY_JDBC", "LAG_JDBC", "LAG_ICEBERG"), name);

        Map<LocalDateTime, long[]> thr = new LinkedHashMap<>(); // [publish, apply]
        Map<LocalDateTime, long[]> lag = new LinkedHashMap<>(); // [jdbc, iceberg]
        for (SeriesRow r : rows) {
            switch (r.metric()) {
                case "PUBLISH" -> bucket(thr, r.ts())[0] += r.agg();
                case "APPLY_JDBC" -> bucket(thr, r.ts())[1] += r.agg();
                case "LAG_JDBC" -> bucket(lag, r.ts())[0] += r.agg();
                case "LAG_ICEBERG" -> bucket(lag, r.ts())[1] += r.agg();
                default -> {
                }
            }
        }
        List<ThroughputPoint> throughput = new ArrayList<>();
        thr.forEach((ts, v) -> throughput.add(new ThroughputPoint(ts, v[0], v[1])));
        List<LagPoint> lags = new ArrayList<>();
        lag.forEach((ts, v) -> lags.add(new LagPoint(ts, v[0], v[1])));

        Map<String, MetricsSample> latest = new LinkedHashMap<>();
        for (MetricsSample s : repository.recentResources(LocalDateTime.now().minusMinutes(10))) {
            latest.put(s.name(), s); // 시간순 조회 — 마지막이 최신
        }
        List<ResourceNow> resources = latest.values().stream()
                .map(s -> new ResourceNow(s.name(),
                        (s.value2() == null ? 0 : s.value2()) / 10.0, s.value() / 1024))
                .toList();
        return new Dashboard(throughput, lags, resources);
    }

    private static long[] bucket(Map<LocalDateTime, long[]> map, LocalDateTime ts) {
        return map.computeIfAbsent(ts, k -> new long[2]);
    }
}
