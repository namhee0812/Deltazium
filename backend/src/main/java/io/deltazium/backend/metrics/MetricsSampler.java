package io.deltazium.backend.metrics;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 파일명 : MetricsSampler.java
 * 작성일자 : 26. 08. 06.
 * 작성자 : 최남희
 * 설명 : 대시보드 시계열 수집기 (1분 주기 → metrics_samples, 7일 보존).
 * - 처리량: 토픽 end offset·apply offset(= end - lag)의 분당 델타 (첫 샘플은 기준선만)
 * - lag: jdbc·iceberg sink의 시점 lag
 * - 자원: pid 파일(~/deltazium-runtime/pids) + 자기 자신의 /proc CPU%·RSS
 *   (JMX 대신 최소 비용 수집 — Prometheus/JMX exporter는 TODO.md 백로그)
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@Component
@ConditionalOnProperty(name = "deltazium.metrics-sampler.enabled",
        havingValue = "true", matchIfMissing = true)
public class MetricsSampler {

    private static final Logger log = LoggerFactory.getLogger(MetricsSampler.class);
    private static final long CLOCK_TICKS_PER_SEC = 100; // Linux 기본 HZ

    private final MetricsSampleRepository repository;
    private final KafkaMetricsService metrics;
    private final Path pidsDir;

    /** 델타 계산용 직전 관측값: key → 누적값 (topic:publish, topic:jdbc, topic:iceberg, pid ticks) */
    private final Map<String, Long> lastValues = new HashMap<>();
    private long lastSampleAtMs;

    public MetricsSampler(MetricsSampleRepository repository,
                          KafkaMetricsService metrics,
                          @Value("${deltazium.pids-dir:${user.home}/deltazium-runtime/pids}") String pidsDir) {
        this.repository = repository;
        this.metrics = metrics;
        this.pidsDir = Path.of(pidsDir);
    }

    @Scheduled(fixedDelayString = "${deltazium.metrics-sampler.interval-ms:60000}")
    public void sample() {
        LocalDateTime now = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);
        try {
            sampleThroughputAndLag(now);
        } catch (Exception e) {
            log.debug("처리량 샘플 실패: {}", e.getMessage());
        }
        try {
            sampleResources(now);
        } catch (Exception e) {
            log.debug("자원 샘플 실패: {}", e.getMessage());
        }
        try {
            repository.deleteBefore(now.minusDays(7));
        } catch (Exception e) {
            log.debug("보존 정리 실패: {}", e.getMessage());
        }
        lastSampleAtMs = System.currentTimeMillis();
    }

    private void sampleThroughputAndLag(LocalDateTime now) {
        for (KafkaMetricsService.TableMetrics t : metrics.tableMetrics()) {
            long publish = t.totalEvents();
            long jdbcApplied = publish - Math.max(0, t.jdbcLag());
            long icebergApplied = publish - Math.max(0, t.icebergLag());
            delta(now, "PUBLISH", t.topic(), publish);
            delta(now, "APPLY_JDBC", t.topic(), jdbcApplied);
            delta(now, "APPLY_ICEBERG", t.topic(), icebergApplied);
            repository.insert(now, "LAG_JDBC", t.topic(), Math.max(0, t.jdbcLag()), null);
            repository.insert(now, "LAG_ICEBERG", t.topic(), Math.max(0, t.icebergLag()), null);
        }
    }

    /** 누적값의 증가분을 기록 — 첫 관측(기준선)과 리셋(재스냅샷 등으로 감소)은 건너뜀. */
    private void delta(LocalDateTime now, String metric, String name, long cumulative) {
        Long prev = lastValues.put(metric + ":" + name, cumulative);
        if (prev != null && cumulative >= prev) {
            repository.insert(now, metric, name, cumulative - prev, null);
        }
    }

    private void sampleResources(LocalDateTime now) {
        Map<String, Long> pids = new HashMap<>();
        if (Files.isDirectory(pidsDir)) {
            try (Stream<Path> files = Files.list(pidsDir)) {
                files.filter(p -> p.getFileName().toString().endsWith(".pid")).forEach(p -> {
                    try {
                        String name = p.getFileName().toString().replace(".pid", "");
                        pids.put(name, Long.parseLong(Files.readString(p).strip()));
                    } catch (Exception ignored) {
                        // 손상된 pid 파일은 건너뜀
                    }
                });
            } catch (Exception ignored) {
                // pids 디렉터리 조회 실패 — 자기 자신만 수집
            }
        }
        pids.put("backend", ProcessHandle.current().pid()); // IntelliJ 실행 시 pid 파일이 없어도 자신은 수집

        long elapsedMs = lastSampleAtMs == 0 ? 0 : System.currentTimeMillis() - lastSampleAtMs;
        for (Map.Entry<String, Long> e : pids.entrySet()) {
            try {
                Path proc = Path.of("/proc/" + e.getValue());
                if (!Files.isDirectory(proc)) {
                    continue; // 죽은 프로세스의 잔존 pid 파일
                }
                long rssKb = ProcReader.parseVmRssKb(Files.readString(proc.resolve("status")));
                long ticks = ProcReader.parseCpuTicks(Files.readString(proc.resolve("stat")));
                Long prevTicks = lastValues.put("ticks:" + e.getKey() + ":" + e.getValue(), ticks);
                long cpuPctX10 = 0;
                if (prevTicks != null && ticks >= prevTicks && elapsedMs > 0) {
                    double cpuSec = (ticks - prevTicks) / (double) CLOCK_TICKS_PER_SEC;
                    cpuPctX10 = Math.round(cpuSec / (elapsedMs / 1000.0) * 100 * 10);
                }
                if (rssKb >= 0) {
                    repository.insert(now, "RESOURCE", e.getKey(), rssKb, cpuPctX10);
                }
            } catch (Exception ex) {
                log.debug("자원 수집 실패({}): {}", e.getKey(), ex.getMessage());
            }
        }
    }
}
