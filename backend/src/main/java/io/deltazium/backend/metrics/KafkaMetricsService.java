package io.deltazium.backend.metrics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import io.deltazium.backend.registration.RegisteredTable;
import io.deltazium.backend.registration.RegisteredTableRepository;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * 테이블별 실측 지표 — 토픽 end offset(총 이벤트), offset 증가율(이벤트/s),
 * sink별 consumer lag. Kafka AdminClient만 사용 (JMX 불필요).
 */
@Service
public class KafkaMetricsService {

    public record TableMetrics(
            String schemaName,
            String tableName,
            String topic,
            long totalEvents,
            double eventsPerSec,
            long jdbcLag,
            long icebergLag) {
    }

    private record Sample(long endOffset, long atMillis) {
    }

    static final String JDBC_GROUP = "connect-dz-jdbc-sink";
    static final String ICEBERG_GROUP = "connect-dz-iceberg-sink";

    private final RegisteredTableRepository registrations;
    private final String bootstrap;
    private final String topicPrefix;
    private final Map<String, Sample> lastSamples = new ConcurrentHashMap<>();
    private volatile AdminClient admin;

    public KafkaMetricsService(RegisteredTableRepository registrations,
                               @Value("${deltazium.kafka.bootstrap}") String bootstrap,
                               @Value("${deltazium.topic-prefix}") String topicPrefix) {
        this.registrations = registrations;
        this.bootstrap = bootstrap;
        this.topicPrefix = topicPrefix;
    }

    public List<TableMetrics> tableMetrics() {
        List<RegisteredTable> tables = registrations.findAll();
        if (tables.isEmpty()) {
            return List.of();
        }
        Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
        for (RegisteredTable t : tables) {
            latest.put(new TopicPartition(topic(t), 0), OffsetSpec.latest());
        }
        try {
            AdminClient client = admin();
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends =
                    client.listOffsets(latest).all().get();
            Map<TopicPartition, OffsetAndMetadata> jdbc =
                    client.listConsumerGroupOffsets(JDBC_GROUP).partitionsToOffsetAndMetadata().get();
            Map<TopicPartition, OffsetAndMetadata> iceberg =
                    client.listConsumerGroupOffsets(ICEBERG_GROUP).partitionsToOffsetAndMetadata().get();

            long now = System.currentTimeMillis();
            return tables.stream().map(t -> {
                TopicPartition tp = new TopicPartition(topic(t), 0);
                long end = ends.containsKey(tp) ? ends.get(tp).offset() : 0L;
                double rate = rate(tp.topic(), end, now);
                return new TableMetrics(
                        t.schemaName(), t.tableName(), tp.topic(), end, rate,
                        lag(end, jdbc.get(tp)), lag(end, iceberg.get(tp)));
            }).toList();
        } catch (ExecutionException e) {
            throw new MetricsException("Kafka 지표 조회 실패: " + e.getCause().getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MetricsException("Kafka 지표 조회 중단", e);
        }
    }

    private String topic(RegisteredTable t) {
        return topicPrefix + "." + t.qualified();
    }

    /** 직전 조회와의 offset 증가율. 첫 조회거나 시간차가 없으면 0. */
    private double rate(String topic, long endOffset, long now) {
        Sample prev = lastSamples.put(topic, new Sample(endOffset, now));
        if (prev == null || now <= prev.atMillis() || endOffset < prev.endOffset()) {
            return 0.0;
        }
        return (endOffset - prev.endOffset()) * 1000.0 / (now - prev.atMillis());
    }

    private static long lag(long end, OffsetAndMetadata committed) {
        return committed == null ? end : Math.max(0, end - committed.offset());
    }

    private AdminClient admin() {
        AdminClient a = admin;
        if (a == null) {
            synchronized (this) {
                if (admin == null) {
                    Properties props = new Properties();
                    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
                    props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
                    admin = AdminClient.create(props);
                }
                a = admin;
            }
        }
        return a;
    }

    @PreDestroy
    void close() {
        if (admin != null) {
            admin.close();
        }
    }

    public static class MetricsException extends RuntimeException {
        public MetricsException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
