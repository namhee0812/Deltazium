package io.deltazium.backend.capture;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.deltazium.backend.events.TableEventService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 파일명 : SnapshotNotificationPoller.java
 * 작성일자 : 26. 08. 04.
 * 작성자 : 최남희
 * 설명 : Debezium notification 토픽({prefix}-notifications) 상시 소비 —
 * Initial Snapshot의 STARTED / IN_PROGRESS / TABLE_SCAN_COMPLETED / COMPLETED / ABORTED를
 * 수신해 (1) 인메모리 진행 상태(UI 폴링용)를 갱신하고 (2) 테이블 이벤트로 적재한다.
 * 스냅샷 진행률을 offset 증가량 같은 근사치가 아니라 공식 이벤트로 추적하는 것이 목적.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 04.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@Component
@ConditionalOnProperty(name = "deltazium.notification-poller.enabled",
        havingValue = "true", matchIfMissing = true)
public class SnapshotNotificationPoller {

    private static final Logger log = LoggerFactory.getLogger(SnapshotNotificationPoller.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    /** UI 폴링용 스냅샷 진행 상태. tables: 완료 테이블 → 스캔 행수. */
    public record SnapshotStatus(String phase, String currentTable,
                                 Map<String, Long> tables, Long startedAtMs, Long completedAtMs) {
        static SnapshotStatus none() {
            return new SnapshotStatus("NONE", null, Map.of(), null, null);
        }
    }

    private final TableEventService events;
    private final String bootstrap;
    private final String topic;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<SnapshotStatus> status = new AtomicReference<>(SnapshotStatus.none());
    private KafkaConsumer<String, String> consumer;
    private Thread thread;

    public SnapshotNotificationPoller(TableEventService events,
                                      @Value("${deltazium.kafka.bootstrap}") String bootstrap,
                                      @Value("${deltazium.topic-prefix}") String topicPrefix) {
        this.events = events;
        this.bootstrap = bootstrap;
        this.topic = topicPrefix + "-notifications";
    }

    public SnapshotStatus status() {
        return status.get();
    }

    /** 재스냅샷 트리거 직후 UI가 즉시 "요청됨"을 보이도록 진행 상태를 리셋. */
    public void markRequested() {
        status.set(new SnapshotStatus("REQUESTED", null, Map.of(), System.currentTimeMillis(), null));
    }

    @PostConstruct
    void start() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "deltazium-backend-notifications");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumer = new KafkaConsumer<>(props);
        running.set(true);
        thread = new Thread(this::pollLoop, "snapshot-notification-poller");
        thread.setDaemon(true);
        thread.start();
    }

    private void pollLoop() {
        try {
            consumer.subscribe(List.of(topic));
            while (running.get()) {
                var records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> rec : records) {
                    if (rec.value() != null) {
                        handle(rec.value());
                    }
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            // 종료 경로
        } catch (Exception e) {
            log.error("notification poller 중단: {}", e.getMessage(), e);
        } finally {
            consumer.close();
        }
    }

    /** Debezium notification 한 건 처리 (형식: 공식 문서 notification.html — 방어적으로 파싱). */
    void handle(String json) {
        JsonNode n;
        try {
            n = JSON.readTree(json);
        } catch (Exception e) {
            log.debug("notification 파싱 불가 — 건너뜀: {}", json);
            return;
        }
        String aggregate = n.path("aggregate_type").asText("");
        if (!"Initial Snapshot".equalsIgnoreCase(aggregate)) {
            return; // incremental snapshot 등은 현재 범위 외 — 로그만
        }
        String type = n.path("type").asText("");
        JsonNode data = n.path("additional_data");
        long ts = n.path("timestamp").asLong(System.currentTimeMillis());
        SnapshotStatus cur = status.get();

        switch (type) {
            case "STARTED" -> {
                status.set(new SnapshotStatus("IN_PROGRESS", null, Map.of(), ts, null));
                events.info("-", "dz-source", "SNAPSHOT_STARTED", "초기 스냅샷 시작");
            }
            case "IN_PROGRESS" -> {
                String current = data.path("current_collection_in_progress").asText(null);
                status.set(new SnapshotStatus("IN_PROGRESS", current, cur.tables(),
                        cur.startedAtMs() != null ? cur.startedAtMs() : ts, null));
            }
            case "TABLE_SCAN_COMPLETED" -> {
                String table = data.path("scanned_collection").asText("?");
                long rows = data.path("total_rows_scanned").asLong(0);
                Map<String, Long> tables = new LinkedHashMap<>(cur.tables());
                tables.put(table, rows);
                status.set(new SnapshotStatus("IN_PROGRESS", null, Map.copyOf(tables),
                        cur.startedAtMs() != null ? cur.startedAtMs() : ts, null));
                events.info("-", "dz-source", "SNAPSHOT_TABLE_COMPLETED",
                        table + " 스캔 완료 (" + rows + "행, " + data.path("status").asText("?") + ")");
            }
            case "COMPLETED" -> {
                status.set(new SnapshotStatus("COMPLETED", null, cur.tables(), cur.startedAtMs(), ts));
                events.info("-", "dz-source", "SNAPSHOT_COMPLETED",
                        "초기 스냅샷 완료 — 스트리밍(go-live) 전환");
            }
            case "ABORTED" -> {
                status.set(new SnapshotStatus("ABORTED", null, cur.tables(), cur.startedAtMs(), ts));
                events.record("-", "dz-source", "SNAPSHOT_ABORTED", "WARN", "초기 스냅샷 중단", json);
            }
            default -> log.debug("미분류 notification type={} — 무시", type);
        }
    }

    @PreDestroy
    void shutdown() {
        running.set(false);
        if (consumer != null) {
            consumer.wakeup();
        }
        if (thread != null) {
            try {
                thread.join(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
