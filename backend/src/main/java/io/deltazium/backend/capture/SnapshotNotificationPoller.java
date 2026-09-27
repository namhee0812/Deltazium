package io.deltazium.backend.capture;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.deltazium.backend.connect.ConnectorNames;
import io.deltazium.backend.events.TableEventService;
import io.deltazium.backend.registry.DbConnectionService;
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
 * 26. 08. 04.       | 최남희  | {schema,payload} 봉투 언랩 (워커 JSON converter가
 * |                          | schemas.enabled=true — 최상위에서 찾다 전부 무시하던 버그),
 * |                          | auto.offset.reset earliest (첫 스냅샷 STARTED 유실 방지)
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ②: 전역 topic-prefix 제거 — 시작 시점의
 * |                          | SOURCE 커넥션 전체의 notification 토픽(<prefix>-notifications)을
 * |                          | 구독한다. 상태는 여전히 전역 하나(SnapshotStatus) — 소스별 구분은
 * |                          | 하지 않는다(재스냅샷이 단일 소스 전제라 지금은 충분, TODO ②)
 * --------------------------------------------------
 * 26. 09. 27.       | 최남희  | 상태를 전역 AtomicReference에서 소스별(topicPrefix별) Map으로
 * |                          | 전환 — 재스냅샷 중 lag 경고 제외(KafkaMetricsService.
 * |                          | snapshotInProgress)가 소스 단위로 정확해야 하기 때문. status()는
 * |                          | status(topicPrefix)로, GET /api/capture/snapshot은 overview()
 * |                          | (기존 필드 + bySource)로 확장했다 (docs/internals.md)
 * --------------------------------------------------
 */
@Component
@ConditionalOnProperty(name = "deltazium.notification-poller.enabled",
        havingValue = "true", matchIfMissing = true)
public class SnapshotNotificationPoller {

    private static final Logger log = LoggerFactory.getLogger(SnapshotNotificationPoller.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    /** 소스(topicPrefix) 하나의 스냅샷 진행 상태. tables: 완료 테이블 → 스캔 행수. */
    public record SnapshotStatus(String phase, String currentTable,
                                 Map<String, Long> tables, Long startedAtMs, Long completedAtMs) {
        static SnapshotStatus none() {
            return new SnapshotStatus("NONE", null, Map.of(), null, null);
        }
    }

    /** GET /api/capture/snapshot 응답 — 기존 필드(phase 등)는 호환을 위해 남기고, 대표 소스
     *  하나의 값을 그대로 노출한다(재스냅샷은 아직 단일 소스 전제, TODO ②). bySource가 소스별
     *  실제 phase다. */
    public record SnapshotOverview(String phase, String currentTable, Map<String, Long> tables,
                                   Long startedAtMs, Long completedAtMs,
                                   Map<String, SnapshotStatus> bySource) {
    }

    private final TableEventService events;
    private final DbConnectionService connections;
    private final String bootstrap;
    private final AtomicBoolean running = new AtomicBoolean(false);
    /** topicPrefix(소스 식별자) → 그 소스의 스냅샷 진행 상태. 소스마다 notification 토픽이
     *  독립이라 상태도 소스별로 분리한다(2026-09-27, 재스냅샷 중 lag 경고 제외). */
    private final Map<String, SnapshotStatus> statuses = new ConcurrentHashMap<>();
    private KafkaConsumer<String, String> consumer;
    private Thread thread;

    public SnapshotNotificationPoller(TableEventService events,
                                      DbConnectionService connections,
                                      @Value("${deltazium.kafka.bootstrap}") String bootstrap) {
        this.events = events;
        this.connections = connections;
        this.bootstrap = bootstrap;
    }

    /** 시작 시점의 SOURCE 커넥션 전체의 notification 토픽. 새 소스는 backend 재기동 후 반영. */
    private List<String> notificationTopics() {
        return connections.list().stream()
                .filter(c -> "SOURCE".equals(c.role()) && c.topicPrefix() != null)
                .map(c -> ConnectorNames.notificationTopic(c.topicPrefix()))
                .distinct()
                .toList();
    }

    /** 소스(topicPrefix) 하나의 스냅샷 상태 — 없으면 NONE. */
    public SnapshotStatus status(String topicPrefix) {
        return statuses.getOrDefault(topicPrefix, SnapshotStatus.none());
    }

    /** 소스별 상태 전체 — KafkaMetricsService(snapshotInProgress 산출)·CaptureController(overview) 용. */
    public Map<String, SnapshotStatus> byPrefix() {
        return Map.copyOf(statuses);
    }

    /** GET /api/capture/snapshot 응답 조립. */
    public SnapshotOverview overview() {
        SnapshotStatus primary = statuses.values().stream()
                .filter(s -> !"NONE".equals(s.phase()))
                .findFirst()
                .orElse(SnapshotStatus.none());
        return new SnapshotOverview(primary.phase(), primary.currentTable(), primary.tables(),
                primary.startedAtMs(), primary.completedAtMs(), byPrefix());
    }

    /** 재스냅샷 트리거 직후 UI가 즉시 "요청됨"을 보이도록 해당 소스의 진행 상태를 리셋. */
    public void markRequested(String topicPrefix) {
        statuses.put(topicPrefix, new SnapshotStatus("REQUESTED", null, Map.of(), System.currentTimeMillis(), null));
    }

    @PostConstruct
    void start() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "deltazium-backend-notifications");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // earliest: 토픽이 구독 후에 생성되는 첫 스냅샷에서 STARTED를 놓치지 않기 위함
        // (커밋 이후로는 커밋 offset이 우선이라 재처리 없음)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumer = new KafkaConsumer<>(props);
        running.set(true);
        List<String> topics = notificationTopics();
        thread = new Thread(() -> pollLoop(topics), "snapshot-notification-poller");
        thread.setDaemon(true);
        thread.start();
    }

    private void pollLoop(List<String> topics) {
        try {
            if (topics.isEmpty()) {
                log.info("SOURCE 커넥션 없음 — snapshot notification poller 대기 상태로 시작");
                while (running.get()) {
                    Thread.sleep(2000);
                }
                return;
            }
            consumer.subscribe(topics);
            while (running.get()) {
                var records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> rec : records) {
                    if (rec.value() != null) {
                        handle(rec.topic(), rec.value());
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

    /** Debezium notification 한 건 처리 (형식: 공식 문서 notification.html — 방어적으로 파싱).
     *  topic에서 소스(topicPrefix)를 복원해 그 소스만의 상태를 갱신한다. */
    void handle(String topic, String json) {
        JsonNode n;
        try {
            n = JSON.readTree(json);
        } catch (Exception e) {
            log.debug("notification 파싱 불가 — 건너뜀: {}", json);
            return;
        }
        // 워커가 JSON converter(schemas.enabled=true)라 {schema, payload} 봉투에 싸여 온다
        if (n.has("payload") && n.get("payload").isObject()) {
            n = n.get("payload");
        }
        String aggregate = n.path("aggregate_type").asText("");
        if (!"Initial Snapshot".equalsIgnoreCase(aggregate)) {
            return; // incremental snapshot 등은 현재 범위 외 — 로그만
        }
        String prefix = ConnectorNames.topicPrefixFromNotificationTopic(topic);
        String sourceConnector = ConnectorNames.source(prefix);
        String type = n.path("type").asText("");
        JsonNode data = n.path("additional_data");
        long ts = n.path("timestamp").asLong(System.currentTimeMillis());
        SnapshotStatus cur = statuses.getOrDefault(prefix, SnapshotStatus.none());

        switch (type) {
            case "STARTED" -> {
                statuses.put(prefix, new SnapshotStatus("IN_PROGRESS", null, Map.of(), ts, null));
                events.info("-", sourceConnector, "SNAPSHOT_STARTED", "초기 스냅샷 시작");
            }
            case "IN_PROGRESS" -> {
                String current = data.path("current_collection_in_progress").asText(null);
                statuses.put(prefix, new SnapshotStatus("IN_PROGRESS", current, cur.tables(),
                        cur.startedAtMs() != null ? cur.startedAtMs() : ts, null));
            }
            case "TABLE_SCAN_COMPLETED" -> {
                String table = data.path("scanned_collection").asText("?");
                long rows = data.path("total_rows_scanned").asLong(0);
                Map<String, Long> tables = new LinkedHashMap<>(cur.tables());
                tables.put(table, rows);
                statuses.put(prefix, new SnapshotStatus("IN_PROGRESS", null, Map.copyOf(tables),
                        cur.startedAtMs() != null ? cur.startedAtMs() : ts, null));
                events.info("-", sourceConnector, "SNAPSHOT_TABLE_COMPLETED",
                        table + " 스캔 완료 (" + rows + "행, " + data.path("status").asText("?") + ")");
            }
            case "COMPLETED" -> {
                statuses.put(prefix, new SnapshotStatus("COMPLETED", null, cur.tables(), cur.startedAtMs(), ts));
                events.info("-", sourceConnector, "SNAPSHOT_COMPLETED",
                        "초기 스냅샷 완료 — 스트리밍(go-live) 전환");
            }
            case "ABORTED" -> {
                statuses.put(prefix, new SnapshotStatus("ABORTED", null, cur.tables(), cur.startedAtMs(), ts));
                events.record("-", sourceConnector, "SNAPSHOT_ABORTED", "WARN", "초기 스냅샷 중단", json);
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
