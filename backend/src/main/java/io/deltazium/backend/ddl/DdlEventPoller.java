package io.deltazium.backend.ddl;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import io.deltazium.backend.registry.DbConnection;
import io.deltazium.backend.registry.DbConnectionService;
import io.deltazium.backend.registry.DbType;
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
 * 파일명 : DdlEventPoller.java
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : schema change topic(들) 상시 소비 → ddl_events 적재 (origin=SCHEMA_TOPIC).
 * 토픽 retention(24h)이 지나면 이벤트가 사라지므로 DB 적재가 원본 보존 수단이다 (실측 확인).
 * schema change topic을 발행하지 않는 소스(PostgreSQL 등)는 SchemaFingerprintService가 담당한다
 * (architecture.md 7절 개정, DbType.hasSchemaChangeTopic()로 분기).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ②: 전역 topic-prefix 제거 — 시작 시점의
 * |                          | schema change topic 보유 소스(SOURCE 역할 + DbType.hasSchemaChangeTopic)
 * |                          | 전체를 구독한다. 소스는 그 토픽 이름 자체가 topic.prefix다.
 * |                          | (한계) 시작 후 추가된 소스는 backend 재기동 전까지 구독되지 않는다 —
 * |                          | 다른 poller들(SnapshotNotificationPoller 등)과 같은 기존 패턴.
 * --------------------------------------------------
 */
@Component
@ConditionalOnProperty(name = "deltazium.ddl-poller.enabled", havingValue = "true", matchIfMissing = true)
public class DdlEventPoller {

    private static final Logger log = LoggerFactory.getLogger(DdlEventPoller.class);

    private final DdlEventRepository repository;
    private final DbConnectionService connections;
    private final String bootstrap;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private KafkaConsumer<String, String> consumer;
    private Thread thread;

    public DdlEventPoller(DdlEventRepository repository,
                          DbConnectionService connections,
                          @Value("${deltazium.kafka.bootstrap}") String bootstrap) {
        this.repository = repository;
        this.connections = connections;
        this.bootstrap = bootstrap;
    }

    /** schema change topic을 발행하는 소스들의 topic.prefix = 토픽 이름 자체 (4절). */
    private List<String> schemaChangeTopics() {
        return connections.list().stream()
                .filter(c -> "SOURCE".equals(c.role()) && c.topicPrefix() != null)
                .filter(c -> DbType.find(c.dbType()).map(DbType::hasSchemaChangeTopic).orElse(false))
                .map(DbConnection::topicPrefix)
                .distinct()
                .toList();
    }

    @PostConstruct
    void start() {
        List<String> topics = schemaChangeTopics();
        if (topics.isEmpty()) {
            log.info("schema change topic 발행 소스 없음 — DDL poller 대기 상태로 시작 (등록 후 backend 재기동 필요)");
        }
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "deltazium-backend-ddl");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumer = new KafkaConsumer<>(props);
        running.set(true);
        thread = new Thread(() -> pollLoop(topics), "ddl-event-poller");
        thread.setDaemon(true);
        thread.start();
    }

    private void pollLoop(List<String> topics) {
        try {
            if (topics.isEmpty()) {
                // 구독 대상이 없다 — wakeup만 기다린다 (빈 subscribe는 poll이 즉시 빈 배치를 반환)
                while (running.get()) {
                    Thread.sleep(2000);
                }
                return;
            }
            consumer.subscribe(topics);
            while (running.get()) {
                var records = consumer.poll(Duration.ofSeconds(2));
                boolean stored = false;
                for (ConsumerRecord<String, String> rec : records) {
                    if (rec.value() == null) {
                        continue;
                    }
                    var parsed = DdlEventParser.parse(rec.value());
                    if (parsed.isEmpty()) {
                        log.debug("DDL 이벤트 파싱 불가 — offset {} 건너뜀", rec.offset());
                        continue;
                    }
                    var p = parsed.get();
                    String state = p.snapshot() ? "SNAPSHOT"
                            : DdlEventParser.ignorable(p.ddl()) ? "IGNORED" : "DETECTED";
                    stored |= repository.insertIfAbsent(rec.offset(), p.tsMs(), p.scn(),
                            p.schemaName(), p.tableName(), p.ddl(), state);
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                    if (stored) {
                        log.info("DDL 이벤트 {}건 적재", records.count());
                    }
                }
            }
        } catch (WakeupException e) {
            // 종료 경로
        } catch (Exception e) {
            log.error("DDL poller 중단: {}", e.getMessage(), e);
        } finally {
            consumer.close();
        }
    }

    @PreDestroy
    void stop() {
        running.set(false);
        if (consumer != null) {
            consumer.wakeup();
        }
        if (thread != null) {
            try {
                thread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
