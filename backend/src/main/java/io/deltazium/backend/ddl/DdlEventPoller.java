package io.deltazium.backend.ddl;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

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
 * schema change topic(topic.prefix) 상시 소비 → ddl_events 적재.
 * 토픽 retention(24h)이 지나면 이벤트가 사라지므로 DB 적재가 원본 보존 수단이다 (실측 확인).
 */
@Component
@ConditionalOnProperty(name = "deltazium.ddl-poller.enabled", havingValue = "true", matchIfMissing = true)
public class DdlEventPoller {

    private static final Logger log = LoggerFactory.getLogger(DdlEventPoller.class);

    private final DdlEventRepository repository;
    private final String bootstrap;
    private final String topic;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private KafkaConsumer<String, String> consumer;
    private Thread thread;

    public DdlEventPoller(DdlEventRepository repository,
                          @Value("${deltazium.kafka.bootstrap}") String bootstrap,
                          @Value("${deltazium.topic-prefix}") String topicPrefix) {
        this.repository = repository;
        this.bootstrap = bootstrap;
        this.topic = topicPrefix;
    }

    @PostConstruct
    void start() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "deltazium-backend-ddl");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumer = new KafkaConsumer<>(props);
        running.set(true);
        thread = new Thread(this::pollLoop, "ddl-event-poller");
        thread.setDaemon(true);
        thread.start();
    }

    private void pollLoop() {
        try {
            consumer.subscribe(List.of(topic));
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
