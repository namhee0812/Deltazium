package io.deltazium.backend.ddl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.deltazium.backend.connect.ConnectorNames;
import io.deltazium.backend.registration.RegisteredTable;
import io.deltazium.backend.registration.RegisteredTableRepository;
import io.deltazium.backend.registry.DbConnection;
import io.deltazium.backend.registry.DbConnectionService;
import io.deltazium.backend.registry.DbType;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 파일명 : SchemaFingerprintPoller.java
 * 작성일자 : 26. 09. 07.
 * 작성자 : 최남희
 * 설명 : schema change topic이 없는 소스(PostgreSQL 등, DbType.hasSchemaChangeTopic()==false)의
 * DDL 감지 — 상주 KafkaConsumer 1개(assign, group 없음)를 스케줄러 스레드 하나에서만 써서
 * 1분마다 감시 대상 테이블 토픽의 마지막 메시지 1건(tombstone이면 최대 20건 거슬러)을 읽어
 * value.schema의 after struct 지문을 비교한다 (architecture.md 7절 개정).
 * 순수 로직(지문·diff·초안 DDL)은 SchemaFingerprint로 분리해 단위 테스트한다.
 * 다른 consumer group의 offset과 무관하고 트래픽을 다시 읽지 않는다(각 파티션 끝에서만 읽음).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 최초 생성 — 다중 소스·다중 타깃 ②
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 리뷰 반영: @PreDestroy로 backend 종료 시 consumer를 닫도록
 * |                          | 추가(누락돼 있었음) — 종료 플래그를 poll() 시작에서 확인해
 * |                          | 스케줄러 스레드와의 경합 없이 즉시 반환하게 함
 * --------------------------------------------------
 */
@Component
@ConditionalOnProperty(name = "deltazium.fingerprint-poller.enabled", havingValue = "true", matchIfMissing = true)
public class SchemaFingerprintPoller {

    private static final Logger log = LoggerFactory.getLogger(SchemaFingerprintPoller.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int MAX_BACKTRACK = 20;
    private static final long PARTITION_READ_TIMEOUT_MS = 3000;

    private final RegisteredTableRepository registrations;
    private final DbConnectionService connections;
    private final DdlEventRepository ddlEvents;
    private final String bootstrap;
    /** 상주 consumer — @Scheduled(fixedDelay)는 이전 실행이 끝나야 다음이 시작되므로
     * 항상 스케줄러의 같은 스레드 하나에서만 이 필드를 건드린다(@PreDestroy 예외). */
    private KafkaConsumer<String, String> consumer;
    /** backend 종료 신호 — poll()이 시작 시 이 값을 보고 즉시 반환해 @PreDestroy의 close()와
     * 스케줄러 스레드가 consumer를 동시에 건드리지 않게 한다. */
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    public SchemaFingerprintPoller(RegisteredTableRepository registrations,
                                   DbConnectionService connections,
                                   DdlEventRepository ddlEvents,
                                   @Value("${deltazium.kafka.bootstrap}") String bootstrap) {
        this.registrations = registrations;
        this.connections = connections;
        this.ddlEvents = ddlEvents;
        this.bootstrap = bootstrap;
    }

    @Scheduled(fixedDelayString = "${deltazium.fingerprint-poller.interval-ms:60000}",
            initialDelayString = "${deltazium.fingerprint-poller.interval-ms:60000}")
    public void poll() {
        if (shuttingDown.get()) {
            return;
        }
        List<RegisteredTable> targets = fingerprintTargets();
        if (targets.isEmpty()) {
            return;
        }
        KafkaConsumer<String, String> c = consumer();
        for (RegisteredTable t : targets) {
            try {
                checkTable(c, t);
            } catch (Exception e) {
                log.warn("스키마 지문 확인 실패 ({}.{}): {}", t.schemaName(), t.tableName(), e.getMessage());
            }
        }
    }

    /** 대상: schema change topic이 없는 DbType 소스에 속한 등록 테이블 전부. */
    private List<RegisteredTable> fingerprintTargets() {
        List<RegisteredTable> all = registrations.findAll();
        if (all.isEmpty()) {
            return List.of();
        }
        List<RegisteredTable> targets = new ArrayList<>();
        for (RegisteredTable t : all) {
            DbConnection source = connections.get(t.sourceConnectionId());
            boolean noSchemaTopic = DbType.find(source.dbType())
                    .map(dt -> !dt.hasSchemaChangeTopic()).orElse(false);
            if (noSchemaTopic) {
                targets.add(t);
            }
        }
        return targets;
    }

    private void checkTable(KafkaConsumer<String, String> c, RegisteredTable t) {
        DbConnection source = connections.get(t.sourceConnectionId());
        String topic = ConnectorNames.captureTopic(source.topicPrefix(), t.schemaName(), t.tableName());
        List<PartitionInfo> partitions = c.partitionsFor(topic);
        if (partitions == null || partitions.isEmpty()) {
            return; // 토픽이 아직 없음 — 다음 주기에 재확인
        }
        List<TopicPartition> tps = partitions.stream()
                .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
        c.assign(tps);
        Map<TopicPartition, Long> ends = c.endOffsets(tps);

        // 파티션이 여럿이면 마지막으로 읽은 것을 채택한다 — 현재 num.partitions=1 전제(운영 확인).
        // 여러 파티션의 스키마가 서로 다를 수는 없다(같은 테이블의 같은 컬럼 구조이므로).
        JsonNode lastValue = null;
        for (TopicPartition tp : tps) {
            long end = ends.getOrDefault(tp, 0L);
            if (end == 0) {
                continue; // 이 파티션엔 메시지 없음
            }
            JsonNode v = lastNonTombstone(c, tp, end);
            if (v != null) {
                lastValue = v;
            }
        }
        if (lastValue == null) {
            return;
        }

        List<SchemaFingerprint.FieldDesc> newFields = SchemaFingerprint.afterFields(lastValue);
        if (newFields.isEmpty()) {
            return; // 형식 밖 — 다음 주기에 재확인
        }
        String newFingerprint = SchemaFingerprint.hash(newFields);
        String storedFingerprint = t.schemaFingerprint();
        if (storedFingerprint == null) {
            // 첫 지문 — 이벤트 없이 저장 (TODO ②)
            registrations.updateFingerprint(t.id(), newFingerprint, SchemaFingerprint.toJson(newFields));
            return;
        }
        if (storedFingerprint.equals(newFingerprint)) {
            return;
        }
        recordChange(t, newFields, newFingerprint);
    }

    private void recordChange(RegisteredTable t, List<SchemaFingerprint.FieldDesc> newFields,
                              String newFingerprint) {
        List<SchemaFingerprint.FieldDesc> oldFields = SchemaFingerprint.fromJson(t.schemaFieldsJson());
        List<SchemaFingerprint.FieldChange> changes = SchemaFingerprint.diff(oldFields, newFields);
        if (changes.isEmpty()) {
            // 지문은 바뀌었는데 필드 목록 비교로는 차이가 안 보이는 경우(예: 이전 스냅샷 소실) —
            // 그래도 지문은 최신으로 갱신해 다음 주기부터 정상 비교되게 한다.
            registrations.updateFingerprint(t.id(), newFingerprint, SchemaFingerprint.toJson(newFields));
            return;
        }
        DbConnection target = connections.get(t.targetConnectionId());
        String draftDdl = SchemaFingerprint.draftDdl(target.dbType(), t.targetSchema(), t.targetTable(), changes);
        String summary = SchemaFingerprint.summarize(changes);
        String ddlText = draftDdl != null ? summary + " — 초안:\n" + draftDdl
                : summary + " (자동 초안 없음 — 확인 후 수동 DDL 필요)";
        // 초안이 있으면 승인 대기(DETECTED), 없으면 정보성(SNAPSHOT — 기존 DdlPanel이 승인 버튼을
        // 숨기는 상태를 그대로 재사용, 실행 가능한 DDL이 없어 승인 자체가 무의미하기 때문).
        String state = draftDdl != null ? "DETECTED" : "SNAPSHOT";
        long id = ddlEvents.insertFingerprintEvent(System.currentTimeMillis(),
                t.schemaName(), t.tableName(), ddlText, state);
        registrations.updateFingerprint(t.id(), newFingerprint, SchemaFingerprint.toJson(newFields));
        log.info("스키마 지문 변경 — {}.{} (ddl_event id={}, state={})",
                t.schemaName(), t.tableName(), id, state);
    }

    /** 파티션의 마지막 메시지부터 최대 MAX_BACKTRACK건 거슬러 tombstone이 아닌 첫 값을 찾는다. */
    private JsonNode lastNonTombstone(KafkaConsumer<String, String> c, TopicPartition tp, long endOffset) {
        for (int back = 0; back < MAX_BACKTRACK; back++) {
            long offset = endOffset - 1 - back;
            if (offset < 0) {
                return null;
            }
            c.seek(tp, offset);
            ConsumerRecord<String, String> rec = pollForOffset(c, tp, offset);
            if (rec == null) {
                return null; // 그 offset의 레코드를 못 받음 — 포기(다음 주기 재시도)
            }
            if (rec.value() != null) {
                try {
                    return JSON.readTree(rec.value());
                } catch (Exception e) {
                    return null; // 파싱 불가 — 형식 밖으로 취급
                }
            }
            // tombstone — 한 칸 더 거슬러 올라간다
        }
        return null;
    }

    /** tp를 offset에 seek한 뒤 그 레코드가 도착할 때까지 짧게 poll — 다른 파티션 레코드는 무시. */
    private ConsumerRecord<String, String> pollForOffset(KafkaConsumer<String, String> c, TopicPartition tp,
                                                          long offset) {
        long deadline = System.currentTimeMillis() + PARTITION_READ_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = c.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> rec : records) {
                if (rec.topic().equals(tp.topic()) && rec.partition() == tp.partition()
                        && rec.offset() == offset) {
                    return rec;
                }
            }
        }
        return null;
    }

    private KafkaConsumer<String, String> consumer() {
        if (consumer == null) {
            java.util.Properties props = new java.util.Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            // group.id 없음 — assign() 전용, 다른 consumer group의 offset과 무관 (7절)
            consumer = new KafkaConsumer<>(props);
        }
        return consumer;
    }

    /**
     * backend 종료 시 consumer를 닫는다. shuttingDown을 먼저 세워 poll()이 시작 시점에
     * 즉시 반환하게 한 뒤 close하므로, fixedDelay 스케줄러(다음 실행은 이전 실행이 끝나야
     * 시작됨)와 이 스레드가 consumer를 동시에 쓰지 않는다.
     */
    @PreDestroy
    void shutdown() {
        shuttingDown.set(true);
        KafkaConsumer<String, String> c = consumer;
        if (c != null) {
            c.close(Duration.ofSeconds(5));
            consumer = null;
        }
    }
}
