package io.deltazium.backend.ddl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.databind.JsonNode;
import io.deltazium.backend.connect.ConnectorNames;
import io.deltazium.backend.registration.RegisteredTable;
import io.deltazium.backend.registration.RegisteredTableRepository;
import io.deltazium.backend.registry.DbConnection;
import io.deltazium.backend.registry.DbConnectionService;
import io.deltazium.backend.registry.DbType;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
 * 26. 09. 07.       | 최남희  | PG 소스 실 배선 스모크 수정: recordChange가 ddl_text에 요약+
 * |                          | 초안을 세미콜론과 함께 섞어 넣어 승인 시 그대로 실행돼
 * |                          | ORA-00900이 났다(실측 ddl_events id=39) — ddl_text는 실행
 * |                          | 가능한 단일 문장(또는 빈 문자열)만, 요약은 note로 분리
 * --------------------------------------------------
 * 26. 09. 27.       | 최남희  | 테이블별 DDL 반영 정책(architecture.md 7절 개정) — recordChange가
 * |                          | 이벤트를 적재한 직후 DdlEventService.handleNewEvent를 불러 AUTO
 * |                          | 정책 자동 적용을 즉시 시도한다
 * --------------------------------------------------
 * 26. 09. 27.       | 최남희  | 결함 R1 수정(feature/recovery-type-hints): checkTable의 마지막
 * |                          | 레코드 읽기 로직을 CaptureTopicSchemaReader로 추출 — RecoveryService가
 * |                          | 복구 트리거 시 같은 방식으로 캡처 토픽 스키마를 읽어 논리 타입 힌트를
 * |                          | 만드는 데 재사용한다(docs/internals.md)
 * --------------------------------------------------
 */
@Component
@ConditionalOnProperty(name = "deltazium.fingerprint-poller.enabled", havingValue = "true", matchIfMissing = true)
public class SchemaFingerprintPoller {

    private static final Logger log = LoggerFactory.getLogger(SchemaFingerprintPoller.class);

    private final RegisteredTableRepository registrations;
    private final DbConnectionService connections;
    private final DdlEventRepository ddlEvents;
    private final DdlEventService ddlEventService;
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
                                   DdlEventService ddlEventService,
                                   @Value("${deltazium.kafka.bootstrap}") String bootstrap) {
        this.registrations = registrations;
        this.connections = connections;
        this.ddlEvents = ddlEvents;
        this.ddlEventService = ddlEventService;
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
        JsonNode lastValue = CaptureTopicSchemaReader.lastEnvelopeValue(c, topic).orElse(null);
        if (lastValue == null) {
            return; // 토픽이 없거나 메시지가 없음(전부 tombstone 포함) — 다음 주기에 재확인
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
        EventPayload payload = buildEventPayload(changes, target.dbType(), t.targetSchema(), t.targetTable());
        long id = ddlEvents.insertFingerprintEvent(System.currentTimeMillis(),
                t.schemaName(), t.tableName(), payload.ddlText(), payload.state(), payload.note());
        registrations.updateFingerprint(t.id(), newFingerprint, SchemaFingerprint.toJson(newFields));
        log.info("스키마 지문 변경 — {}.{} (ddl_event id={}, state={})",
                t.schemaName(), t.tableName(), id, payload.state());
        ddlEventService.handleNewEvent(id); // DDL 반영 정책(AUTO) 즉시 분기
    }

    /** ddl_events에 저장할 (ddl_text, note, state) — 순수 조립만 분리해 단위 테스트로 검증한다. */
    record EventPayload(String ddlText, String note, String state) {
    }

    /**
     * ddl_text는 승인 시 그대로 실행되는 **단일 문장**만 담는다(초안이 없으면 빈 문자열 —
     * ddl_events.ddl_text는 NOT NULL). 요약·안내 문구는 note에만 쓴다 — 2026-09-07 수정:
     * 이전엔 요약과 초안을 세미콜론과 함께 한 컬럼에 섞어 넣어 승인 시 그대로 실행되며
     * ORA-00900이 났다(실측 ddl_events id=39, 타깃 Oracle). 초안이 있으면 승인 대기
     * (DETECTED), 없으면 정보성(SNAPSHOT — 기존 DdlPanel이 승인 버튼을 숨기는 상태를 그대로
     * 재사용, 실행 가능한 DDL이 없어 승인 자체가 무의미하기 때문).
     */
    static EventPayload buildEventPayload(List<SchemaFingerprint.FieldChange> changes, String targetDbType,
                                          String targetSchema, String targetTable) {
        String draftDdl = SchemaFingerprint.draftDdl(targetDbType, targetSchema, targetTable, changes);
        String summary = SchemaFingerprint.summarize(changes);
        String ddlText = draftDdl != null ? draftDdl : "";
        String note = draftDdl != null ? summary : summary + " (자동 초안 없음 — 확인 후 수동 DDL 필요)";
        String state = draftDdl != null ? "DETECTED" : "SNAPSHOT";
        return new EventPayload(ddlText, note, state);
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
