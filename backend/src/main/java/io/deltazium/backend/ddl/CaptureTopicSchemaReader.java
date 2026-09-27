package io.deltazium.backend.ddl;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/**
 * 파일명 : CaptureTopicSchemaReader.java
 * 작성일자 : 26. 09. 27.
 * 작성자 : 최남희
 * 설명 : 캡처 토픽(&lt;prefix&gt;.&lt;schema&gt;.&lt;table&gt;)의 **마지막 레코드 1건**
 * (tombstone이면 최대 20건 거슬러)을 읽어 Debezium envelope JSON(value)을 돌려준다.
 * SchemaFingerprintPoller가 스키마 지문 비교에 쓰던 읽기 로직(assign 기반, consumer group
 * 미사용, 트래픽 재구독 없음)을 결함 R1(복구 재조립 논리 타입 힌트, architecture.md 6.2절,
 * docs/experiments/2026-09-27-pg2pg-recovery-rehearsal.md)에서도 그대로 써야 해서 공용
 * 헬퍼로 뽑았다 — 호출측이 각자의 용도에 맞게 KafkaConsumer를 만들어 넘긴다(상주 소비자인지
 * 트리거 시점 단발 소비자인지는 호출측 책임).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 27.       | 최남희  | 최초 생성 — SchemaFingerprintPoller.checkTable의 마지막 레코드
 * |                          | 읽기 로직 추출(결함 R1 수정, feature/recovery-type-hints)
 * --------------------------------------------------
 */
public final class CaptureTopicSchemaReader {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int MAX_BACKTRACK = 20;
    private static final long PARTITION_READ_TIMEOUT_MS = 3000;

    private CaptureTopicSchemaReader() {
    }

    /**
     * 파티션이 여럿이면 마지막으로 읽은 것을 채택한다 — 같은 테이블의 스키마는 파티션마다
     * 다를 수 없다는 전제(SchemaFingerprintPoller와 동일 전제, num.partitions=1 운영 확인).
     * @return 토픽이 없거나 메시지가 없으면(전부 tombstone 포함) empty.
     */
    public static Optional<JsonNode> lastEnvelopeValue(KafkaConsumer<String, String> c, String topic) {
        List<PartitionInfo> partitions = c.partitionsFor(topic);
        if (partitions == null || partitions.isEmpty()) {
            return Optional.empty();
        }
        List<TopicPartition> tps = partitions.stream()
                .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
        c.assign(tps);
        Map<TopicPartition, Long> ends = c.endOffsets(tps);

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
        return Optional.ofNullable(lastValue);
    }

    /** 파티션의 마지막 메시지부터 최대 MAX_BACKTRACK건 거슬러 tombstone이 아닌 첫 값을 찾는다. */
    private static JsonNode lastNonTombstone(KafkaConsumer<String, String> c, TopicPartition tp, long endOffset) {
        for (int back = 0; back < MAX_BACKTRACK; back++) {
            long offset = endOffset - 1 - back;
            if (offset < 0) {
                return null;
            }
            c.seek(tp, offset);
            ConsumerRecord<String, String> rec = pollForOffset(c, tp, offset);
            if (rec == null) {
                return null; // 그 offset의 레코드를 못 받음 — 포기(호출측이 다음 기회에 재시도)
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
    private static ConsumerRecord<String, String> pollForOffset(KafkaConsumer<String, String> c, TopicPartition tp,
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
}
