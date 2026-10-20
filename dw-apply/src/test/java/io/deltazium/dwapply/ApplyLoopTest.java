package io.deltazium.dwapply;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * 파일명 : ApplyLoopTest.java
 * 작성일자 : 26. 10. 19.
 * 작성자 : 최남희
 * 설명 : 워커 루프 검증 — MockConsumer + 가짜 DatabricksClient. 전 테이블 성공 시 커밋·파일 삭제,
 * 한 테이블 실패 시 커밋 안 됨·seek 후 재시도에서 성공.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 파싱 불가 레코드 skip 카운터 상태 파일 검증
 * --------------------------------------------------
 */
class ApplyLoopTest {

    private static final String T1 = "dz1.S.A";
    private static final String T2 = "dz1.S.B";

    private static final String CONFIG = """
            {"targetId":3,"bootstrap":"x:9092","groupId":"g","host":"h","httpPath":"/p","catalog":"cat",
             "volume":"/Volumes/cat/default/v","batchMs":1000,"batchMaxBytes":1048576,"parallelism":2,
             "tables":[
              {"topics":["%s"],"schema":"default","table":"a","keyColumns":["id"],
               "columns":[{"source":"ID","target":"id"},{"source":"NAME","target":"name"}]},
              {"topics":["%s"],"schema":"default","table":"b","keyColumns":["id"],
               "columns":[{"source":"ID","target":"id"},{"source":"NAME","target":"name"}]}]}""".formatted(T1, T2);

    static class FakeClient implements DatabricksClient {
        final List<String> uploads = new CopyOnWriteArrayList<>();
        final List<String> deletes = new CopyOnWriteArrayList<>();
        final List<String> merges = new CopyOnWriteArrayList<>();
        volatile int failMergesRemaining;
        volatile String failTableContains = "`b`";

        @Override public void upload(String path, byte[] content) { uploads.add(path); }
        @Override public void delete(String path) { deletes.add(path); }
        @Override public List<String> listDir(String path) throws IOException { return List.of(); }
        @Override public void merge(String sql) throws SQLException {
            if (failMergesRemaining > 0 && sql.contains(failTableContains)) {
                failMergesRemaining--;
                throw new SQLException("boom");
            }
            merges.add(sql);
        }
        @Override public Map<String, String> describeColumns(String schema, String table) {
            return Map.of("id", "bigint", "name", "string");
        }
        @Override public void close() { }
    }

    private static String value(long id) {
        return """
                {"schema":{"fields":[{"field":"after","fields":[{"field":"ID","type":"int64"},{"field":"NAME","type":"string"}]}]},
                 "payload":{"op":"c","before":null,"after":{"ID":%d,"NAME":"n%d"}}}""".formatted(id, id);
    }

    private static MockConsumer<String, String> consumer() {
        MockConsumer<String, String> c = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        c.subscribe(List.of(T1, T2));
        TopicPartition p1 = new TopicPartition(T1, 0);
        TopicPartition p2 = new TopicPartition(T2, 0);
        c.rebalance(List.of(p1, p2));
        c.updateBeginningOffsets(Map.of(p1, 0L, p2, 0L));
        return c;
    }

    private static void feed(MockConsumer<String, String> c) {
        c.addRecord(new ConsumerRecord<>(T1, 0, 0L, "k", value(1)));
        c.addRecord(new ConsumerRecord<>(T1, 0, 1L, "k", value(2)));
        c.addRecord(new ConsumerRecord<>(T2, 0, 0L, "k", value(9)));
    }

    @Test
    void allTablesSucceedCommitsAndDeletesFiles() throws Exception {
        MockConsumer<String, String> c = consumer();
        FakeClient client = new FakeClient();
        AtomicLong now = new AtomicLong(0);
        ApplyLoop loop = new ApplyLoop(WorkerConfig.parse(new ObjectMapper().readTree(CONFIG)), c, client,
                new StatusWriter(null), now::get, ms -> { }, 10, 40);

        feed(c);
        loop.step();                    // 윈도우 시작, 아직 닫히지 않음
        assertTrue(client.merges.isEmpty());
        now.set(1500);
        loop.step();                    // batchMs 경과 → 배치 처리

        assertEquals(2, client.merges.size());
        assertEquals(2, client.uploads.size());
        assertTrue(client.uploads.contains("/Volumes/cat/default/v/default__a/dz1.S.A-p0-0-1.jsonl"));
        assertEquals(2, client.deletes.size());
        assertEquals(2L, c.committed(java.util.Set.of(new TopicPartition(T1, 0))).get(new TopicPartition(T1, 0)).offset());
        assertEquals(1L, c.committed(java.util.Set.of(new TopicPartition(T2, 0))).get(new TopicPartition(T2, 0)).offset());
    }

    @Test
    void oneTableFailureSkipsCommitSeeksBackAndRetrySucceeds() throws Exception {
        MockConsumer<String, String> c = consumer();
        FakeClient client = new FakeClient();
        client.failMergesRemaining = 1;
        AtomicLong now = new AtomicLong(0);
        List<Long> sleeps = new ArrayList<>();
        ApplyLoop loop = new ApplyLoop(WorkerConfig.parse(new ObjectMapper().readTree(CONFIG)), c, client,
                new StatusWriter(null), now::get, sleeps::add, 10, 40);

        feed(c);
        loop.step();
        now.set(1500);
        loop.step();                    // b 테이블 MERGE 실패

        TopicPartition p1 = new TopicPartition(T1, 0);
        TopicPartition p2 = new TopicPartition(T2, 0);
        Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committed =
                c.committed(java.util.Set.of(p1, p2));
        assertNull(committed.get(p1), "실패 배치는 커밋되지 않는다");
        assertNull(committed.get(p2));
        assertEquals(0L, c.position(p1), "마지막 커밋(없음 → beginning)으로 seek");
        assertEquals(0L, c.position(p2));
        assertEquals(List.of(10L), sleeps);
        assertEquals(client.uploads.size(), client.deletes.size(), "실패한 시도의 파일도 best effort로 정리");

        // 브로커가 되감긴 offset부터 다시 전달하는 상황을 재현
        feed(c);
        now.set(3000);
        loop.step();
        now.set(4500);
        loop.step();

        assertNotNull(c.committed(java.util.Set.of(p1)).get(p1));
        assertEquals(2L, c.committed(java.util.Set.of(p1)).get(p1).offset());
        assertEquals(1L, c.committed(java.util.Set.of(p2)).get(p2).offset());
        assertTrue(client.merges.stream().anyMatch(s -> s.contains("`b`")));
    }

    @Test
    void unparseableRecordIsSkippedAndCountedInStatus() throws Exception {
        MockConsumer<String, String> c = consumer();
        FakeClient client = new FakeClient();
        java.nio.file.Path status = java.nio.file.Files.createTempFile("dw-status", ".json");
        AtomicLong now = new AtomicLong(0);
        ApplyLoop loop = new ApplyLoop(WorkerConfig.parse(new ObjectMapper().readTree(CONFIG)), c, client,
                new StatusWriter(status), now::get, ms -> { }, 10, 40);

        c.addRecord(new ConsumerRecord<>(T1, 0, 0L, "k", "{not json"));
        c.addRecord(new ConsumerRecord<>(T1, 0, 1L, "k", value(2)));
        loop.step();

        var n = new ObjectMapper().readTree(status.toFile());
        assertEquals(1, n.get("skippedRecords").asLong());
        assertTrue(n.get("lastSkipReason").asText().startsWith(T1 + "-p0@0"));
        assertNotNull(n.get("lastSkipAt").asText());
    }
}
