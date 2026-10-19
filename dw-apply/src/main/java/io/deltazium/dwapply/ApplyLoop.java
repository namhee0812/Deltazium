package io.deltazium.dwapply;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 파일명 : ApplyLoop.java
 * 작성일자 : 26. 10. 19.
 * 작성자 : 최남희
 * 설명 : 워커 메인 루프(단일 스레드, 수동 커밋). poll → 테이블별 메모리 버퍼 누적 → 배치 닫힘
 * 조건(BatchPolicy) 도달 시 테이블별 (파일 쓰기 → MERGE)를 스레드 풀에서 병렬 실행 → 전 테이블
 * 성공이면 commitSync, 하나라도 실패하면 커밋 없이 각 파티션을 마지막 커밋 offset(없으면
 * beginning)으로 seek하고 버퍼를 비운 뒤 지수 백오프(5초→60초) 후 계속한다. 워커는 죽지
 * 않는다. 배치 처리가 루프 안에서 동기이므로 "이전 배치 완료 후 다음 배치"가 자동 성립하고,
 * 부하 시 처리 중 쌓인 만큼 다음 배치가 커지는 자기 조절이 된다. at-least-once — 재전달은
 * MERGE의 PK별 1건 선별과 키 매칭이 흡수한다. 같은 키가 서로 다른 파티션에 흩어지는 경우(단일
 * 파티션 토픽 가정 위반)는 범위 밖.
 * 변경이 없는 poll(skip된 레코드만)은 DB 작업 없이 offset만 커밋한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public final class ApplyLoop {

    /** 백오프 사이 대기 — 테스트에서 가짜로 교체. */
    @FunctionalInterface
    public interface Sleeper {
        void sleep(long ms) throws InterruptedException;
    }

    private static final Logger log = LoggerFactory.getLogger(ApplyLoop.class);

    private final WorkerConfig cfg;
    private final Map<String, TableSpec> topicIndex;
    private final Consumer<String, String> consumer;
    private final DatabricksClient client;
    private final Flattener flattener = new Flattener();
    private final StatusWriter status;
    private final LongSupplier clock;
    private final Sleeper sleeper;
    private final long backoffInitMs;
    private final long backoffMaxMs;
    private final ExecutorService pool;

    /** 타깃 컬럼 타입 캐시(테이블키 → 컬럼명(소문자) → 타입). MERGE 실패 시 해당 테이블만 무효화. */
    private final Map<String, Map<String, String>> columnTypes = new ConcurrentHashMap<>();
    private final Map<String, List<FlatRow>> buffers = new LinkedHashMap<>();
    private long bufferedBytes;
    private boolean pending;
    private long windowStartMs;
    private long backoffMs;

    public ApplyLoop(WorkerConfig cfg, Consumer<String, String> consumer, DatabricksClient client,
                     StatusWriter status, LongSupplier clock, Sleeper sleeper,
                     long backoffInitMs, long backoffMaxMs) {
        this.cfg = cfg;
        this.topicIndex = cfg.topicIndex();
        this.consumer = consumer;
        this.client = client;
        this.status = status;
        this.clock = clock;
        this.sleeper = sleeper;
        this.backoffInitMs = backoffInitMs;
        this.backoffMaxMs = backoffMaxMs;
        this.backoffMs = backoffInitMs;
        this.pool = Executors.newFixedThreadPool(cfg.parallelism(), r -> {
            Thread t = new Thread(r, "dw-apply-merge");
            t.setDaemon(true);
            return t;
        });
    }

    public void run(BooleanSupplier running) {
        try {
            while (running.getAsBoolean()) {
                step();
            }
        } catch (WakeupException e) {
            // 종료 신호
        } finally {
            pool.shutdownNow();
        }
    }

    /** 루프 1회 — poll, 버퍼 누적, 필요 시 배치 처리. 테스트가 직접 호출한다. */
    public void step() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
        for (ConsumerRecord<String, String> r : records) {
            if (!pending) {
                pending = true;
                windowStartMs = clock.getAsLong();
            }
            TableSpec spec = topicIndex.get(r.topic());
            if (spec == null) {
                continue;
            }
            try {
                flattener.flatten(spec, r.topic(), r.partition(), r.offset(), r.value()).ifPresent(row -> {
                    buffers.computeIfAbsent(spec.key(), k -> new ArrayList<>()).add(row);
                    bufferedBytes += row.bytes();
                });
            } catch (Exception e) {
                // 파싱 불가 레코드는 건너뛴다 — 재시도로 해결되지 않아 배치를 영원히 막기 때문
                log.error("envelope 파싱 실패, 건너뜀 topic={} partition={} offset={}: {}",
                        r.topic(), r.partition(), r.offset(), e.toString());
            }
        }
        if (pending && BatchPolicy.shouldClose(windowStartMs, clock.getAsLong(), bufferedBytes,
                cfg.batchMs(), cfg.batchMaxBytes())) {
            closeBatch();
        }
    }

    private void closeBatch() {
        if (buffers.isEmpty()) {
            // 변경 없음(skip만) — DB 작업 없이 offset만 전진
            try {
                consumer.commitSync();
            } catch (RuntimeException e) {
                log.warn("offset 커밋 실패(변경 없는 배치): {}", e.toString());
            }
            reset();
            return;
        }
        long t0 = clock.getAsLong();
        Map<String, List<FlatRow>> snapshot = new LinkedHashMap<>(buffers);
        Map<String, Future<TableResult>> futures = new LinkedHashMap<>();
        Map<String, List<String>> uploaded = new HashMap<>();
        for (var e : snapshot.entrySet()) {
            TableSpec spec = specByKey(e.getKey());
            List<String> up = java.util.Collections.synchronizedList(new ArrayList<>());
            uploaded.put(e.getKey(), up);
            Callable<TableResult> task = () -> applyTable(spec, e.getValue(), up);
            futures.put(e.getKey(), pool.submit(task));
        }
        Map<String, long[]> perTable = new LinkedHashMap<>();
        List<String> failed = new ArrayList<>();
        Exception firstError = null;
        for (var e : futures.entrySet()) {
            try {
                TableResult r = e.getValue().get();
                perTable.put(e.getKey(), new long[] {r.rows(), r.mergeMs()});
            } catch (Exception ex) {
                Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                failed.add(e.getKey());
                if (firstError == null) {
                    firstError = cause instanceof Exception ce ? ce : ex;
                }
                log.error("배치 실패 table={}: {}", e.getKey(), cause.toString());
            }
        }
        if (failed.isEmpty()) {
            try {
                consumer.commitSync();
            } catch (RuntimeException ex) {
                failed.add("(offset commit)");
                firstError = ex;
                log.error("offset 커밋 실패: {}", ex.toString());
            }
        }
        if (failed.isEmpty()) {
            long ms = clock.getAsLong() - t0;
            long rows = perTable.values().stream().mapToLong(v -> v[0]).sum();
            uploaded.values().forEach(this::deleteBestEffort);
            status.batchDone(rows, ms, perTable);
            log.info("배치 완료 테이블={} 행={} 소요={}ms", perTable.size(), rows, ms);
            backoffMs = backoffInitMs;
            reset();
            return;
        }
        // 실패: 커밋하지 않고 마지막 커밋 offset으로 되감는다
        uploaded.values().forEach(this::deleteBestEffort);
        failed.forEach(columnTypes::remove);
        status.error(String.valueOf(firstError));
        rewind();
        reset();
        log.warn("{}ms 후 재시도", backoffMs);
        try {
            sleeper.sleep(backoffMs);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        backoffMs = Math.min(backoffMs * 2, backoffMaxMs);
    }

    private void rewind() {
        Set<TopicPartition> assigned = consumer.assignment();
        Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(assigned);
        for (TopicPartition tp : assigned) {
            OffsetAndMetadata om = committed.get(tp);
            if (om != null) {
                consumer.seek(tp, om.offset());
            } else {
                consumer.seekToBeginning(java.util.List.of(tp));
            }
        }
    }

    private void reset() {
        buffers.clear();
        bufferedBytes = 0;
        pending = false;
    }

    private TableSpec specByKey(String key) {
        return cfg.tables().stream().filter(t -> t.key().equals(key)).findFirst().orElseThrow();
    }

    private record TableResult(long rows, long mergeMs) {
    }

    /** 테이블 1개: (토픽, 파티션)별 파일 쓰기 → MERGE 1문. 스레드 풀에서 실행된다. */
    private TableResult applyTable(TableSpec spec, List<FlatRow> rows, List<String> uploaded) throws Exception {
        Map<String, String> types = columnTypes.get(spec.key());
        if (types == null) {
            types = client.describeColumns(spec.schema(), spec.table());
            columnTypes.put(spec.key(), types);
        }
        // (topic, partition)별로 묶어 offset 범위가 결정적인 파일명을 만든다
        Map<String, List<FlatRow>> groups = new LinkedHashMap<>();
        for (FlatRow r : rows) {
            groups.computeIfAbsent(r.topic() + "\u0000" + r.partition(), k -> new ArrayList<>()).add(r);
        }
        List<String> paths = new ArrayList<>();
        for (List<FlatRow> g : groups.values()) {
            long first = g.get(0).offset();
            long last = g.get(g.size() - 1).offset();
            String path = StagingFiles.path(cfg.volume(), spec, g.get(0).topic(), g.get(0).partition(), first, last);
            StringBuilder sb = new StringBuilder();
            g.forEach(r -> sb.append(r.line()).append('\n'));
            client.upload(path, sb.toString().getBytes(StandardCharsets.UTF_8));
            uploaded.add(path);
            paths.add(path);
        }
        String sql = MergeSqlBuilder.build(cfg.catalog(), spec, types, paths);
        long t0 = clock.getAsLong();
        try {
            client.merge(sql);
        } catch (Exception e) {
            log.error("MERGE 실패 table={} SQL=\n{}", spec.key(), sql);
            throw e;
        }
        return new TableResult(rows.size(), clock.getAsLong() - t0);
    }

    private void deleteBestEffort(List<String> paths) {
        for (String p : new ArrayList<>(paths)) {
            try {
                client.delete(p);
            } catch (Exception e) {
                log.warn("스테이징 파일 삭제 실패(무시, 기동 시 sweep됨): {} {}", p, e.toString());
            }
        }
    }
}
