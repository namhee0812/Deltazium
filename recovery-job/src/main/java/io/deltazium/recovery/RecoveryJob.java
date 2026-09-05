package io.deltazium.recovery;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.deltazium.recovery.envelope.ConnectJsonAssembler;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 파일명 : RecoveryJob.java
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : 복구 재발행 (architecture.md 6절): Iceberg changelog scan(시각 진입점 → ts_ms 파티션
 * 프루닝) → `_pos` 순서로 정렬 → envelope 재조립 → 복구 토픽 발행. **타깃 apply는 하지 않는다**
 * — recovery-sink(live와 동일한 JDBC sink 설정)가 담당한다. 여기가 이 잡의 경계다.
 * 인자(키=값):
 * catalog-uri, catalog-user, catalog-password, warehouse,
 * s3-endpoint, s3-access-key, s3-secret-key,
 * table=changelog_dz.cdc_auto_100, from-ts-ms=1753300000000,
 * key-columns=ID[,COL2], bootstrap=localhost:9092, topic=dz-recovery.cdc_auto_100
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 09. 05.       | 최남희  | 다중 소스·다중 타깃 ① changelog 중립 계약(architecture.md 5.1·5.2·
 * |                          | 6.2절): 진입점을 SCN에서 시각으로, 정렬 기준을 source.scn·txId에서
 * |                          | `_pos`(partition, offset)로 전환 — 장기 트랜잭션에서 scn 순서와
 * |                          | 방출 순서가 어긋나는 문제 회피. scan은 ts_ms 파티션 한 칸 앞부터
 * |                          | 전부 읽고 정밀 절단은 하지 않는다(PK upsert 멱등이 흡수).
 * |                          | 재조립에서 `_pos`는 제외(envelope 무손실 불변식 유지).
 * --------------------------------------------------
 */
public final class RecoveryJob {

    private static final Logger log = LoggerFactory.getLogger(RecoveryJob.class);

    /**
     * 파티션 폭(ms) — backend의 ChangelogTableService.PARTITION_WIDTH_MS와 반드시 같아야
     * 한다(5.2절). recovery-job은 backend에 의존하지 않는 플레인 Java 모듈이라 상수를
     * 중복 정의한다.
     */
    static final long PARTITION_WIDTH_MS = 86_400_000L;

    /**
     * 재생 순서 (6.2절): `_pos.partition`별로 `_pos.offset` 오름차순. 파티션 간 인터리빙은
     * 임의 — 같은 PK는 항상 같은 파티션이라 upsert 정합에 영향 없다. source.scn·txId 등
     * 소스 전용 위치로 정렬하지 않는다(장기 트랜잭션에서 커밋 순서와 어긋나는 사고, 5.1절).
     */
    static final Comparator<Record> REPLAY_ORDER = Comparator
            .comparing(RecoveryJob::posPartition, Comparator.nullsLast(Comparator.naturalOrder()))
            .thenComparing(RecoveryJob::posOffset, Comparator.nullsLast(Comparator.naturalOrder()));

    public static void main(String[] rawArgs) {
        Map<String, String> args = parseArgs(rawArgs);
        long fromTsMs = Long.parseLong(require(args, "from-ts-ms"));
        String tableName = require(args, "table");
        List<String> keyColumns = List.of(require(args, "key-columns").split(","));
        String topic = require(args, "topic");

        JdbcCatalog catalog = openCatalog(args);
        try {
            int dot = tableName.indexOf('.');
            Table table = catalog.loadTable(TableIdentifier.of(
                    tableName.substring(0, dot), tableName.substring(dot + 1)));

            // 5.2절: 진입 시각의 한 파티션 앞부터 스캔 — 정밀 절단은 하지 않는다.
            long scanFromTsMs = lookbackBoundary(fromTsMs, PARTITION_WIDTH_MS);
            List<Record> replay = new ArrayList<>();
            try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                    .where(Expressions.greaterThanOrEqual("source.ts_ms", scanFromTsMs))
                    .build()) {
                for (Record row : rows) {
                    replay.add(row);
                }
            } catch (Exception e) {
                throw new IllegalStateException("Iceberg scan 실패: " + e.getMessage(), e);
            }
            replay.sort(REPLAY_ORDER);
            log.info("재발행 대상 {}건 (from-ts-ms={}, scan-from-ts-ms={}, table={})",
                    replay.size(), fromTsMs, scanFromTsMs, tableName);

            ConnectJsonAssembler assembler = new ConnectJsonAssembler();
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, require(args, "bootstrap"));
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

            long published = 0;
            long skipped = 0;
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                for (Record row : replay) {
                    ObjectNode key = assembler.key(table.schema(), row, keyColumns);
                    if (key == null) {
                        skipped++;
                        continue;
                    }
                    ObjectNode value = assembler.value(table.schema(), row);
                    producer.send(new ProducerRecord<>(topic, key.toString(), value.toString()));
                    published++;
                }
                producer.flush();
            }
            log.info("복구 토픽 발행 완료: {}건 (건너뜀 {}건) → {}", published, skipped, topic);
            // backend가 stdout 마지막 줄로 결과를 파싱한다
            System.out.println("RECOVERY_RESULT published=" + published + " skipped=" + skipped);
        } finally {
            try {
                catalog.close();
            } catch (Exception ignored) {
                // 종료 경로
            }
        }
    }

    /**
     * 진입 시각이 속한 ts_ms 파티션의 한 칸 앞 경계 — Iceberg의 truncate(ts_ms, width) 파티션과
     * 정렬되는 값이라 파티션 프루닝이 그대로 적용된다(5.2절).
     */
    static long lookbackBoundary(long fromTsMs, long partitionWidthMs) {
        long entryPartitionStart = Math.floorDiv(fromTsMs, partitionWidthMs) * partitionWidthMs;
        return entryPartitionStart - partitionWidthMs;
    }

    private static Integer posPartition(Record row) {
        Record pos = (Record) row.getField("_pos");
        return pos == null ? null : (Integer) pos.getField("partition");
    }

    private static Long posOffset(Record row) {
        Record pos = (Record) row.getField("_pos");
        return pos == null ? null : (Long) pos.getField("offset");
    }

    private static JdbcCatalog openCatalog(Map<String, String> args) {
        JdbcCatalog catalog = new JdbcCatalog();
        Map<String, String> props = new HashMap<>();
        props.put("uri", require(args, "catalog-uri"));
        props.put("jdbc.user", require(args, "catalog-user"));
        props.put("jdbc.password", require(args, "catalog-password"));
        props.put("warehouse", require(args, "warehouse"));
        props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("s3.endpoint", require(args, "s3-endpoint"));
        props.put("s3.path-style-access", "true");
        props.put("s3.access-key-id", require(args, "s3-access-key"));
        props.put("s3.secret-access-key", require(args, "s3-secret-key"));
        props.put("client.region", "us-east-1");
        // sink·backend와 같은 카탈로그 이름이어야 같은 테이블이 보인다
        catalog.initialize("iceberg", props);
        return catalog;
    }

    private static Map<String, String> parseArgs(String[] rawArgs) {
        Map<String, String> args = new HashMap<>();
        for (String a : rawArgs) {
            int eq = a.indexOf('=');
            if (eq > 0) {
                args.put(a.substring(0, eq).replaceFirst("^--", ""), a.substring(eq + 1));
            }
        }
        return args;
    }

    private static String require(Map<String, String> args, String key) {
        String v = args.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("필수 인자 누락: " + key);
        }
        return v;
    }
}
