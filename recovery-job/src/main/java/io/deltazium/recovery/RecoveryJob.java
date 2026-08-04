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
 * 설명 : 복구 재발행 (architecture.md 6절): Iceberg changelog scan(SCN 범위) →
 * envelope 재조립 → 복구 토픽 발행. **타깃 apply는 하지 않는다** — recovery-sink
 * (live와 동일한 JDBC sink 설정)가 담당한다. 여기가 이 잡의 경계다.
 * 인자(키=값):
 * catalog-uri, catalog-user, catalog-password, warehouse,
 * s3-endpoint, s3-access-key, s3-secret-key,
 * table=changelog.cdc_auto_100, from-scn=31066101955,
 * key-columns=ID[,COL2], bootstrap=localhost:9092, topic=dz-recovery.cdc_auto_100
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public final class RecoveryJob {

    private static final Logger log = LoggerFactory.getLogger(RecoveryJob.class);

    public static void main(String[] rawArgs) {
        Map<String, String> args = parseArgs(rawArgs);
        long fromScn = Long.parseLong(require(args, "from-scn"));
        String tableName = require(args, "table");
        List<String> keyColumns = List.of(require(args, "key-columns").split(","));
        String topic = require(args, "topic");

        JdbcCatalog catalog = openCatalog(args);
        try {
            int dot = tableName.indexOf('.');
            Table table = catalog.loadTable(TableIdentifier.of(
                    tableName.substring(0, dot), tableName.substring(dot + 1)));

            // 6.2절: scn은 string이라 Iceberg 필터로 수치 비교 불가 — 스캔 후 캐스팅 필터,
            // 전역 순서(scn, txId)는 여기서 복원한다. (토이 볼륨 전제의 인메모리 정렬)
            List<Record> replay = new ArrayList<>();
            try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
                for (Record row : rows) {
                    Long scn = scnOf(row);
                    if (scn != null && scn >= fromScn) {
                        replay.add(row);
                    }
                }
            } catch (Exception e) {
                throw new IllegalStateException("Iceberg scan 실패: " + e.getMessage(), e);
            }
            replay.sort(Comparator
                    .comparing(RecoveryJob::scnOf, Comparator.nullsLast(Comparator.naturalOrder()))
                    .thenComparing(r -> {
                        Record source = (Record) r.getField("source");
                        Object tx = source == null ? null : source.getField("txId");
                        return tx == null ? "" : tx.toString();
                    }));
            log.info("재발행 대상 {}건 (from-scn={}, table={})", replay.size(), fromScn, tableName);

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

    private static Long scnOf(Record row) {
        Record source = (Record) row.getField("source");
        Object scn = source == null ? null : source.getField("scn");
        try {
            return scn == null ? null : Long.parseLong(scn.toString());
        } catch (NumberFormatException e) {
            return null;
        }
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
