package io.deltazium.dwapply;

import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 파일명 : DwApplyWorker.java
 * 작성일자 : 26. 10. 19.
 * 작성자 : 최남희
 * 설명 : DW apply 워커 진입점 (architecture.md 6.5절 — 자체 코드 금지 규칙의 유일한 예외).
 * Kafka 소비 → UC Volume 스테이징 → MERGE → offset 커밋. Iceberg를 읽지 않고 `source.*`
 * 필드를 읽지 않는다. 인자: config=&lt;설정 JSON 경로&gt; 하나. 토큰은 argv가 아니라 환경변수
 * DZ_DATABRICKS_TOKEN으로 받는다(ps 노출 방지). 종료 코드: 2 = 설정 오류·토큰 없음·JDBC
 * 드라이버 로드 실패 (backend 감시 스레드가 재기동해도 소용없는 종류).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public final class DwApplyWorker {

    private static final Logger log = LoggerFactory.getLogger(DwApplyWorker.class);

    private DwApplyWorker() {
    }

    public static void main(String[] args) throws Exception {
        String configPath = null;
        for (String a : args) {
            if (a.startsWith("config=")) {
                configPath = a.substring("config=".length());
            }
        }
        String token = System.getenv("DZ_DATABRICKS_TOKEN");
        WorkerConfig cfg;
        RestDatabricksClient client;
        try {
            if (configPath == null) {
                throw new IllegalArgumentException("config=<json 경로> 인자 필요");
            }
            if (token == null || token.isBlank()) {
                throw new IllegalArgumentException("환경변수 DZ_DATABRICKS_TOKEN 없음");
            }
            cfg = WorkerConfig.load(Path.of(configPath));
            cfg.topicIndex(); // 토픽 중복 매핑 검증
            client = new RestDatabricksClient(cfg.host(), cfg.httpPath(), cfg.catalog(), token);
            client.checkDriver();
        } catch (Exception e) {
            log.error("시작 실패(설정/드라이버): {}", e.toString());
            System.exit(2);
            return;
        }

        sweep(cfg, client);

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrap());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, cfg.groupId());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // 새로 등록한 테이블의 스냅샷 이벤트를 처음부터 받기 위함
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        AtomicBoolean running = new AtomicBoolean(true);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                running.set(false);
                consumer.wakeup();
            }));
            consumer.subscribe(cfg.allTopics());
            StatusWriter status = new StatusWriter(cfg.statusFile());
            status.start();
            log.info("dw-apply 시작 targetId={} group={} 테이블={} 토픽={}", cfg.targetId(), cfg.groupId(),
                    cfg.tables().size(), cfg.allTopics());
            ApplyLoop loop = new ApplyLoop(cfg, consumer, client, status, System::currentTimeMillis,
                    Thread::sleep, 5_000, 60_000);
            loop.run(running::get);
        } finally {
            client.close();
        }
    }

    /**
     * 기동 시 sweep — 워커 소유 디렉터리(`schema__table`)의 파일을 모두 지운다. 재기동 후 Volume의
     * 파일은 전부 불필요하다(데이터는 Kafka에서 다시 읽는다). 실패해도 계속한다.
     */
    static void sweep(WorkerConfig cfg, DatabricksClient client) {
        for (TableSpec t : cfg.tables()) {
            String dir = StagingFiles.dir(cfg.volume(), t);
            try {
                int n = 0;
                for (String f : client.listDir(dir)) {
                    client.delete(f);
                    n++;
                }
                if (n > 0) {
                    log.info("기동 sweep: {} 파일 {}개 삭제", dir, n);
                }
            } catch (Exception e) {
                log.warn("기동 sweep 실패(계속 진행) {}: {}", dir, e.toString());
            }
        }
    }
}
