package io.deltazium.backend.connect;

/**
 * 파일명 : ConnectorNames.java
 * 작성일자 : 26. 09. 07.
 * 작성자 : 최남희
 * 설명 : 커넥터·컨슈머 그룹·복구 토픽 이름 규칙 단일 진원지 (architecture.md 4절, 2026-09-07
 * 다중 소스·다중 타깃 ②). 소스가 여러 개면 커넥터 이름에 소스 식별자(topic.prefix)가
 * 반드시 들어가야 충돌하지 않는다 — 이 클래스 밖에서 문자열을 조립하지 말 것.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public final class ConnectorNames {

    private ConnectorNames() {
    }

    /** dz-source-<prefix> — 소스당 1개. */
    public static String source(String topicPrefix) {
        return "dz-source-" + topicPrefix;
    }

    /** dz-iceberg-<prefix> — 소스당 1개 (changelog append, 4절). */
    public static String icebergSink(String topicPrefix) {
        return "dz-iceberg-" + topicPrefix;
    }

    /** dz-jdbc-sink-<prefix>-<suffix> — 테이블당 1개. */
    public static String jdbcSink(String topicPrefix, String suffix) {
        return "dz-jdbc-sink-" + topicPrefix + "-" + suffix;
    }

    /** dz-recovery-sink-<prefix>-<suffix> — 복구 시에만 기동, 평시 정지. */
    public static String recoverySink(String topicPrefix, String suffix) {
        return "dz-recovery-sink-" + topicPrefix + "-" + suffix;
    }

    /** 복구 토픽 이름 — 소스 식별자를 넣어 다른 소스의 동일 suffix와 충돌하지 않게 한다. */
    public static String recoveryTopic(String topicPrefix, String suffix) {
        return "dz-recovery." + topicPrefix + "." + suffix;
    }

    /** Kafka Connect 기본 consumer group 이름 규칙 — connect-<커넥터명>. */
    public static String consumerGroup(String connectorName) {
        return "connect-" + connectorName;
    }

    /** 소스 캡처 토픽 — <prefix>.<schema>.<table>. */
    public static String captureTopic(String topicPrefix, String schema, String table) {
        return topicPrefix + "." + schema + "." + table;
    }

    /** Debezium notification 토픽 — <prefix>-notifications. */
    public static String notificationTopic(String topicPrefix) {
        return topicPrefix + "-notifications";
    }
}
