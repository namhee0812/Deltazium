package io.deltazium.backend.iceberg;

import java.util.Locale;
import java.util.Map;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * 파일명 : ChangelogTableService.java
 * 작성일자 : 26. 07. 26.
 * 작성자 : 최남희
 * 설명 : changelog 테이블 사전 생성 (architecture.md 5.1절 개정판).
 * 기본 골격(op/ts_ms/source 핵심 필드)만 만들고 before/after 등 나머지는
 * iceberg-sink의 evolve-schema가 첫 레코드에서 채운다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 26.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 09. 05.       | 최남희  | 다중 소스·다중 타깃 ① changelog 중립 계약(architecture.md 5.1절):
 * |                          | `_pos` struct(topic/partition/offset/timestamp) 추가,
 * |                          | namespace를 고정 설정값 대신 `changelog_<topic.prefix>`로 계산
 * --------------------------------------------------
 */
@Service
public class ChangelogTableService {

    /** 파티션: source.ts_ms(epoch millis)의 1일 truncate — 5.2절 */
    static final int PARTITION_WIDTH_MS = 86_400_000;

    /**
     * JdbcCatalog는 catalog_name 컬럼으로 테이블을 스코핑한다.
     * iceberg-sink의 기본 카탈로그 이름("iceberg", IcebergSinkConfig.DEFAULT_CATALOG_NAME)과
     * 반드시 일치해야 backend가 만든 테이블을 sink가 본다.
     */
    static final String CATALOG_NAME = "iceberg";

    private final IcebergProperties props;
    private final String namespace;
    private volatile JdbcCatalog catalog;

    public ChangelogTableService(IcebergProperties props,
                                 @Value("${deltazium.topic-prefix}") String topicPrefix) {
        this.props = props;
        // namespace는 소스별 — 소스 식별자(topic.prefix)가 늘어나면(② 마일스톤) 인스턴스별로
        // 나뉜다. 지금은 소스가 하나라 전역 topic-prefix를 그대로 쓴다 (5.1절).
        this.namespace = "changelog_" + topicPrefix.toLowerCase(Locale.ROOT);
    }

    /** changelog 테이블명: {namespace}.{schema}_{table} 소문자 (5.1절) */
    public String changelogTableName(String schema, String table) {
        return namespace + "." + (schema + "_" + table).toLowerCase(Locale.ROOT);
    }

    /** 테이블이 없으면 기본 골격 + 파티션 스펙으로 생성. 이미 있으면 그대로 둔다. */
    public void ensureChangelogTable(String schema, String table) {
        TableIdentifier id = TableIdentifier.of(
                namespace, (schema + "_" + table).toLowerCase(Locale.ROOT));
        JdbcCatalog cat = catalog();
        if (!cat.namespaceExists(Namespace.of(namespace))) {
            cat.createNamespace(Namespace.of(namespace));
        }
        if (!cat.tableExists(id)) {
            Schema base = baseSchema();
            cat.createTable(id, base, partitionSpec(base));
        }
    }

    /**
     * changelog 테이블 삭제. purge=true면 S3 데이터 파일까지 지운다.
     * 복구 원본을 지우는 작업 — 사용자가 UI에서 명시적으로 확인한 경우에만 호출할 것.
     */
    public void dropChangelogTable(String schema, String table, boolean purge) {
        TableIdentifier id = TableIdentifier.of(
                namespace, (schema + "_" + table).toLowerCase(Locale.ROOT));
        JdbcCatalog cat = catalog();
        if (cat.tableExists(id)) {
            cat.dropTable(id, purge);
        }
    }

    /**
     * envelope 골격 — 전부 optional (sink의 evolve union과 충돌하지 않도록).
     * `_pos`는 iceberg-sink의 KafkaMetadataTransform(field_name=_pos, nested=true)이 부착하는
     * 구조와 필드명·타입이 정확히 일치해야 한다 (5.1절) — 다르면 evolve-schema가 별도 컬럼으로
     * 추가해버려 재생 순서가 깨진다.
     */
    static Schema baseSchema() {
        return new Schema(
                Types.NestedField.optional(1, "op", Types.StringType.get()),
                Types.NestedField.optional(2, "ts_ms", Types.LongType.get()),
                Types.NestedField.optional(3, "source", Types.StructType.of(
                        Types.NestedField.optional(4, "scn", Types.StringType.get()),
                        Types.NestedField.optional(5, "txId", Types.StringType.get()),
                        Types.NestedField.optional(6, "ts_ms", Types.LongType.get()),
                        Types.NestedField.optional(7, "schema", Types.StringType.get()),
                        Types.NestedField.optional(8, "table", Types.StringType.get()))),
                Types.NestedField.optional(9, "_pos", Types.StructType.of(
                        Types.NestedField.optional(10, "topic", Types.StringType.get()),
                        Types.NestedField.optional(11, "partition", Types.IntegerType.get()),
                        Types.NestedField.optional(12, "offset", Types.LongType.get()),
                        Types.NestedField.optional(13, "timestamp", Types.LongType.get()))));
    }

    static PartitionSpec partitionSpec(Schema schema) {
        return PartitionSpec.builderFor(schema)
                .truncate("source.ts_ms", PARTITION_WIDTH_MS)
                .build();
    }

    String namespace() {
        return namespace;
    }

    JdbcCatalog catalog() {
        JdbcCatalog c = catalog;
        if (c == null) {
            synchronized (this) {
                if (catalog == null) {
                    c = new JdbcCatalog();
                    c.initialize(CATALOG_NAME, Map.of(
                            "uri", props.catalogUri(),
                            "jdbc.user", props.catalogUser(),
                            "jdbc.password", props.catalogPassword(),
                            "warehouse", props.warehouse(),
                            "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
                            "s3.endpoint", props.s3Endpoint(),
                            "s3.path-style-access", "true",
                            "s3.access-key-id", props.s3AccessKey(),
                            "s3.secret-access-key", props.s3SecretKey(),
                            "client.region", "us-east-1"));
                    catalog = c;
                }
                c = catalog;
            }
        }
        return c;
    }
}
