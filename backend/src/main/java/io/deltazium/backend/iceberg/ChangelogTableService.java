package io.deltazium.backend.iceberg;

import java.util.Locale;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
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
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ②: topicPrefix를 생성자 고정값에서 메서드
 * |                          | 인자로 전환 — 소스가 여러 개면 namespace도 호출마다 달라진다
 * |                          | (카탈로그는 설치당 하나, namespace만 소스별, 3·5.1절)
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ③ 저장소 프로파일(MinIO/R2): JdbcCatalog
 * |                          | 직접 참조를 제거하고 `CatalogUtil.buildIcebergCatalog`로 프로파일에
 * |                          | 맞는 카탈로그(JDBC 또는 REST)를 연다(IcebergProperties.catalogProperties()가
 * |                          | 단일 진원지). namespace 존재 검사는 `SupportsNamespaces` 캐스팅 —
 * |                          | REST 카탈로그도 이 인터페이스를 구현한다.
 * --------------------------------------------------
 */
@Service
public class ChangelogTableService {

    /** 파티션: source.ts_ms(epoch millis)의 1일 truncate — 5.2절 */
    static final int PARTITION_WIDTH_MS = 86_400_000;

    /**
     * 카탈로그 구현별 스코핑(JdbcCatalog는 catalog_name 컬럼)과 무관하게, iceberg-sink의
     * 기본 카탈로그 이름("iceberg", IcebergSinkConfig.DEFAULT_CATALOG_NAME)과 반드시 일치해야
     * backend가 만든 테이블을 sink가 본다.
     */
    static final String CATALOG_NAME = "iceberg";

    private final IcebergProperties props;
    private volatile Catalog catalog;

    public ChangelogTableService(IcebergProperties props) {
        this.props = props;
    }

    /** namespace: changelog_<topic.prefix> 소문자 — 소스별로 나뉜다 (5.1절). */
    public String namespace(String topicPrefix) {
        return "changelog_" + topicPrefix.toLowerCase(Locale.ROOT);
    }

    /** changelog 테이블명: {namespace}.{schema}_{table} 소문자 (5.1절) */
    public String changelogTableName(String topicPrefix, String schema, String table) {
        return namespace(topicPrefix) + "." + (schema + "_" + table).toLowerCase(Locale.ROOT);
    }

    /** 테이블이 없으면 기본 골격 + 파티션 스펙으로 생성. 이미 있으면 그대로 둔다. */
    public void ensureChangelogTable(String topicPrefix, String schema, String table) {
        String namespace = namespace(topicPrefix);
        TableIdentifier id = TableIdentifier.of(
                namespace, (schema + "_" + table).toLowerCase(Locale.ROOT));
        Catalog cat = catalog();
        if (cat instanceof SupportsNamespaces nsCatalog && !nsCatalog.namespaceExists(Namespace.of(namespace))) {
            nsCatalog.createNamespace(Namespace.of(namespace));
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
    public void dropChangelogTable(String topicPrefix, String schema, String table, boolean purge) {
        TableIdentifier id = TableIdentifier.of(
                namespace(topicPrefix), (schema + "_" + table).toLowerCase(Locale.ROOT));
        Catalog cat = catalog();
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

    Catalog catalog() {
        Catalog c = catalog;
        if (c == null) {
            synchronized (this) {
                if (catalog == null) {
                    catalog = CatalogUtil.buildIcebergCatalog(CATALOG_NAME, props.catalogProperties(), null);
                }
                c = catalog;
            }
        }
        return c;
    }
}
