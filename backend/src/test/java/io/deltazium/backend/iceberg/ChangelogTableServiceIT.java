package io.deltazium.backend.iceberg;

import java.util.Map;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 파일명 : ChangelogTableServiceIT.java
 * 작성일자 : 26. 07. 26.
 * 작성자 : 최남희
 * 설명 : 실 인프라(PG 카탈로그 + MinIO) 대상 통합 테스트 — 기본 비활성.
 * 실행: ./gradlew :backend:test --tests '*ChangelogTableServiceIT' -Dintegration=true
 * (deploy/start-infra.sh로 인프라가 떠 있어야 한다)
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 26.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@EnabledIfSystemProperty(named = "integration", matches = "true")
class ChangelogTableServiceIT {

    private final IcebergProperties props = new IcebergProperties(
            "jdbc:postgresql://localhost:5433/iceberg_catalog", "deltazium", "deltazium",
            "s3://deltazium-warehouse/warehouse", "http://localhost:9010",
            "deltazium", "deltazium123", "changelog");

    @Test
    void 실제_카탈로그에_사전_생성되고_멱등이다() {
        ChangelogTableService service = new ChangelogTableService(props);
        service.ensureChangelogTable("ITPROBE", "T1");
        service.ensureChangelogTable("ITPROBE", "T1"); // 멱등

        // sink와 같은 카탈로그 이름("iceberg")으로 열어야 같은 테이블이 보인다
        JdbcCatalog catalog = new JdbcCatalog();
        catalog.initialize("iceberg", Map.of(
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
        TableIdentifier id = TableIdentifier.of("changelog", "itprobe_t1");
        assertThat(catalog.tableExists(id)).isTrue();
        Table table = catalog.loadTable(id);
        assertThat(table.spec().fields().get(0).transform().toString()).isEqualTo("truncate[86400000]");
        assertThat(table.schema().findField("source.ts_ms")).isNotNull();

        // 테스트가 방금 만든 probe 테이블 정리 (사용자 데이터 아님)
        catalog.dropTable(id, true);
    }
}
