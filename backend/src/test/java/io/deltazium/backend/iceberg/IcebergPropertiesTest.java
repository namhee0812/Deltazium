package io.deltazium.backend.iceberg;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 파일명 : IcebergPropertiesTest.java
 * 작성일자 : 26. 09. 07.
 * 작성자 : 최남희
 * 설명 : 저장소 프로파일(MinIO/R2)별 카탈로그 속성 맵 단위 테스트 (architecture.md 3절, TODO ③).
 * minio 프로파일은 종전 iceberg-sink.json.tmpl에 하드코딩돼 있던 catalog 블록과 키·값이
 * 완전히 동일해야 한다 — 템플릿에서 backend 주입으로 옮기는 리팩터라 회귀 방지가 목적이다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
class IcebergPropertiesTest {

    @Test
    void 프로파일_미지정이면_minio가_기본값이다() {
        IcebergProperties props = new IcebergProperties(null, "u", "p", "pw", "wh",
                "ep", "ak", "sk", null, null, null, null, null, null, null);
        assertThat(props.profile()).isEqualTo("minio");
        assertThat(props.isR2()).isFalse();
    }

    @Test
    void minio_프로파일_카탈로그_속성은_종전_템플릿과_키_값이_동일하다() {
        IcebergProperties props = IcebergProperties.minio(
                "jdbc:postgresql://localhost:5433/iceberg_catalog", "deltazium", "deltazium",
                "s3://deltazium-warehouse/warehouse", "http://localhost:9010",
                "deltazium", "deltazium123");

        assertThat(props.catalogProperties())
                .containsEntry("catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
                .containsEntry("uri", "jdbc:postgresql://localhost:5433/iceberg_catalog")
                .containsEntry("jdbc.user", "deltazium")
                .containsEntry("jdbc.password", "deltazium")
                .containsEntry("warehouse", "s3://deltazium-warehouse/warehouse")
                .containsEntry("io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .containsEntry("s3.endpoint", "http://localhost:9010")
                .containsEntry("s3.path-style-access", "true")
                .containsEntry("s3.access-key-id", "deltazium")
                .containsEntry("s3.secret-access-key", "deltazium123")
                .containsEntry("client.region", "us-east-1")
                .hasSize(11);
    }

    @Test
    void r2_프로파일은_rest_카탈로그_속성에_토큰을_담고_jdbc_키가_없다() {
        IcebergProperties props = IcebergProperties.r2(
                "https://catalog.cloudflarestorage.com/xyz/iceberg",
                "my-bucket", "my-bucket", "r2-token-abc", null, null, null);

        var map = props.catalogProperties();
        assertThat(map)
                .containsEntry("type", "rest")
                .containsEntry("uri", "https://catalog.cloudflarestorage.com/xyz/iceberg")
                .containsEntry("warehouse", "my-bucket")
                .containsEntry("token", "r2-token-abc")
                .containsEntry("io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .containsEntry("client.region", "auto");
        // 위임(vended credentials) 전제 — S3 엔드포인트/키가 없으면 넣지 않는다
        assertThat(map).doesNotContainKeys("s3.endpoint", "s3.access-key-id", "s3.secret-access-key");
        // JDBC 카탈로그 키는 존재하지 않는다
        assertThat(map).doesNotContainKeys("catalog-impl", "jdbc.user", "jdbc.password");
    }

    @Test
    void r2_프로파일에_s3_키를_주면_위임_실패_대비_설정이_추가된다() {
        IcebergProperties props = IcebergProperties.r2(
                "https://catalog.cloudflarestorage.com/xyz/iceberg", "my-bucket", "my-bucket",
                "r2-token-abc", "https://acct.r2.cloudflarestorage.com", "ak", "sk");

        assertThat(props.catalogProperties())
                .containsEntry("s3.endpoint", "https://acct.r2.cloudflarestorage.com")
                .containsEntry("s3.path-style-access", "true")
                .containsEntry("s3.access-key-id", "ak")
                .containsEntry("s3.secret-access-key", "sk");
    }
}
