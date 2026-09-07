package io.deltazium.backend.iceberg;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 파일명 : ChangelogStorageServiceTest.java
 * 작성일자 : 26. 09. 07.
 * 작성자 : 최남희
 * 설명 : changelog 저장소 카드 요약(info()) 단위 테스트 — 비밀값이 응답에 없는지, 프로파일별
 * catalogType·호스트·버킷 파싱이 맞는지 검증한다. test()(실 카탈로그 연결)는 통합 테스트 영역.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
class ChangelogStorageServiceTest {

    @Test
    void minio_프로파일_요약은_jdbc_uri에서_호스트만_뽑고_비밀값이_없다() {
        IcebergProperties props = IcebergProperties.minio(
                "jdbc:postgresql://localhost:5433/iceberg_catalog", "deltazium", "deltazium",
                "s3://deltazium-warehouse/warehouse", "http://localhost:9010",
                "deltazium", "deltazium123");
        ChangelogStorageService service = new ChangelogStorageService(props, new ChangelogTableService(props));

        var info = service.info();
        assertThat(info.profile()).isEqualTo("minio");
        assertThat(info.catalogType()).isEqualTo("JDBC");
        assertThat(info.catalogUriHost()).isEqualTo("localhost:5433");
        assertThat(info.warehouse()).isEqualTo("s3://deltazium-warehouse/warehouse");
        assertThat(info.bucket()).isEqualTo("deltazium-warehouse");
        assertThat(info.s3Endpoint()).isEqualTo("http://localhost:9010");
        assertThat(info.externallyReachable()).isFalse();

        // record 필드에 password가 없다는 것이 곧 응답에 비밀값이 없다는 보장 — toString에도 안 찍힌다
        assertThat(info.toString()).doesNotContain("deltazium123");
    }

    @Test
    void r2_프로파일_요약은_rest_카탈로그이고_외부_접근_가능이다() {
        IcebergProperties props = IcebergProperties.r2(
                "https://api.cloudflarestorage.com/xyz/iceberg", "my-bucket", "my-bucket",
                "r2-token-abc", "https://acct.r2.cloudflarestorage.com", null, null);
        ChangelogStorageService service = new ChangelogStorageService(props, new ChangelogTableService(props));

        var info = service.info();
        assertThat(info.profile()).isEqualTo("r2");
        assertThat(info.catalogType()).isEqualTo("REST");
        assertThat(info.catalogUriHost()).isEqualTo("api.cloudflarestorage.com");
        assertThat(info.warehouse()).isEqualTo("my-bucket");
        assertThat(info.bucket()).isEqualTo("my-bucket");
        assertThat(info.s3Endpoint()).isEqualTo("https://acct.r2.cloudflarestorage.com");
        assertThat(info.externallyReachable()).isTrue();
        assertThat(info.toString()).doesNotContain("r2-token-abc");
    }

    @Test
    void hostOf_jdbc_uri에서_host_port만_뽑는다() {
        assertThat(ChangelogStorageService.hostOf("jdbc:postgresql://localhost:5433/iceberg_catalog"))
                .isEqualTo("localhost:5433");
        assertThat(ChangelogStorageService.hostOf(null)).isNull();
        assertThat(ChangelogStorageService.hostOf("")).isNull();
    }

    @Test
    void bucketOf_s3_uri에서_버킷명만_뽑는다() {
        assertThat(ChangelogStorageService.bucketOf("s3://deltazium-warehouse/warehouse"))
                .isEqualTo("deltazium-warehouse");
        assertThat(ChangelogStorageService.bucketOf(null)).isNull();
    }
}
