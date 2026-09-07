package io.deltazium.backend.iceberg;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 파일명 : IcebergProperties.java
 * 작성일자 : 26. 07. 26.
 * 작성자 : 최남희
 * 설명 : changelog 저장소(Iceberg) 설정 — Deltazium 자체 인프라라서 사용자 등록 대상이 아니다.
 * DB 연결 저장소의 TARGET(실적재 Oracle)과 구분할 것.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 26.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 09. 05.       | 최남희  | namespace 필드 제거 — 다중 소스·다중 타깃 ① changelog 중립
 * |                          | 계약(architecture.md 5.1절)에서 namespace는 소스별
 * |                          | `changelog_<topic.prefix>`로 계산돼야 하므로 고정 설정값에서
 * |                          | ChangelogTableService의 계산값으로 이관
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 다중 소스·다중 타깃 ③ 저장소 프로파일(MinIO/R2, architecture.md
 * |                          | 2.2·3절): `profile`(minio|r2) 추가, R2용 원시 설정(r2Uri 등) 추가,
 * |                          | `catalogProperties()`를 카탈로그 속성 맵의 단일 진원지로 신설 —
 * |                          | backend(CatalogUtil.buildIcebergCatalog)·iceberg-sink 배포
 * |                          | (iceberg.catalog.&lt;key&gt;)·recovery-job 기동 인자(catalog.&lt;key&gt;)
 * |                          | 세 곳이 이 맵을 공유한다. minio 프로파일 값·키는 종전과 완전히
 * |                          | 동일(회귀 테스트로 고정). 비밀값(password·token)은 이 맵에만
 * |                          | 담기며 로그·API 응답에 노출하지 않을 것(UI 카드는 별도 요약 DTO 사용).
 * --------------------------------------------------
 */
@ConfigurationProperties(prefix = "deltazium.iceberg")
public record IcebergProperties(
        String profile,
        String catalogUri,
        String catalogUser,
        String catalogPassword,
        String warehouse,
        String s3Endpoint,
        String s3AccessKey,
        String s3SecretKey,
        String r2Uri,
        String r2Warehouse,
        String r2Bucket,
        String r2Token,
        String r2S3Endpoint,
        String r2S3AccessKey,
        String r2S3SecretKey) {

    public IcebergProperties {
        profile = (profile == null || profile.isBlank()) ? "minio" : profile.toLowerCase(Locale.ROOT);
    }

    /** minio 프로파일 편의 생성자 (테스트용) — R2 필드는 전부 미설정. */
    public static IcebergProperties minio(String catalogUri, String catalogUser, String catalogPassword,
            String warehouse, String s3Endpoint, String s3AccessKey, String s3SecretKey) {
        return new IcebergProperties("minio", catalogUri, catalogUser, catalogPassword, warehouse,
                s3Endpoint, s3AccessKey, s3SecretKey, null, null, null, null, null, null, null);
    }

    /** r2 프로파일 편의 생성자 (테스트용) — s3AccessKey/SecretKey는 자격 위임 실패 대비 선택값. */
    public static IcebergProperties r2(String uri, String warehouse, String bucket, String token,
            String s3Endpoint, String s3AccessKey, String s3SecretKey) {
        return new IcebergProperties("r2", null, null, null, null, null, null, null,
                uri, warehouse, bucket, token, s3Endpoint, s3AccessKey, s3SecretKey);
    }

    public boolean isR2() {
        return "r2".equals(profile);
    }

    /**
     * Iceberg 카탈로그 속성 맵 — 단일 진원지 (architecture.md 3절, TODO ③).
     * 반환하는 키는 Iceberg 표준 카탈로그 속성 이름 그대로라(CatalogProperties·S3FileIOProperties
     * 상수와 동일 문자열) 세 소비처가 그대로 쓸 수 있다:
     * ① backend 자신 — {@code CatalogUtil.buildIcebergCatalog("iceberg", catalogProperties(), null)},
     * ② iceberg-sink 배포 — {@code "iceberg.catalog." + key}를 extraConfig로,
     * ③ recovery-job 기동 인자 — {@code "catalog." + key + "=" + value}.
     * 비밀값(jdbc.password·token)이 섞여 있으므로 로그에 그대로 찍지 말 것.
     */
    public Map<String, String> catalogProperties() {
        Map<String, String> props = new LinkedHashMap<>();
        if (isR2()) {
            // R2 Data Catalog: Iceberg REST — type=rest, S3 자격은 원칙적으로 카탈로그가 위임
            // (vended credentials). 위임 실패 대비 선택적 S3 키만 있으면 얹는다.
            props.put("type", "rest");
            props.put("uri", r2Uri);
            props.put("warehouse", r2Warehouse);
            props.put("token", r2Token);
            props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            props.put("client.region", "auto");
            if (notBlank(r2S3Endpoint)) {
                props.put("s3.endpoint", r2S3Endpoint);
                props.put("s3.path-style-access", "true");
            }
            if (notBlank(r2S3AccessKey) && notBlank(r2S3SecretKey)) {
                props.put("s3.access-key-id", r2S3AccessKey);
                props.put("s3.secret-access-key", r2S3SecretKey);
            }
        } else {
            // minio 프로파일 — 종전 하드코딩 값과 키·값 동일(회귀 테스트로 고정)
            props.put("catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog");
            props.put("uri", catalogUri);
            props.put("jdbc.user", catalogUser);
            props.put("jdbc.password", catalogPassword);
            props.put("warehouse", warehouse);
            props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            props.put("s3.endpoint", s3Endpoint);
            props.put("s3.path-style-access", "true");
            props.put("s3.access-key-id", s3AccessKey);
            props.put("s3.secret-access-key", s3SecretKey);
            props.put("client.region", "us-east-1");
        }
        return props;
    }

    private static boolean notBlank(String s) {
        return s != null && !s.isBlank();
    }
}
