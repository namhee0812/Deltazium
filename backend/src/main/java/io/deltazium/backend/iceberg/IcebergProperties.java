package io.deltazium.backend.iceberg;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * changelog 저장소(Iceberg/MinIO) 설정 — Deltazium 자체 인프라라서 사용자 등록 대상이 아니다.
 * DB 연결 저장소의 TARGET(실적재 Oracle)과 구분할 것.
 */
@ConfigurationProperties(prefix = "deltazium.iceberg")
public record IcebergProperties(
        String catalogUri,
        String catalogUser,
        String catalogPassword,
        String warehouse,
        String s3Endpoint,
        String s3AccessKey,
        String s3SecretKey,
        String namespace) {
}
