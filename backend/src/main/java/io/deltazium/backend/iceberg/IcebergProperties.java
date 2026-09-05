package io.deltazium.backend.iceberg;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 파일명 : IcebergProperties.java
 * 작성일자 : 26. 07. 26.
 * 작성자 : 최남희
 * 설명 : changelog 저장소(Iceberg/MinIO) 설정 — Deltazium 자체 인프라라서 사용자 등록 대상이 아니다.
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
 */
@ConfigurationProperties(prefix = "deltazium.iceberg")
public record IcebergProperties(
        String catalogUri,
        String catalogUser,
        String catalogPassword,
        String warehouse,
        String s3Endpoint,
        String s3AccessKey,
        String s3SecretKey) {
}
