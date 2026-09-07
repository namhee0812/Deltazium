package io.deltazium.backend.iceberg;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.springframework.stereotype.Service;

/**
 * 파일명 : ChangelogStorageService.java
 * 작성일자 : 26. 09. 07.
 * 작성자 : 최남희
 * 설명 : changelog 저장소 프로파일(MinIO/R2) 요약·연결 테스트 (architecture.md 2.2·3절, TODO ③).
 * UI "changelog 저장소" 카드(읽기 전용)와 8절 DW 타깃 사전 점검(externallyReachable, ④에서 연결)이
 * 이 서비스를 쓴다. **비밀값(jdbc.password·s3 키·token)은 절대 반환하지 않는다** — 호스트·
 * 프로파일 등 요약 정보만 노출.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@Service
public class ChangelogStorageService {

    /** UI 카드용 요약 — 비밀값 없음. externallyReachable=profile==r2 (8절 DW 타깃 사전 점검 기준). */
    public record ChangelogStorageInfo(String profile, String catalogType, String catalogUriHost,
            String warehouse, String bucket, String s3Endpoint, boolean externallyReachable) {
    }

    public record TestResult(boolean ok, String message, long elapsedMs) {
    }

    private final IcebergProperties props;
    private final ChangelogTableService tables;

    public ChangelogStorageService(IcebergProperties props, ChangelogTableService tables) {
        this.props = props;
        this.tables = tables;
    }

    public ChangelogStorageInfo info() {
        boolean r2 = props.isR2();
        String uri = r2 ? props.r2Uri() : props.catalogUri();
        String warehouse = r2 ? props.r2Warehouse() : props.warehouse();
        String bucket = r2 ? props.r2Bucket() : bucketOf(props.warehouse());
        String s3Endpoint = r2 ? props.r2S3Endpoint() : props.s3Endpoint();
        return new ChangelogStorageInfo(
                props.profile(),
                r2 ? "REST" : "JDBC",
                hostOf(uri),
                warehouse,
                bucket,
                s3Endpoint,
                r2);
    }

    /**
     * 카탈로그 namespace 목록 조회(연결성 확인) + changelog_* namespace 수 + 데이터 경로 접근 확인.
     * 마지막 항목은 Catalog 인터페이스가 FileIO를 직접 노출하지 않아 임의의 changelog 테이블 하나를
     * 열어 그 테이블의 {@code Table.io()}로 위치 존재를 확인하는 방식으로 대신한다(등록된 테이블이
     * 하나도 없으면 이 단계는 건너뛰고 namespace 조회 성공만으로 ok를 판정).
     */
    public TestResult test() {
        long start = System.currentTimeMillis();
        try {
            Catalog cat = tables.catalog();
            List<Namespace> changelogNamespaces = new ArrayList<>();
            if (cat instanceof SupportsNamespaces nsCatalog) {
                for (Namespace ns : nsCatalog.listNamespaces()) {
                    if (ns.length() == 1 && ns.level(0).startsWith("changelog_")) {
                        changelogNamespaces.add(ns);
                    }
                }
            }
            String pathCheck = checkAnyTablePath(cat, changelogNamespaces);
            long elapsed = System.currentTimeMillis() - start;
            return new TestResult(true,
                    "changelog namespace " + changelogNamespaces.size() + "개 조회됨" + pathCheck, elapsed);
        } catch (Exception e) {
            return new TestResult(false,
                    e.getMessage() == null ? e.toString() : e.getMessage(),
                    System.currentTimeMillis() - start);
        }
    }

    private String checkAnyTablePath(Catalog cat, List<Namespace> namespaces) {
        for (Namespace ns : namespaces) {
            List<TableIdentifier> ids = cat.listTables(ns);
            if (!ids.isEmpty()) {
                Table table = cat.loadTable(ids.get(0));
                boolean exists = table.io().newInputFile(table.location()).exists();
                return exists ? " · 데이터 경로 접근 확인(" + table.name() + ")"
                        : " · 데이터 경로 접근 실패(" + table.name() + ")";
            }
        }
        return " · 확인할 changelog 테이블 없음(등록 전)";
    }

    /** jdbc: 접두를 벗기고 host[:port]만 뽑는다 — 자격증명이 섞이지 않는 값만 UI에 노출. */
    static String hostOf(String uri) {
        if (uri == null || uri.isBlank()) {
            return null;
        }
        try {
            URI parsed = URI.create(uri.startsWith("jdbc:") ? uri.substring(5) : uri);
            String host = parsed.getHost();
            if (host == null) {
                return null;
            }
            return parsed.getPort() > 0 ? host + ":" + parsed.getPort() : host;
        } catch (Exception e) {
            return null;
        }
    }

    /** s3://bucket/prefix 형태의 warehouse에서 버킷명만 뽑는다 (minio 프로파일 전용). */
    static String bucketOf(String warehouseUri) {
        if (warehouseUri == null || warehouseUri.isBlank()) {
            return null;
        }
        try {
            return URI.create(warehouseUri).getHost();
        } catch (Exception e) {
            return null;
        }
    }
}
