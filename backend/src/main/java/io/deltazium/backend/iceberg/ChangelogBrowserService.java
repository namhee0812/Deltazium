package io.deltazium.backend.iceberg;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * changelog(Iceberg/S3) 현황 조회 — S3 오브젝트를 직접 훑지 않고 Iceberg 메타데이터만 읽는다.
 * 카탈로그 호출이 수백 ms 걸릴 수 있어 주기 폴링 금지, 탭 진입/수동 새로고침 시에만 호출할 것.
 */
@Service
public class ChangelogBrowserService {

    private static final Logger log = LoggerFactory.getLogger(ChangelogBrowserService.class);

    /** 파티션 한 칸 = 1일 (5.2절 truncate(source.ts_ms, 86400000)) */
    public record PartitionInfo(String day, long records, int files, long bytes) {
    }

    public record ChangelogInfo(
            String table,           // changelog.<schema>_<table>
            long totalRecords,
            int totalFiles,
            long totalBytes,
            int snapshotCount,
            Long lastCommitAtMs,    // null = 커밋 없음 (빈 테이블)
            List<PartitionInfo> partitions) {
    }

    private final ChangelogTableService tables;

    public ChangelogBrowserService(ChangelogTableService tables) {
        this.tables = tables;
    }

    /** changelog 네임스페이스의 모든 테이블 — 등록 해제 후 보존된 changelog도 보인다. */
    public List<ChangelogInfo> list() {
        List<ChangelogInfo> result = new ArrayList<>();
        for (TableIdentifier id : tables.catalog().listTables(Namespace.of(tables.namespace()))) {
            result.add(describe(id));
        }
        result.sort(Comparator.comparing(ChangelogInfo::table));
        return result;
    }

    private ChangelogInfo describe(TableIdentifier id) {
        Table table = tables.catalog().loadTable(id);
        Snapshot current = table.currentSnapshot();

        long records = 0;
        int files = 0;
        long bytes = 0;
        int snapshots = 0;
        Long lastCommit = null;
        for (Snapshot s : table.snapshots()) {
            snapshots++;
            lastCommit = s.timestampMillis();
        }
        if (current != null) {
            Map<String, String> summary = current.summary();
            records = parseLong(summary.get("total-records"));
            files = (int) parseLong(summary.get("total-data-files"));
            bytes = parseLong(summary.get("total-files-size"));
        }

        return new ChangelogInfo(id.namespace() + "." + id.name(),
                records, files, bytes, snapshots, lastCommit, partitions(table));
    }

    /**
     * 파일 스캔 플랜에서 데이터 파일별 파티션 값·건수·크기를 집계.
     * (PARTITIONS 메타데이터 테이블은 iceberg-data 1.11의 generic reader가 METADATA 포맷을
     * 지원하지 않아 사용 불가 — 실측. manifest 기반 플랜은 core API만으로 동작한다)
     */
    private List<PartitionInfo> partitions(Table table) {
        if (table.currentSnapshot() == null) {
            return List.of();
        }
        record Agg(long[] records, int[] files, long[] bytes) {
        }
        Map<String, Agg> byDay = new java.util.TreeMap<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                DataFile file = task.file();
                StructLike partition = file.partition();
                String day = "(미분할)";
                if (partition != null && partition.size() > 0) {
                    Long v = partition.get(0, Long.class);
                    if (v != null) {
                        day = Instant.ofEpochMilli(v).toString().substring(0, 10);
                    }
                }
                Agg agg = byDay.computeIfAbsent(day,
                        k -> new Agg(new long[1], new int[1], new long[1]));
                agg.records()[0] += file.recordCount();
                agg.files()[0]++;
                agg.bytes()[0] += file.fileSizeInBytes();
            }
        } catch (Exception e) {
            // 파티션 상세는 부가 정보 — 실패해도 요약은 내려준다
            log.warn("파티션 집계 실패 ({}): {}", table.name(), e.toString());
            return List.of();
        }
        List<PartitionInfo> result = new ArrayList<>();
        byDay.forEach((day, agg) ->
                result.add(new PartitionInfo(day, agg.records()[0], agg.files()[0], agg.bytes()[0])));
        return result;
    }

    private static long parseLong(String v) {
        try {
            return v == null ? 0 : Long.parseLong(v);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
