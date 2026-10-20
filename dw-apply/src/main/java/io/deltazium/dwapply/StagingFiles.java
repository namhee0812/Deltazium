package io.deltazium.dwapply;

/**
 * 파일명 : StagingFiles.java
 * 작성일자 : 26. 10. 19.
 * 작성자 : 최남희
 * 설명 : UC Volume 스테이징 파일 경로 규칙. 파일명은 (토픽, 파티션, 첫 offset, 마지막 offset)에서
 * 결정적으로 정해진다 — 같은 범위를 재시도하면 같은 이름으로 overwrite된다.
 * `<volume>/<schema>__<table>/<topic>-p<partition>-<first>-<last>.jsonl`
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public final class StagingFiles {

    private StagingFiles() {
    }

    public static String fileName(String topic, int partition, long firstOffset, long lastOffset) {
        return "%s-p%d-%d-%d.jsonl".formatted(topic, partition, firstOffset, lastOffset);
    }

    public static String dir(String volume, TableSpec spec) {
        return volume + "/" + spec.dirName();
    }

    public static String path(String volume, TableSpec spec, String topic, int partition,
                              long firstOffset, long lastOffset) {
        return dir(volume, spec) + "/" + fileName(topic, partition, firstOffset, lastOffset);
    }
}
