package io.deltazium.recovery;

import java.util.ArrayList;
import java.util.List;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

/**
 * 파일명 : RecoveryJobTest.java
 * 작성일자 : 26. 09. 05.
 * 작성자 : 최남희
 * 설명 : 재생 순서·파티션 lookback 순수 로직 단위 테스트 (다중 소스·다중 타깃 ① changelog
 * 중립 계약, architecture.md 5.1·5.2·6.2절). Iceberg scan은 통합 테스트 영역이라 여기서는
 * 카탈로그 없이 REPLAY_ORDER·lookbackBoundary만 검증한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 05.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
class RecoveryJobTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "op", Types.StringType.get()),
            Types.NestedField.optional(2, "source", Types.StructType.of(
                    Types.NestedField.optional(3, "scn", Types.StringType.get()))),
            Types.NestedField.optional(4, "_pos", Types.StructType.of(
                    Types.NestedField.optional(5, "topic", Types.StringType.get()),
                    Types.NestedField.optional(6, "partition", Types.IntegerType.get()),
                    Types.NestedField.optional(7, "offset", Types.LongType.get()),
                    Types.NestedField.optional(8, "timestamp", Types.LongType.get()))));

    private static Record row(String scn, int partition, long offset) {
        Record source = GenericRecord.create(SCHEMA.findField("source").type().asStructType());
        source.setField("scn", scn);

        Record pos = GenericRecord.create(SCHEMA.findField("_pos").type().asStructType());
        pos.setField("topic", "dz.CDC.T1");
        pos.setField("partition", partition);
        pos.setField("offset", offset);
        pos.setField("timestamp", 1_700_000_000_000L);

        Record row = GenericRecord.create(SCHEMA.asStruct());
        row.setField("op", "u");
        row.setField("source", source);
        row.setField("_pos", pos);
        return row;
    }

    /**
     * architecture.md 5.1절의 장기 트랜잭션 예: T1은 scn 100에 값을 바꾸고 scn 300에 커밋,
     * T2는 scn 200에 값을 바꾸고 scn 250에 커밋 — 커밋은 T2(250)가 T1(300)보다 먼저라
     * Debezium은 T2를 먼저 방출한다(따라서 Kafka offset도 T2가 더 작다). 라이브 타깃의 최종
     * 상태는 "나중에 커밋한 T1의 값" — `_pos.offset` 오름차순 재생만이 이 순서를 재현한다.
     * source.scn(변경 시점 SCN)으로 정렬하면 T1(100) → T2(200) 순이 되어 라이브와 어긋난다.
     */
    @Test
    void 장기_트랜잭션에서_scn_순서와_방출_순서가_다르면_pos_offset이_우선한다() {
        Record t1CommitsLast = row("100", 0, 20L);  // 변경 scn은 작지만 커밋·방출은 나중
        Record t2CommitsFirst = row("200", 0, 10L);  // 변경 scn은 크지만 커밋·방출은 먼저

        List<Record> replay = new ArrayList<>(List.of(t1CommitsLast, t2CommitsFirst));
        replay.sort(RecoveryJob.REPLAY_ORDER);

        // scn 기준이었다면 t1CommitsLast(scn=100)가 먼저였겠지만, 방출 순서(offset)로는 반대
        assertIterableEquals(List.of(t2CommitsFirst, t1CommitsLast), replay);
    }

    @Test
    void 파티션별로_묶이고_파티션_안에서는_offset_오름차순() {
        Record p1Low = row("1", 1, 1L);
        Record p0High = row("2", 0, 99L);
        Record p0Low = row("3", 0, 5L);
        Record p1High = row("4", 1, 42L);

        List<Record> replay = new ArrayList<>(List.of(p1Low, p0High, p0Low, p1High));
        replay.sort(RecoveryJob.REPLAY_ORDER);

        assertIterableEquals(List.of(p0Low, p0High, p1Low, p1High), replay);
    }

    @Test
    void 파티션_경계에_걸친_진입_시각도_한_파티션_앞_경계로_스냅된다() {
        long width = RecoveryJob.PARTITION_WIDTH_MS;
        long partitionStart = 5 * width;

        // 파티션 시작 정각
        assertEquals(4 * width, RecoveryJob.lookbackBoundary(partitionStart, width));
        // 같은 파티션 안의 임의 시각 — 같은 파티션 경계로 스냅
        assertEquals(4 * width, RecoveryJob.lookbackBoundary(partitionStart + 500, width));
        // 다음 파티션 진입 직전
        assertEquals(4 * width, RecoveryJob.lookbackBoundary(partitionStart + width - 1, width));
    }
}
