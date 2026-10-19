package io.deltazium.dwapply;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * 파일명 : MergeSqlBuilderTest.java
 * 작성일자 : 26. 10. 19.
 * 작성자 : 최남희
 * 설명 : MERGE SQL 생성 골든, 파일명 규칙, 배치 닫힘 조건.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
class MergeSqlBuilderTest {

    private static final TableSpec SPEC = new TableSpec(List.of("dz1.S.T"), "default", "orders",
            List.of("id", "seq"), List.of(new ColumnMapping("ID", "id"), new ColumnMapping("SEQ", "seq"),
                    new ColumnMapping("NAME", "name"), new ColumnMapping("AMT", "amt")));
    private static final Map<String, String> TYPES = Map.of("id", "bigint", "seq", "int",
            "name", "string", "amt", "decimal(10,2)");

    @Test
    void goldenSingleFile() {
        String sql = MergeSqlBuilder.build("workspace", SPEC, TYPES,
                List.of("/Volumes/workspace/default/v/default__orders/dz1.S.T-p0-10-20.jsonl"));
        assertEquals("""
                MERGE INTO `workspace`.`default`.`orders` t
                USING (SELECT * FROM (
                  SELECT *, ROW_NUMBER() OVER (PARTITION BY `id`, `seq` ORDER BY `_part`, `_offset` DESC) AS `_rn`
                  FROM read_files('/Volumes/workspace/default/v/default__orders/dz1.S.T-p0-10-20.jsonl', format => 'json', schema => '`id` bigint, `seq` int, `name` string, `amt` decimal(10,2), `_op` STRING, `_offset` BIGINT, `_part` INT', schemaEvolutionMode => 'none')
                ) WHERE `_rn` = 1) s
                ON t.`id` = s.`id` AND t.`seq` = s.`seq`
                WHEN MATCHED AND s.`_op` = 'd' THEN DELETE
                WHEN MATCHED THEN UPDATE SET `name` = s.`name`, `amt` = s.`amt`
                WHEN NOT MATCHED AND s.`_op` <> 'd' THEN INSERT (`id`, `seq`, `name`, `amt`) VALUES (s.`id`, s.`seq`, s.`name`, s.`amt`)""",
                sql);
    }

    @Test
    void multipleFilesUnionAll() {
        String sql = MergeSqlBuilder.build("c", SPEC, TYPES, List.of("/a.jsonl", "/b.jsonl"));
        assertTrue(sql.contains("FROM (SELECT * FROM read_files('/a.jsonl'"));
        assertTrue(sql.contains(" UNION ALL SELECT * FROM read_files('/b.jsonl'"));
    }

    @Test
    void missingTargetColumnFails() {
        assertThrows(IllegalStateException.class,
                () -> MergeSqlBuilder.build("c", SPEC, Map.of("id", "bigint"), List.of("/a")));
    }

    @Test
    void fileNameIsDeterministic() {
        assertEquals("dz1.S.T-p3-100-250.jsonl", StagingFiles.fileName("dz1.S.T", 3, 100, 250));
        assertEquals("/Volumes/c/s/v/default__orders/dz1.S.T-p3-100-250.jsonl",
                StagingFiles.path("/Volumes/c/s/v", SPEC, "dz1.S.T", 3, 100, 250));
    }

    @Test
    void batchClosesOnTimeOrBytes() {
        assertFalse(BatchPolicy.shouldClose(0, 9_999, 10, 10_000, 1_000));
        assertTrue(BatchPolicy.shouldClose(0, 10_000, 10, 10_000, 1_000));
        assertTrue(BatchPolicy.shouldClose(0, 1, 1_000, 10_000, 1_000));
    }
}
