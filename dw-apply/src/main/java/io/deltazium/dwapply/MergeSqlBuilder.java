package io.deltazium.dwapply;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 파일명 : MergeSqlBuilder.java
 * 작성일자 : 26. 10. 19.
 * 작성자 : 최남희
 * 설명 : 테이블 1개의 배치 MERGE SQL 생성. Volume 파일을 read_files로 직접 읽는다(COPY INTO·
 * 스테이징 테이블 없음). 소스는 PK별 1건만 남긴다 — 한 배치에 같은 키의 이벤트가 둘 이상이면
 * Databricks가 DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE로 실패하므로 필수.
 * 같은 키는 같은 파티션에 들어오므로 (_part, _offset 내림차순)의 첫 행이 최신이다.
 * 식별자는 전부 백틱, UPDATE SET은 키 외 컬럼을 명시 나열한다(SET * 금지).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | read_files에 timestampFormat·dateFormat 명시(Flattener 출력과 일치 — Spark 기본 추론 의존 제거)
 * --------------------------------------------------
 */
public final class MergeSqlBuilder {

    /** Flattener.TS_FORMAT과 동일한 Spark 패턴 — JSON 리더 기본 추론에 맡기지 않는다. */
    static final String TS_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS";

    private MergeSqlBuilder() {
    }

    /**
     * @param columnTypes 타깃 컬럼명(소문자) → full_data_type (information_schema)
     * @param files       이번 배치가 올린 Volume 파일 경로(1개 이상)
     */
    public static String build(String catalog, TableSpec spec, Map<String, String> columnTypes,
                               List<String> files) {
        List<String> schemaParts = new ArrayList<>();
        List<String> allCols = new ArrayList<>();
        for (ColumnMapping m : spec.columns()) {
            String type = columnTypes.get(m.target().toLowerCase());
            if (type == null) {
                throw new IllegalStateException("타깃 테이블에 컬럼 없음: " + spec.key() + "." + m.target());
            }
            schemaParts.add(q(m.target()) + " " + type);
            allCols.add(m.target());
        }
        schemaParts.add("`_op` STRING");
        schemaParts.add("`_offset` BIGINT");
        schemaParts.add("`_part` INT");
        String schemaLiteral = lit(String.join(", ", schemaParts));

        // 파일이 여러 개(토픽·파티션 여럿)면 UNION ALL로 합친다 — 한 파일이면 단일 read_files
        List<String> reads = files.stream()
                .map(f -> "read_files('" + lit(f) + "', format => 'json', schema => '" + schemaLiteral
                        + "', timestampFormat => '" + TS_FORMAT + "', dateFormat => 'yyyy-MM-dd'"
                        + ", schemaEvolutionMode => 'none')")
                .toList();
        String from = reads.size() == 1 ? reads.get(0)
                : "(" + reads.stream().map(r -> "SELECT * FROM " + r).collect(Collectors.joining(" UNION ALL ")) + ")";

        String keys = spec.keyColumns().stream().map(MergeSqlBuilder::q).collect(Collectors.joining(", "));
        String on = spec.keyColumns().stream().map(k -> "t." + q(k) + " = s." + q(k))
                .collect(Collectors.joining(" AND "));
        List<String> values = spec.valueColumns();

        StringBuilder sb = new StringBuilder();
        sb.append("MERGE INTO ").append(q(catalog)).append('.').append(q(spec.schema())).append('.')
                .append(q(spec.table())).append(" t\n");
        sb.append("USING (SELECT * FROM (\n");
        sb.append("  SELECT *, ROW_NUMBER() OVER (PARTITION BY ").append(keys)
                .append(" ORDER BY `_part`, `_offset` DESC) AS `_rn`\n");
        sb.append("  FROM ").append(from).append("\n");
        sb.append(") WHERE `_rn` = 1) s\n");
        sb.append("ON ").append(on).append("\n");
        sb.append("WHEN MATCHED AND s.`_op` = 'd' THEN DELETE\n");
        if (!values.isEmpty()) {
            sb.append("WHEN MATCHED THEN UPDATE SET ")
                    .append(values.stream().map(c -> q(c) + " = s." + q(c)).collect(Collectors.joining(", ")))
                    .append("\n");
        }
        sb.append("WHEN NOT MATCHED AND s.`_op` <> 'd' THEN INSERT (")
                .append(allCols.stream().map(MergeSqlBuilder::q).collect(Collectors.joining(", ")))
                .append(") VALUES (")
                .append(allCols.stream().map(c -> "s." + q(c)).collect(Collectors.joining(", ")))
                .append(")");
        return sb.toString();
    }

    static String q(String ident) {
        return "`" + ident.replace("`", "``") + "`";
    }

    /** SQL 문자열 리터럴 내부용 이스케이프 (' → \'  / \ → \\). Databricks 기본 이스케이프 규칙. */
    static String lit(String s) {
        return s.replace("\\", "\\\\").replace("'", "\\'");
    }
}
