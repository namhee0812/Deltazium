package io.deltazium.backend.recovery;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import io.deltazium.backend.registry.DbType;

/**
 * 파일명 : ChecksumSql.java
 * 작성일자 : 26. 09. 23.
 * 작성자 : 최남희
 * 설명 : 정합 검증 체크섬 SQL 생성 — DbType별 분리 (architecture.md 6.4절 ⑤, docs/internals.md
 * "정합 검증 체크섬" 절).
 * <p>
 * Oracle: 전 컬럼을 한 문자열로 이어 ORA_HASH 하면 컬럼이 많은 테이블(예: 206개)에서
 * VARCHAR2 4000바이트 한도(ORA-01489)를 넘는다. 컬럼별로 먼저 ORA_HASH를 내(10자리 이하
 * 숫자 문자열)로 줄인 뒤 그것들을 이어 행 해시를 낸다 — 컬럼당 기여분이 ~11바이트로 줄어
 * 200컬럼까지는 안전(2200바이트)하다. 그래도 넘을 수 있는 컬럼 수(200 초과)는
 * {@link #ORACLE_CHUNK_SIZE}씩 묶어 묶음 해시를 낸 뒤, 묶음 해시들을 다시 이어 행 해시를
 * 낸다(해시의 해시) — 묶음이 몇 개든 묶음 해시 하나는 10자리 이하라 다시 4000을 넘지 않는다.
 * <p>
 * PostgreSQL: text는 length 한도가 없어 묶음이 필요 없다 — concat_ws로 전 컬럼을 이은 뒤
 * md5 앞 8자리(32bit)를 정수로 변환해 SUM한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 23.       | 최남희  | 최초 생성 — 결함 1(206컬럼 ORA-01489, PG 미지원) 수정으로
 * |                          | 기존 RecoveryService.checksumSql(Oracle 전용, 컬럼당 미해시)을
 * |                          | 분리·대체
 * --------------------------------------------------
 */
final class ChecksumSql {

    /** Oracle 컬럼 해시 묶음 크기 — 200 * (10자리 숫자 + 구분자 1바이트) = 2200 &lt; 4000,
     * 여유를 둔 상수 (2026-09-23 결정, docs/internals.md). */
    static final int ORACLE_CHUNK_SIZE = 200;

    private ChecksumSql() {
    }

    /** Oracle 체크섬 SQL — 컬럼별 ORA_HASH 후 이어붙여 행 해시(필요 시 묶음의 해시). */
    static String forOracle(String schema, String table, List<String> cols) {
        List<List<String>> chunks = chunkColumns(cols);
        List<String> chunkHashes = new ArrayList<>();
        for (List<String> chunk : chunks) {
            String joined = chunk.stream()
                    .map(c -> "NVL(TO_CHAR(ORA_HASH(\"" + c + "\")), '~null~')")
                    .collect(Collectors.joining(" || '|' || "));
            chunkHashes.add(chunks.size() == 1 ? joined : "TO_CHAR(ORA_HASH(" + joined + "))");
        }
        String rowExpr = "ORA_HASH(" + String.join(" || '|' || ", chunkHashes) + ")";
        return "SELECT COUNT(*), NVL(SUM(" + rowExpr + "), 0) FROM " + fromClause(DbType.ORACLE, schema, table);
    }

    /** PostgreSQL 체크섬 SQL — concat_ws로 전 컬럼을 이어 md5 앞 8자리를 32bit 정수로 SUM. */
    static String forPostgres(String schema, String table, List<String> cols) {
        String concat = cols.stream()
                .map(c -> "coalesce(\"" + c + "\"::text, '~null~')")
                .collect(Collectors.joining(", "));
        String rowHash = "('x' || left(md5(concat_ws('|', " + concat + ")), 8))::bit(32)::bigint";
        return "SELECT COUNT(*), COALESCE(SUM(" + rowHash + "), 0) FROM " + fromClause(DbType.POSTGRESQL, schema, table);
    }

    /** 이종 DB 조합 — 체크섬 없이 행 수만 비교. */
    static String rowCountOnly(DbType type, String schema, String table) {
        return "SELECT COUNT(*) FROM " + fromClause(type, schema, table);
    }

    /** {@link #ORACLE_CHUNK_SIZE} 이하면 묶지 않고 컬럼 전체를 한 묶음으로 돌려준다. */
    private static List<List<String>> chunkColumns(List<String> cols) {
        if (cols.size() <= ORACLE_CHUNK_SIZE) {
            return List.of(cols);
        }
        List<List<String>> chunks = new ArrayList<>();
        for (int i = 0; i < cols.size(); i += ORACLE_CHUNK_SIZE) {
            chunks.add(cols.subList(i, Math.min(i + ORACLE_CHUNK_SIZE, cols.size())));
        }
        return chunks;
    }

    /** 타깃 저장값은 이미 DbType.foldIdentifier로 폴딩돼 있다(architecture.md 8절) —
     * PostgreSQL은 폴딩된(소문자) 원문을 그대로 따옴표로 감싸면 실제 카탈로그 식별자와 일치한다. */
    private static String fromClause(DbType type, String schema, String table) {
        return type == DbType.POSTGRESQL
                ? "\"" + schema + "\".\"" + table + "\""
                : schema + "." + table;
    }
}
