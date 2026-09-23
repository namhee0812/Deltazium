package io.deltazium.backend.recovery;

import java.util.List;
import java.util.stream.IntStream;

import io.deltazium.backend.registry.DbType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 파일명 : ChecksumSqlTest.java
 * 작성일자 : 26. 09. 23.
 * 작성자 : 최남희
 * 설명 : 체크섬 SQL 생성 단위 테스트 — 결함 1(206컬럼 ORA-01489, PG 미지원) 회귀 방지.
 * 컬럼당 기여분을 ~11바이트로 줄이는 컬럼별 해시 방식이 300개 초과에서도(예: 450컬럼)
 * 묶음(ORACLE_CHUNK_SIZE) 경계 안에서 VARCHAR2 4000바이트를 넘지 않는지 검증한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 23.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
class ChecksumSqlTest {

    @Test
    void 오라클_체크섬은_컬럼별_해시를_먼저_내고_행_해시로_잇는다() {
        String sql = ChecksumSql.forOracle("TGT", "T1", List.of("ID", "AMOUNT"));
        assertThat(sql).isEqualTo(
                "SELECT COUNT(*), NVL(SUM(ORA_HASH("
                + "NVL(TO_CHAR(ORA_HASH(\"ID\")), '~null~') || '|' || "
                + "NVL(TO_CHAR(ORA_HASH(\"AMOUNT\")), '~null~')"
                + ")), 0) FROM TGT.T1");
    }

    @Test
    void 오라클_200컬럼_이하는_묶지_않는다() {
        List<String> cols = IntStream.rangeClosed(1, 200).mapToObj(i -> "C" + i).toList();
        String sql = ChecksumSql.forOracle("CDC", "T", cols);

        assertThat(countOccurrences(sql, "TO_CHAR(ORA_HASH(\"")).isEqualTo(200);
        // 묶음이 없으면 컬럼 해시들을 감싸는 ORA_HASH는 최외곽 행 해시 하나뿐이다
        assertThat(countOccurrences(sql, "ORA_HASH(")).isEqualTo(201);
    }

    @Test
    void 오라클_206컬럼은_200개씩_두_묶음의_해시로_행_해시를_낸다() {
        // 실제 결함(NH_CDC_TEST_5, ORA-01489)과 같은 컬럼 수 — 200 + 6으로 묶인다
        List<String> cols = IntStream.rangeClosed(1, 206).mapToObj(i -> "C" + i).toList();
        String sql = ChecksumSql.forOracle("CDC", "NH_CDC_TEST_5", cols);

        // 묶음 해시 표현 TO_CHAR(ORA_HASH(...))이 정확히 2개(200개 묶음 + 6개 묶음)
        assertThat(countOccurrences(sql, "TO_CHAR(ORA_HASH(NVL")).isEqualTo(2);
        assertThat(sql).startsWith("SELECT COUNT(*), NVL(SUM(ORA_HASH(TO_CHAR(ORA_HASH(");
        assertThat(sql).endsWith("FROM CDC.NH_CDC_TEST_5");

        묶음_경계_안에서_4000바이트를_넘지_않는다(cols);
    }

    @Test
    void 오라클_450컬럼은_200_200_50으로_세_묶음의_해시를_낸다() {
        List<String> cols = IntStream.rangeClosed(1, 450).mapToObj(i -> "COLUMN_" + i).toList();
        String sql = ChecksumSql.forOracle("CDC", "WIDE", cols);

        assertThat(countOccurrences(sql, "TO_CHAR(ORA_HASH(NVL")).isEqualTo(3);
        묶음_경계_안에서_4000바이트를_넘지_않는다(cols);
    }

    /**
     * 실제 VARCHAR2 4000바이트 한도는 컬럼 "값"(런타임 데이터)에 좌우되지만, 컬럼별 해시가
     * 항상 10자리 이하 숫자거나 '~null~'(6자)이므로 한 묶음의 최대 바이트 수는
     * "묶음 컬럼 수 * (10자리 숫자 + 구분자 1바이트)"로 상한이 고정된다 — 이 상한이 4000
     * 미만인지 검증한다 (ORACLE_CHUNK_SIZE=200 → 200*11=2200).
     */
    private void 묶음_경계_안에서_4000바이트를_넘지_않는다(List<String> cols) {
        int chunkSize = ChecksumSql.ORACLE_CHUNK_SIZE;
        int chunks = (cols.size() + chunkSize - 1) / chunkSize;
        for (int i = 0; i < chunks; i++) {
            int size = Math.min(chunkSize, cols.size() - i * chunkSize);
            int worstCaseBytes = size * 11; // 10자리 숫자 + '|' 구분자 1바이트
            assertThat(worstCaseBytes).isLessThan(4000);
        }
    }

    @Test
    void 포스트그레스_체크섬은_md5_기반이고_식별자를_따옴표로_감싼다() {
        String sql = ChecksumSql.forPostgres("tgt", "t1", List.of("id", "amount"));
        assertThat(sql).isEqualTo(
                "SELECT COUNT(*), COALESCE(SUM(('x' || left(md5(concat_ws('|', "
                + "coalesce(\"id\"::text, '~null~'), coalesce(\"amount\"::text, '~null~')"
                + ")), 8))::bit(32)::bigint), 0) FROM \"tgt\".\"t1\"");
    }

    @Test
    void 이종_조합은_컬럼_없이_행_수만_비교한다() {
        assertThat(ChecksumSql.rowCountOnly(DbType.ORACLE, "CDC", "T1"))
                .isEqualTo("SELECT COUNT(*) FROM CDC.T1");
        assertThat(ChecksumSql.rowCountOnly(DbType.POSTGRESQL, "cdc", "t1"))
                .isEqualTo("SELECT COUNT(*) FROM \"cdc\".\"t1\"");
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) != -1) {
            count++;
            idx += needle.length();
        }
        return count;
    }
}
