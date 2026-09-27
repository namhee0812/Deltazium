package io.deltazium.backend.registry;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 파일명 : DbTypeTest.java
 * 작성일자 : 26. 09. 22.
 * 작성자 : 최남희
 * 설명 : 식별자 정규화(normalizeIdentifier)·폴딩(foldIdentifier) 단위 테스트.
 * PG 타깃 검증(2026-09-22)에서 발견한 결함 D1 회귀 방지 — 두 메서드는 용도가 다르다
 * (normalizeIdentifier는 소스 딕셔너리 조회용, foldIdentifier는 타깃 저장값 전용,
 * architecture.md 8절, DbType.java 참고).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 22.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 09. 28.       | 최남희  | SingleStore 타깃 지원 — foldIdentifier 3분기(원문 유지)·
 * |                          | quoteIdentifier(백틱)·sourceCapable·jdbcUrl 테스트 추가
 * --------------------------------------------------
 */
class DbTypeTest {

    @Test
    void foldIdentifier_Oracle은_대문자로_접는다() {
        assertThat(DbType.ORACLE.foldIdentifier("cdc_tmp")).isEqualTo("CDC_TMP");
        assertThat(DbType.ORACLE.foldIdentifier("Cdc_Tmp")).isEqualTo("CDC_TMP");
    }

    @Test
    void foldIdentifier_PostgreSQL은_소문자로_접는다() {
        assertThat(DbType.POSTGRESQL.foldIdentifier("CDC_TMP")).isEqualTo("cdc_tmp");
        assertThat(DbType.POSTGRESQL.foldIdentifier("Cdc_Tmp")).isEqualTo("cdc_tmp");
    }

    @Test
    void foldIdentifier_null은_null을_반환한다() {
        assertThat(DbType.ORACLE.foldIdentifier(null)).isNull();
        assertThat(DbType.POSTGRESQL.foldIdentifier(null)).isNull();
    }

    @Test
    void normalizeIdentifier는_foldIdentifier와_다르다_PostgreSQL은_원문_유지() {
        // 소스 딕셔너리 조회용 — mixed-case 소스 테이블을 실제 카탈로그 값과 맞추기 위해
        // PostgreSQL은 원문을 유지한다(foldIdentifier처럼 소문자로 접지 않는다).
        assertThat(DbType.POSTGRESQL.normalizeIdentifier("Mixed_Case")).isEqualTo("Mixed_Case");
        assertThat(DbType.ORACLE.normalizeIdentifier("cdc_tmp")).isEqualTo("CDC_TMP");
    }

    @Test
    void foldIdentifier_SingleStore는_원문을_그대로_유지한다() {
        // SingleStore는 식별자 대소문자를 보존하는 DB라 "접는 형태" 자체가 없다
        // (실측 9.0.44: CREATE TABLE MixedCase 후 select mixedcase는 1146 오류).
        assertThat(DbType.SINGLESTORE.foldIdentifier("Cdc_Tmp")).isEqualTo("Cdc_Tmp");
        assertThat(DbType.SINGLESTORE.foldIdentifier("cdc_tmp")).isEqualTo("cdc_tmp");
    }

    @Test
    void quoteIdentifier_SingleStore는_백틱_그_외는_큰따옴표() {
        assertThat(DbType.SINGLESTORE.quoteIdentifier("orders")).isEqualTo("`orders`");
        assertThat(DbType.ORACLE.quoteIdentifier("ORDERS")).isEqualTo("\"ORDERS\"");
        assertThat(DbType.POSTGRESQL.quoteIdentifier("orders")).isEqualTo("\"orders\"");
    }

    @Test
    void sourceCapable은_SingleStore만_false다() {
        assertThat(DbType.ORACLE.sourceCapable()).isTrue();
        assertThat(DbType.POSTGRESQL.sourceCapable()).isTrue();
        assertThat(DbType.SINGLESTORE.sourceCapable()).isFalse();
    }

    @Test
    void jdbcUrl은_SingleStore일_때_mysql_URL을_만든다() {
        DbConnection c = new DbConnection(null, "ss-tgt", "SINGLESTORE", "TARGET",
                "127.0.0.1", 3307, "cdc_tgt", "root", "secret");
        assertThat(c.jdbcUrl()).isEqualTo(
                "jdbc:mysql://127.0.0.1:3307/cdc_tgt?useSSL=false&allowPublicKeyRetrieval=true");
    }
}
