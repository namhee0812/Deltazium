package io.deltazium.backend.recovery;

import java.util.List;

import io.deltazium.backend.registration.RegisteredTable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 파일명 : RecoveryServiceTest.java
 * 작성일자 : 26. 07. 29.
 * 작성자 : 최남희
 * 설명 : 순수 로직 검증 — 커맨드 조립·체크섬 SQL. 프로세스 기동·검증 쿼리는 리허설(E2E)에서 확인.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 29.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 09. 05.       | 최남희  | 복구 커맨드 인자를 from-scn에서 from-ts-ms로 전환 (6.2절)
 * --------------------------------------------------
 */
class RecoveryServiceTest {

    @Test
    void 체크섬_SQL은_NULL을_안전하게_다루고_컬럼을_구분자로_잇는다() {
        String sql = RecoveryService.checksumSql("TGT.T1", List.of("ID", "AMOUNT"));
        assertThat(sql).isEqualTo(
                "SELECT COUNT(*), NVL(SUM(ORA_HASH("
                + "NVL(TO_CHAR(\"ID\"), '~null~') || '|' || NVL(TO_CHAR(\"AMOUNT\"), '~null~')"
                + ")), 0) FROM TGT.T1");
    }

    @Test
    void 복구_커맨드에_카탈로그와_재생_인자가_전부_들어간다() {
        var iceberg = new io.deltazium.backend.iceberg.IcebergProperties(
                "jdbc:postgresql://localhost:5433/iceberg_catalog", "u", "p",
                "s3://wh/warehouse", "http://localhost:9010", "ak", "sk");
        var changelog = new io.deltazium.backend.iceberg.ChangelogTableService(iceberg, "dz");
        RecoveryService service = new RecoveryService(null, null, null, null, null,
                changelog, iceberg, null, null, "localhost:9092", "/opt/recovery-job/bin/recovery-job", "/tmp");

        RegisteredTable table = new RegisteredTable(1L, "CDC", "AUTO_100", 1, 2, null, null);
        List<String> cmd = service.buildCommand(table, 1753300000000L,
                List.of("ID"), "dz-recovery.cdc_auto_100", "/tmp/x.log");

        assertThat(cmd.get(0)).isEqualTo("/opt/recovery-job/bin/recovery-job");
        assertThat(cmd).contains(
                "table=changelog_dz.cdc_auto_100",
                "from-ts-ms=1753300000000",
                "key-columns=ID",
                "topic=dz-recovery.cdc_auto_100",
                "bootstrap=localhost:9092",
                "catalog-uri=jdbc:postgresql://localhost:5433/iceberg_catalog");
    }
}
