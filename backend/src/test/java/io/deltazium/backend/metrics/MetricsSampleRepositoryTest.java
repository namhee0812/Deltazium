package io.deltazium.backend.metrics;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

import io.deltazium.backend.config.MyBatisConfig;
import org.junit.jupiter.api.Test;
import org.mybatis.spring.boot.test.autoconfigure.MybatisTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.sql.init.SqlInitializationAutoConfiguration;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 파일명 : MetricsSampleRepositoryTest.java
 * 작성일자 : 26. 08. 06.
 * 작성자 : 최남희
 * 설명 : 시계열 저장소 테스트 — SQL 집계(series)와 MIN→HOUR 롤업의
 * 집계 함수(합/최대)·멱등성(NOT EXISTS, 재실행 시 중복 없음) 검증. H2 방언 경로.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@MybatisTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ImportAutoConfiguration(SqlInitializationAutoConfiguration.class)
@Import(MyBatisConfig.class)
class MetricsSampleRepositoryTest {

    @Autowired
    MetricsSampleRepository repository;

    private final LocalDateTime h0 = LocalDateTime.now()
            .minusHours(3).truncatedTo(ChronoUnit.HOURS);

    @Test
    void series는_전_테이블_합_또는_특정_토픽만_집계한다() {
        repository.insert(h0, "PUBLISH", "dz.CDC.T1", 100, null, "MIN");
        repository.insert(h0, "PUBLISH", "dz.CDC.T2", 40, null, "MIN");

        List<SeriesRow> all = repository.series("MIN", h0.minusMinutes(1), List.of("PUBLISH"), null);
        assertThat(all).hasSize(1);
        assertThat(all.get(0).agg()).isEqualTo(140);

        List<SeriesRow> one = repository.series("MIN", h0.minusMinutes(1), List.of("PUBLISH"), "dz.CDC.T1");
        assertThat(one.get(0).agg()).isEqualTo(100);
    }

    @Test
    void 시간_롤업은_처리량을_합치고_lag는_최대를_보존하며_재실행해도_중복이_없다() {
        repository.insert(h0.plusMinutes(1), "PUBLISH", "dz.CDC.T1", 10, null, "MIN");
        repository.insert(h0.plusMinutes(2), "PUBLISH", "dz.CDC.T1", 30, null, "MIN");
        repository.insert(h0.plusMinutes(1), "LAG_JDBC", "dz.CDC.T1", 5, null, "MIN");
        repository.insert(h0.plusMinutes(2), "LAG_JDBC", "dz.CDC.T1", 999, null, "MIN");
        repository.insert(h0.plusMinutes(3), "LAG_JDBC", "dz.CDC.T1", 0, null, "MIN");

        LocalDateTime cutoff = h0.plusHours(1);
        int first = repository.rollupHourSum(cutoff) + repository.rollupHourMax(cutoff);
        assertThat(first).isEqualTo(2);

        List<SeriesRow> thr = repository.series("HOUR", h0.minusMinutes(1), List.of("PUBLISH"), null);
        assertThat(thr.get(0).ts()).isEqualTo(h0);
        assertThat(thr.get(0).agg()).isEqualTo(40);   // 합
        List<SeriesRow> lag = repository.series("HOUR", h0.minusMinutes(1), List.of("LAG_JDBC"), null);
        assertThat(lag.get(0).agg()).isEqualTo(999);  // 구간 최대 — 스파이크 보존

        // 멱등: 다시 돌려도 새 행 없음
        int second = repository.rollupHourSum(cutoff) + repository.rollupHourMax(cutoff);
        assertThat(second).isZero();
    }

    @Test
    void 미완결_구간은_롤업하지_않는다() {
        LocalDateTime now = LocalDateTime.now();
        repository.insert(now, "PUBLISH", "dz.CDC.T1", 10, null, "MIN");
        int rolled = repository.rollupHourSum(now.truncatedTo(ChronoUnit.HOURS));
        assertThat(rolled).isZero(); // 현재 시간의 샘플은 cutoff 이후 — 대상 아님
    }
}
