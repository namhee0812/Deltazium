package io.deltazium.backend.metrics;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 파일명 : MetricsRollupService.java
 * 작성일자 : 26. 08. 06.
 * 작성자 : 최남희
 * 설명 : 시계열 2단 롤업 — MIN(48h 보존) → HOUR(60일) → DAY(1년).
 * 완결된 구간만 대상이라 현재 그래프에 영향이 없고, NOT EXISTS 멱등이라
 * 몇 번을 다시 돌아도 중복이 생기지 않는다. 삭제는 롤업과 무관하게
 * 보존 기간으로만 — 롤업이 실패해도 원본은 48시간 동안 남아 재시도된다.
 * 집계: 처리량=합, lag=구간 최대(스파이크 보존), 자원=평균.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@Service
@ConditionalOnProperty(name = "deltazium.metrics-rollup.enabled",
        havingValue = "true", matchIfMissing = true)
public class MetricsRollupService {

    private static final Logger log = LoggerFactory.getLogger(MetricsRollupService.class);

    private final MetricsSampleRepository repository;

    public MetricsRollupService(MetricsSampleRepository repository) {
        this.repository = repository;
    }

    /** 매시 5분 — 완결된 시간·날짜 롤업 후 해상도별 보존 정리. */
    @Scheduled(cron = "${deltazium.metrics-rollup.cron:0 5 * * * *}")
    @Transactional
    public void rollup() {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime hourCutoff = now.truncatedTo(ChronoUnit.HOURS);
        LocalDateTime dayCutoff = now.truncatedTo(ChronoUnit.DAYS);

        int hour = repository.rollupHourSum(hourCutoff)
                + repository.rollupHourMax(hourCutoff)
                + repository.rollupHourAvg(hourCutoff);
        int day = repository.rollupDaySum(dayCutoff)
                + repository.rollupDayMax(dayCutoff)
                + repository.rollupDayAvg(dayCutoff);

        int purged = repository.deleteResolutionBefore("MIN", now.minusHours(48))
                + repository.deleteResolutionBefore("HOUR", now.minusDays(60))
                + repository.deleteResolutionBefore("DAY", now.minusDays(365));

        if (hour + day + purged > 0) {
            log.info("메트릭 롤업: HOUR {}행, DAY {}행 생성 · 보존 정리 {}행", hour, day, purged);
        }
    }
}
