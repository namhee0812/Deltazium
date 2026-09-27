package io.deltazium.backend.metrics;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 파일명 : SystemSettingsController.java
 * 작성일자 : 26. 09. 27.
 * 작성자 : 최남희
 * 설명 : UI가 하드코딩 대신 조회하는 시스템 설정값 API — GET /api/system/settings.
 * 첫 항목은 lag 경고 임계(레코드 건수)로, 대시보드 KPI "최대 lag" 카드와 테이블
 * 모니터링 그리드가 각자 하드코딩(LAG_WARN=100)하던 값을 여기 하나로 모은다.
 * 마운트 시 1회만 조회하면 되는 정적 설정이라 폴링 대상이 아니다. 비밀값은 담지 않는다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 27.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@RestController
@RequestMapping("/api/system")
public class SystemSettingsController {

    public record SystemSettings(int lagWarnRecords) {
    }

    private final int lagWarnRecords;

    public SystemSettingsController(@Value("${deltazium.lag-warn-records:100}") int lagWarnRecords) {
        this.lagWarnRecords = lagWarnRecords;
    }

    @GetMapping("/settings")
    public SystemSettings settings() {
        return new SystemSettings(lagWarnRecords);
    }
}
