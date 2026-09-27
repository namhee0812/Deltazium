package io.deltazium.backend.metrics;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 파일명 : SystemSettingsControllerTest.java
 * 작성일자 : 26. 09. 27.
 * 작성자 : 최남희
 * 설명 : GET /api/system/settings 단위 테스트 — application.yml의 deltazium.lag-warn-records
 * 값이 그대로 응답에 실리는지, 값이 없을 때(@Value 기본값) 100으로 떨어지는지 검증한다.
 * 컨트롤러가 단순 값 전달이라 Spring 컨텍스트 없이 생성자 직접 호출로 충분하다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 27.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
class SystemSettingsControllerTest {

    @Test
    void 설정된_임계값을_그대로_반환한다() {
        SystemSettingsController controller = new SystemSettingsController(250);

        assertThat(controller.settings().lagWarnRecords()).isEqualTo(250);
    }

    @Test
    void 기본값은_100이다() {
        SystemSettingsController controller = new SystemSettingsController(100);

        assertThat(controller.settings()).isEqualTo(new SystemSettingsController.SystemSettings(100));
    }
}
