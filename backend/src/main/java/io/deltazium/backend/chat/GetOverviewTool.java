package io.deltazium.backend.chat;

import java.util.function.Supplier;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import org.springframework.lang.Nullable;

import io.deltazium.backend.assist.OverviewService;

/**
 * 파일명 : GetOverviewTool.java
 * 작성일자 : 26. 08. 12.
 * 작성자 : 최남희
 * 설명 : Claude tool use 도구 1 — assist 패키지 OverviewService를 호출해 CDC 파이프라인
 * 전체 상태를 JSON으로 돌려준다. 진단은 반드시 이 도구로 시작한다(시스템 프롬프트 지시).
 *
 * <p>
 * 파라미터가 없는 도구지만, anthropic-java-core:2.53.0은 로컬 스키마 검증에서
 * {@code properties}가 빈 도구를 거부한다(RunnableTool.FromClass가 항상
 * JsonSchemaLocalValidation.YES로 재검증 — addTool(Class, validation) 인자와 무관).
 * 그래서 사용하지 않는 nullable 필드 하나를 둔다.
 *
 * <p>
 * 인스턴스는 Jackson이 매 호출마다 기본 생성자로 새로 만든다({@link ToolBeans} 참고) —
 * Spring 빈은 정적 브릿지로만 접근 가능하다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 12.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@JsonClassDescription("CDC 파이프라인 전체 상태 요약. 커넥터 실패/일시정지/기타, lag 초과 테이블, "
        + "최근 ERROR, 승인 대기 DDL, 소스 도달성을 한 번에 돌려준다. 진단은 반드시 여기서 시작하라.")
public class GetOverviewTool implements Supplier<String> {

    @Nullable
    @JsonPropertyDescription("사용하지 않음 — 이 도구는 파라미터가 없다. 값을 채우지 말 것.")
    public String unused;

    @Override
    public String get() {
        OverviewService overviewService = ToolBeans.overviewService;
        if (overviewService == null || ToolBeans.objectMapper == null) {
            return ToolBeans.errorJson("overview 서비스가 초기화되지 않았다");
        }
        try {
            return ToolBeans.objectMapper.writeValueAsString(overviewService.overview());
        } catch (Exception e) {
            return ToolBeans.errorJson("overview 조회 실패: " + e.getMessage());
        }
    }
}
