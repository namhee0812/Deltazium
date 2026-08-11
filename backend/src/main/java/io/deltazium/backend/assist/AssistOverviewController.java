package io.deltazium.backend.assist;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 파일명 : AssistOverviewController.java
 * 작성일자 : 26. 08. 11.
 * 작성자 : 최남희
 * 설명 : AI 진단 어시스턴트의 진입점 REST API (조회 전용 — GET만 둔다).
 * 대상 미지정 질문("왜 CDC가 멈췄어?")에서 AI가 이 한 번의 호출로 이상 지점을 특정한 뒤,
 * 로그 검색(/api/assist/logs) 등 기존 도구로 파고든다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 11.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@RestController
@RequestMapping("/api/assist")
public class AssistOverviewController {

    private final OverviewService service;

    public AssistOverviewController(OverviewService service) {
        this.service = service;
    }

    /**
     * 전체 상태 한 장 요약. 파라미터 없음.
     * 소스(Connect/Kafka/DB) 일부가 죽어 있어도 500이 아니라 200으로 응답한다 —
     * 실패한 소스는 sources에 UNREACHABLE, 해당 섹션은 null.
     */
    @GetMapping("/overview")
    public OverviewResult overview() {
        return service.overview();
    }
}
