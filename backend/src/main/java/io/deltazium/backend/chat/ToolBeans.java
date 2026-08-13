package io.deltazium.backend.chat;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.deltazium.backend.assist.LogSearchService;
import io.deltazium.backend.assist.OverviewService;

/**
 * 파일명 : ToolBeans.java
 * 작성일자 : 26. 08. 12.
 * 작성자 : 최남희
 * 설명 : GetOverviewTool·SearchLogsTool(Claude 도구 클래스)이 Spring 빈에 접근하기 위한 정적 브릿지.
 *
 * <p>
 * <b>결정 필요(구현 중 확인된 SDK 제약):</b> BetaToolRunner의 {@code addTool(Class)}는
 * 도구 클래스 인스턴스를 Jackson이 기본 생성자로 매 호출마다 새로 만든다
 * (SDK 내부 {@code RunnableTool.FromClass.run()}이 매번 새 인스턴스를 역직렬화 — 실제
 * anthropic-java-core:2.53.0 바이트코드로 확인). 생성자 주입 경로가 없어 OverviewService·
 * LogSearchService를 도구 클래스에 직접 넣을 수 없다.
 * 이 두 서비스는 상태 없는 읽기 전용 싱글턴이라 정적 필드 공유가 요청 간 안전하다고 보고
 * 이 방식을 택했다 — 다만 이것이 "우회"로 보이는지, 더 나은 대안이 있는지는 확인이 필요하다.
 * ChatService 생성자(Spring 컨테이너가 기동 시 한 번 생성)에서 값을 채운다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 12.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
final class ToolBeans {

    static volatile OverviewService overviewService;
    static volatile LogSearchService logSearchService;
    static volatile ObjectMapper objectMapper;

    private ToolBeans() {
    }

    /** 도구 실행 실패를 예외 대신 {"error": "..."} JSON으로 안전하게 만든다. */
    static String errorJson(String message) {
        ObjectMapper mapper = objectMapper;
        if (mapper != null) {
            try {
                return mapper.writeValueAsString(Map.of("error", message == null ? "알 수 없는 오류" : message));
            } catch (Exception ignored) {
                // 아래 수동 이스케이프로 폴백
            }
        }
        String safe = message == null ? "알 수 없는 오류" : message;
        String escaped = safe.replace("\\", "\\\\").replace("\"", "\\\"");
        return "{\"error\":\"" + escaped + "\"}";
    }
}
