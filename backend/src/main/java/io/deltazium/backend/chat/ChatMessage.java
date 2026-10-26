package io.deltazium.backend.chat;

import java.time.OffsetDateTime;

/**
 * 파일명 : ChatMessage.java
 * 작성일자 : 26. 10. 26.
 * 작성자 : 최남희
 * 설명 : 저장된 대화 메시지(user/assistant 텍스트). tool_use/tool_result는 저장하지 않는다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 26.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public record ChatMessage(String role, String content, OffsetDateTime createdAt) {
}
