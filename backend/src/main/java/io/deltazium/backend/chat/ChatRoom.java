package io.deltazium.backend.chat;

import java.time.OffsetDateTime;

/**
 * 파일명 : ChatRoom.java
 * 작성일자 : 26. 10. 26.
 * 작성자 : 최남희
 * 설명 : 대화방 목록 한 건. messageCount는 chat_message 집계.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 26.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public record ChatRoom(long id, String title, OffsetDateTime createdAt, OffsetDateTime lastActiveAt, int messageCount) {
}
