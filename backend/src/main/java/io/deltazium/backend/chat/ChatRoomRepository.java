package io.deltazium.backend.chat;

import java.time.OffsetDateTime;
import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/**
 * 파일명 : ChatRoomRepository.java
 * 작성일자 : 26. 10. 26.
 * 작성자 : 최남희
 * 설명 : 대화방·메시지·설정 저장소. SQL은 resources/mappers/chat-room.xml
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 26.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@Mapper
public interface ChatRoomRepository {

    /** generated key를 받으려면 가변 홀더가 필요하다 (record는 불변) */
    class InsertRoom {
        public Long id;
        public String title;
    }

    void insertRoom(InsertRoom row);

    default long createRoom(String title) {
        InsertRoom row = new InsertRoom();
        row.title = title;
        insertRoom(row);
        return row.id;
    }

    boolean existsRoom(@Param("id") long id);

    /** 목록 — last_active_at 내림차순. */
    List<ChatRoom> findRooms();

    /** title이 아직 null인 방에만 채운다(첫 질문으로 자동 제목). */
    int setTitleIfNull(@Param("id") long id, @Param("title") String title);

    int touchRoom(@Param("id") long id);

    int deleteRoom(@Param("id") long id);

    void insertMessage(@Param("roomId") long roomId, @Param("role") String role, @Param("content") String content);

    /** 시간순 전체. */
    List<ChatMessage> findMessages(@Param("roomId") long roomId);

    /** 최근 limit건을 시간순으로. */
    List<ChatMessage> findRecentMessages(@Param("roomId") long roomId, @Param("limit") int limit);

    int retentionDays();

    int updateRetentionDays(@Param("days") int days);

    /** last_active_at &lt; cutoff 인 방 삭제(메시지는 cascade). 삭제한 방 수. */
    int deleteInactiveBefore(@Param("cutoff") OffsetDateTime cutoff);
}
