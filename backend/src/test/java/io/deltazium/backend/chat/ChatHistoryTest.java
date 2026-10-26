package io.deltazium.backend.chat;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 파일명 : ChatHistoryTest.java
 * 작성일자 : 26. 10. 26.
 * 작성자 : 최남희
 * 설명 : 대화 이력 조립(ChatService.buildMessages)·제목 생성·보관 주기 정리 경계 단위 테스트.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 26.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
class ChatHistoryTest {

    private static ChatMessage m(String role, String text) {
        return new ChatMessage(role, text, null);
    }

    @Test
    void buildMessages_emptyHistory_onlyQuestion() {
        var out = ChatService.buildMessages(List.of(), "질문");
        assertThat(out).extracting(ChatMessage::role, ChatMessage::content).containsExactly(
                org.assertj.core.groups.Tuple.tuple("user", "질문"));
    }

    @Test
    void buildMessages_keepsOnlyRecent20Turns() {
        List<ChatMessage> history = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            history.add(m("user", "q" + i));
            history.add(m("assistant", "a" + i));
        }
        var out = ChatService.buildMessages(history, "now");
        assertThat(out).hasSize(41);
        assertThat(out.get(0).content()).isEqualTo("q10");
        assertThat(out.get(39).content()).isEqualTo("a29");
        assertThat(out.get(40).content()).isEqualTo("now");
    }

    @Test
    void buildMessages_alternates_dropsUnansweredUser() {
        // q1은 답변 실패로 assistant가 빠진 턴 → 앞의 user(q1)를 버린다
        var out = ChatService.buildMessages(List.of(
                m("user", "q0"), m("assistant", "a0"), m("user", "q1"), m("user", "q2"), m("assistant", "a2")), "q3");
        assertThat(out).extracting(ChatMessage::content).containsExactly("q0", "a0", "q2", "a2", "q3");
    }

    @Test
    void buildMessages_trailingUnansweredUser_isDropped_andLeadingAssistantTrimmed() {
        var out = ChatService.buildMessages(List.of(
                m("assistant", "orphan"), m("user", "q0"), m("assistant", "a0"), m("user", "qFail")), "q1");
        assertThat(out).extracting(ChatMessage::content).containsExactly("q0", "a0", "q1");
    }

    @Test
    void titleOf_firstLine40chars_noNewlines() {
        assertThat(ChatService.titleOf("a\nb\r\n c")).isEqualTo("a b c");
        assertThat(ChatService.titleOf("x".repeat(100))).hasSize(40);
    }

    @Test
    void cleaner_cutoffIsNowMinusRetentionDays_strictlyBefore() {
        OffsetDateTime now = OffsetDateTime.parse("2026-10-26T12:00:00+09:00");
        assertThat(ChatRoomCleaner.cutoff(now, 3)).isEqualTo(OffsetDateTime.parse("2026-10-23T12:00:00+09:00"));

        ChatRoomRepository repo = mock(ChatRoomRepository.class);
        when(repo.retentionDays()).thenReturn(7);
        when(repo.deleteInactiveBefore(any())).thenReturn(2);
        assertThat(new ChatRoomCleaner(repo).clean(now)).isEqualTo(2);
        verify(repo).deleteInactiveBefore(OffsetDateTime.parse("2026-10-19T12:00:00+09:00"));
    }
}
