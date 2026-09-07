package io.deltazium.backend.ddl;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 파일명 : SchemaFingerprintPollerTest.java
 * 작성일자 : 26. 09. 07.
 * 작성자 : 최남희
 * 설명 : SchemaFingerprintPoller.buildEventPayload 단위 테스트 — PG 소스 실 배선 스모크에서
 * ddl_text에 요약+초안이 세미콜론과 함께 섞여 저장돼 승인 시 그대로 실행되며 ORA-00900이
 * 났던 결함(ddl_events id=39, 타깃 Oracle)의 회귀 방지.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 최초 생성 — PG 소스 실 배선 스모크 수정
 * --------------------------------------------------
 */
class SchemaFingerprintPollerTest {

    @Test
    void FINGERPRINT_이벤트_ddl_text는_단일_실행문이고_세미콜론이_없다() {
        var changes = List.of(new SchemaFingerprint.FieldChange(SchemaFingerprint.ChangeKind.ADDED, null,
                new SchemaFingerprint.FieldDesc("amount", "string", true, Map.of())));

        var payload = SchemaFingerprintPoller.buildEventPayload(changes, "ORACLE", "TGT", "ORDERS");

        assertThat(payload.ddlText()).doesNotContain(";").doesNotContain("\n")
                .isEqualTo("ALTER TABLE \"TGT\".\"ORDERS\" ADD (\"AMOUNT\" VARCHAR2(4000))");
        assertThat(payload.note()).contains("추가: amount(string)");
        assertThat(payload.state()).isEqualTo("DETECTED");
    }

    @Test
    void 초안이_없으면_ddl_text는_빈_문자열이고_상태는_SNAPSHOT이다() {
        var changes = List.of(new SchemaFingerprint.FieldChange(SchemaFingerprint.ChangeKind.TYPE_CHANGED,
                new SchemaFingerprint.FieldDesc("id", "int32", false, Map.of()),
                new SchemaFingerprint.FieldDesc("id", "int64", false, Map.of())));

        var payload = SchemaFingerprintPoller.buildEventPayload(changes, "ORACLE", "TGT", "T1");

        assertThat(payload.ddlText()).isEmpty();
        assertThat(payload.state()).isEqualTo("SNAPSHOT");
        assertThat(payload.note()).contains("타입변경").contains("자동 초안 없음");
    }

    @Test
    void 추가와_삭제가_섞여도_단일_문장으로_합쳐진다() {
        var changes = List.of(
                new SchemaFingerprint.FieldChange(SchemaFingerprint.ChangeKind.ADDED, null,
                        new SchemaFingerprint.FieldDesc("amount", "int32", true, Map.of())),
                new SchemaFingerprint.FieldChange(SchemaFingerprint.ChangeKind.REMOVED,
                        new SchemaFingerprint.FieldDesc("legacy", "int8", true, Map.of()), null));

        var payload = SchemaFingerprintPoller.buildEventPayload(changes, "POSTGRESQL", "tgt", "orders");

        assertThat(payload.ddlText()).doesNotContain(";")
                .isEqualTo("ALTER TABLE \"tgt\".\"orders\" ADD COLUMN \"amount\" INTEGER, DROP COLUMN \"legacy\"");
        assertThat(payload.state()).isEqualTo("DETECTED");
    }
}
