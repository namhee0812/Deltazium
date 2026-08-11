package io.deltazium.backend.assist;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 파일명 : OverviewResult.java
 * 작성일자 : 26. 08. 11.
 * 작성자 : 최남희
 * 설명 : AI 진단 어시스턴트 overview 응답 DTO.
 * 원칙: <b>정상은 개수만, 비정상만 상세를</b> — 응답 크기가 곧 토큰 비용이다.
 * "멈췄다"의 유형(커넥터 FAILED / task만 FAILED / 사람이 PAUSED / lag 누적 / DDL 승인 대기)이
 * 응답 구조에서 바로 구분되도록 섹션을 나눈다.
 *
 * <p>
 * 소스 장애 격리: 조회에 실패한 소스의 섹션은 null이고, {@link Sources}에 UNREACHABLE로 표기된다.
 * AI가 "Connect 자체가 안 떠 있다"를 판단 근거로 쓸 수 있게 한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 11.       | 최남희  | 최초 생성
 * 26. 08. 11.       | 최남희  | connectors에 other 버킷 추가 (STOPPED·RESTARTING·미지 상태 노출)
 * --------------------------------------------------
 *
 * @param connectors   커넥터 요약 — Connect 조회 실패 시 null
 * @param tables       등록 테이블 lag 요약 — Kafka 조회 실패 시 null
 * @param recentErrors 최근 운영 이벤트 중 ERROR — DB 조회 실패 시 null
 * @param ddl          DDL 승인 대기 — DB 조회 실패 시 null
 * @param sources      소스별 도달 가능 여부 (OK | UNREACHABLE)
 */
public record OverviewResult(
        ConnectorsSection connectors,
        TablesSection tables,
        RecentErrorsSection recentErrors,
        DdlSection ddl,
        Sources sources) {

    /**
     * 상세 목록(failed/paused/unassigned)은 각 최대 {@value OverviewService#MAX_LIST}건 —
     * 전체 개수는 항상 count 필드로 알린다 (목록이 잘려도 규모는 보이게).
     *
     * @param running connector state가 RUNNING이고 FAILED task가 없는 "건강한" 커넥터 수
     * @param other   failed/paused/unassigned 어디에도 안 걸리는 상태(STOPPED·RESTARTING·
     *                미지의 상태 문자열 전부) — 상태 원문 그대로 노출해 AI가 해석하게 한다.
     *                이 버킷이 없으면 total≠running인데 이유가 안 보이는 응답이 된다
     */
    public record ConnectorsSection(
            int total,
            int running,
            int failedCount, List<FailedConnector> failed,
            int pausedCount, List<String> paused,
            int unassignedCount, List<String> unassigned,
            int otherCount, List<OtherConnector> other) {
    }

    /** @param state Connect가 준 상태 문자열 원문 (STOPPED, RESTARTING 등) */
    public record OtherConnector(String name, String state) {
    }

    /**
     * @param connectorState 커넥터 자체 상태. RUNNING인데 이 목록에 있으면 "task만 FAILED" 유형이다
     * @param failedTasks    FAILED 상태인 task만
     */
    public record FailedConnector(String name, String connectorState, List<FailedTask> failedTasks) {
    }

    /** @param traceHead trace 앞 {@value OverviewService#TRACE_HEAD_LINES}줄. 잘렸으면 끝에 "…" */
    public record FailedTask(int id, String traceHead) {
    }

    /**
     * @param total   등록 테이블 총수 (정상 테이블은 개수로만 잡힌다)
     * @param lagging 임계(deltazium.assist.lag-threshold, offset 건수) 초과 테이블만.
     *                최대 {@value OverviewService#MAX_LIST}건 — 전체 규모는 laggingCount
     */
    public record TablesSection(int total, int laggingCount, List<LaggingTable> lagging) {
    }

    /** lag은 offset 건수다 (시간 아님). */
    public record LaggingTable(String schema, String table, String topic, long jdbcLag, long icebergLag) {
    }

    /**
     * @param count 조회 구간(최근 {@value OverviewService#ERROR_WINDOW}건) 내 ERROR 총수
     * @param items 최근 {@value OverviewService#ERROR_ITEMS}건만. detail(스택 등 대용량)은 싣지 않는다 —
     *              깊이 파려면 로그 검색(/api/assist/logs)으로
     */
    public record RecentErrorsSection(int count, List<ErrorItem> items) {
    }

    public record ErrorItem(LocalDateTime occurredAt, String schema, String table,
                            String eventType, String message) {
    }

    /**
     * @param pendingCount    state=DETECTED(승인 대기) 총수
     * @param pendingApproval 최대 {@value OverviewService#MAX_LIST}건.
     *                        승인 대기는 고장이 아니라 <b>사람의 결정을 기다리는 정지</b>다
     */
    public record DdlSection(int pendingCount, List<PendingDdl> pendingApproval) {
    }

    /** @param ddlSummary ddlText 앞 {@value OverviewService#DDL_SUMMARY_CHARS}자. 잘렸으면 끝에 "…" */
    public record PendingDdl(long id, String schema, String table, long eventTsMs, String ddlSummary) {
    }

    /** 각 값은 OK | UNREACHABLE. UNREACHABLE인 소스의 섹션은 null이다. */
    public record Sources(String connect, String kafka, String db) {
    }
}
