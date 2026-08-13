package io.deltazium.backend.chat;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.function.Supplier;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import org.springframework.lang.Nullable;

import io.deltazium.backend.assist.LogSearchService;
import io.deltazium.backend.assist.LogSource;

/**
 * 파일명 : SearchLogsTool.java
 * 작성일자 : 26. 08. 12.
 * 작성자 : 최남희
 * 설명 : Claude tool use 도구 2 — assist 패키지 LogSearchService를 호출해 로그를 검색한다.
 * 잘못된 source·날짜는 예외를 던지지 않고 {"error": "..."} JSON으로 돌려줘 Claude가
 * 파라미터를 고쳐 재시도하게 한다.
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
@JsonClassDescription("backend/connect/kafka/controller 로그를 기간·키워드로 검색한다. "
        + "응답의 file·lineNo를 근거로 인용하라 — 근거 없는 단정은 금지된다.")
public class SearchLogsTool implements Supplier<String> {

    @JsonPropertyDescription("로그 소스. BACKEND(이 backend 자체 로그) | CONNECT(Kafka Connect 워커) | "
            + "KAFKA(브로커) | CONTROLLER(KRaft 컨트롤러) 중 하나.")
    public String source;

    @Nullable
    @JsonPropertyDescription("검색어(대소문자 무시 부분 일치, 정규식 아님). 생략하거나 null이면 전체 줄이 대상.")
    public String q;

    @Nullable
    @JsonPropertyDescription("검색 시작일 YYYY-MM-DD(포함). 생략하면 to와 같은 날.")
    public String from;

    @Nullable
    @JsonPropertyDescription("검색 종료일 YYYY-MM-DD(포함). 생략하면 오늘.")
    public String to;

    @Nullable
    @JsonPropertyDescription("반환할 줄 수 상한. 생략하면 기본값을 쓰고, 서버가 최대 500으로 자른다.")
    public Integer limit;

    @Nullable
    @JsonPropertyDescription("매칭된 줄 뒤에 함께 반환할 문맥 줄 수(스택 트레이스 확인용). "
            + "생략하면 0, 서버가 최대 20으로 자른다.")
    public Integer contextAfter;

    @Override
    public String get() {
        LogSearchService service = ToolBeans.logSearchService;
        if (service == null) {
            return ToolBeans.errorJson("로그 검색 서비스가 초기화되지 않았다");
        }

        LogSource logSource;
        try {
            logSource = LogSource.from(source);
        } catch (IllegalArgumentException e) {
            return ToolBeans.errorJson(e.getMessage());
        }

        LocalDate fromDate;
        LocalDate toDate;
        try {
            fromDate = (from == null || from.isBlank()) ? null : LocalDate.parse(from);
            toDate = (to == null || to.isBlank()) ? null : LocalDate.parse(to);
        } catch (DateTimeParseException e) {
            return ToolBeans.errorJson("날짜 형식이 잘못됐다(YYYY-MM-DD 필요): " + e.getMessage());
        }

        int effectiveLimit = (limit == null) ? LogSearchService.DEFAULT_LIMIT : limit;
        int effectiveContextAfter = (contextAfter == null) ? 0 : contextAfter;

        try {
            var result = service.search(logSource, q, fromDate, toDate, effectiveLimit, effectiveContextAfter);
            return ToolBeans.objectMapper.writeValueAsString(result);
        } catch (IllegalArgumentException e) {
            return ToolBeans.errorJson(e.getMessage());
        } catch (Exception e) {
            return ToolBeans.errorJson("로그 검색 실패: " + e.getMessage());
        }
    }
}
