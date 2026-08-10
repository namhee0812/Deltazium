package io.deltazium.backend.assist;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * 파일명 : AssistLogController.java
 * 작성일자 : 26. 08. 10.
 * 작성자 : 최남희
 * 설명 : AI 진단 어시스턴트용 로그 검색 REST API (조회 전용 — GET만 둔다).
 * 파일 경로 파라미터는 받지 않는다. 대상은 source enum으로만 지정된다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 10.       | 최남희  | 최초 생성
 * 26. 08. 10.       | 최남희  | source를 대소문자 무시로 파싱 (허용 값 목록을 에러 메시지에 포함)
 * --------------------------------------------------
 */
@RestController
@RequestMapping("/api/assist")
public class AssistLogController {

    private static final DateTimeFormatter DATE = DateTimeFormatter.ISO_LOCAL_DATE;

    private final LogSearchService service;

    public AssistLogController(LogSearchService service) {
        this.service = service;
    }

    /**
     * 로그 검색. 최신 매칭부터 limit개를 고르고, 결과는 시간 오름차순으로 반환한다.
     *
     * @param source       BACKEND|CONNECT|KAFKA|CONTROLLER (필수, 대소문자 무시)
     * @param q            검색 키워드 — 대소문자 무시 literal 부분문자열. 없으면 전체 줄
     * @param from         yyyy-MM-dd. 없으면 to와 같은 날 (to도 없으면 오늘 하루)
     * @param to           yyyy-MM-dd. 없으면 오늘
     * @param limit        기본 100, 서버가 500으로 캡
     * @param contextAfter 매칭 줄 뒤 함께 반환할 줄 수 (stack trace용). 기본 0, 최대 20
     */
    @GetMapping("/logs")
    public LogSearchResult logs(
            @RequestParam String source,
            @RequestParam(required = false) String q,
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestParam(defaultValue = "" + LogSearchService.DEFAULT_LIMIT) int limit,
            @RequestParam(defaultValue = "0") int contextAfter) {
        return service.search(LogSource.from(source), q, parseDate("from", from),
                parseDate("to", to), limit, contextAfter);
    }

    /** yyyy-MM-dd 엄격 파싱 — 디렉터리명으로 쓰이므로 형식이 어긋나면 400으로 막는다. */
    private static LocalDate parseDate(String name, String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return LocalDate.parse(value, DATE);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                    name + " 날짜 형식이 잘못됐다 (yyyy-MM-dd): " + value);
        }
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> badRequest(IllegalArgumentException e) {
        return Map.of("error", e.getMessage());
    }

    @ExceptionHandler(IllegalStateException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Map<String, String> readError(IllegalStateException e) {
        return Map.of("error", e.getMessage());
    }
}
