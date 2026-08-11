package io.deltazium.backend.assist;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * 파일명 : LogSource.java
 * 작성일자 : 26. 08. 10.
 * 작성자 : 최남희
 * 설명 : 로그 검색 대상 컴포넌트. 파일명 base는 이 enum에 고정 매핑돼 있으며,
 * 사용자 입력으로 파일명·경로를 만들지 않기 위한 유일한 통로다(임의 파일 접근 차단).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 10.       | 최남희  | 최초 생성
 * 26. 08. 10.       | 최남희  | from() 추가 — 대소문자 무시 파싱, 실패 시 허용 값 안내
 * --------------------------------------------------
 */
public enum LogSource {

    BACKEND("backend"),
    CONNECT("connect"),
    KAFKA("kafka"),
    CONTROLLER("controller");

    private final String base;

    LogSource(String base) {
        this.base = base;
    }

    /** 로그 파일명 base — 오늘은 {base}.log, 지난 날짜는 {yyyy-MM-dd}/{base}-{index}.log. */
    public String base() {
        return base;
    }

    /**
     * 대소문자를 무시하고 파싱한다 — 호출자(AI)가 소문자로 보내는 경우가 흔한데
     * 여기서 400을 내는 건 순수 손해다. 매핑에 없는 값은 허용 값 목록을 담아 거부한다.
     */
    public static LogSource from(String value) {
        if (value != null) {
            String trimmed = value.trim();
            for (LogSource candidate : values()) {
                if (candidate.name().equalsIgnoreCase(trimmed)) {
                    return candidate;
                }
            }
        }
        throw new IllegalArgumentException("source 값이 잘못됐다: " + value
                + " — 허용 값: " + Arrays.stream(values()).map(Enum::name)
                .collect(Collectors.joining(", ")));
    }
}
