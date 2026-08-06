package io.deltazium.backend.metrics;

/**
 * 파일명 : ProcReader.java
 * 작성일자 : 26. 08. 06.
 * 작성자 : 최남희
 * 설명 : /proc 파싱 헬퍼 — 프로세스별 RSS(kB)·CPU tick 추출.
 * JMX 없이 pid 파일 기반으로 컴포넌트 자원을 수집하기 위한 최소 구현 (Linux 전용).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public final class ProcReader {

    private ProcReader() {
    }

    /** /proc/&lt;pid&gt;/status 본문에서 VmRSS(kB). 없으면 -1. */
    public static long parseVmRssKb(String statusContent) {
        for (String line : statusContent.split("\n")) {
            if (line.startsWith("VmRSS:")) {
                String[] parts = line.substring("VmRSS:".length()).trim().split("\\s+");
                try {
                    return Long.parseLong(parts[0]);
                } catch (NumberFormatException e) {
                    return -1;
                }
            }
        }
        return -1;
    }

    /**
     * /proc/&lt;pid&gt;/stat 본문에서 utime+stime(clock tick 합).
     * comm 필드(괄호 안 프로세스명)에 공백·괄호가 올 수 있어 마지막 ')' 이후를 파싱한다.
     * 필드 번호(공백 분리, ')' 이후 1부터): 12=utime, 13=stime.
     */
    public static long parseCpuTicks(String statContent) {
        int close = statContent.lastIndexOf(')');
        if (close < 0) {
            return -1;
        }
        String[] rest = statContent.substring(close + 1).trim().split("\\s+");
        if (rest.length < 13) {
            return -1;
        }
        try {
            return Long.parseLong(rest[11]) + Long.parseLong(rest[12]);
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
