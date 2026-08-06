package io.deltazium.backend.metrics;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 파일명 : ProcReaderTest.java
 * 작성일자 : 26. 08. 06.
 * 작성자 : 최남희
 * 설명 : /proc 파싱 단위 테스트 — comm 필드에 공백·괄호가 있는 케이스 포함.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
class ProcReaderTest {

    @Test
    void VmRSS를_kB로_읽는다() {
        String status = """
                Name:\tjava
                VmPeak:\t 9999999 kB
                VmRSS:\t 2097152 kB
                Threads:\t120
                """;
        assertThat(ProcReader.parseVmRssKb(status)).isEqualTo(2097152L);
    }

    @Test
    void stat의_utime_stime_합을_읽는다_comm에_공백과_괄호가_있어도() {
        // comm = "(a b) c" 같은 병적 케이스 — 마지막 ')' 이후를 파싱해야 안전
        String stat = "1234 ((a b) c) S 1 1234 1234 0 -1 4194560 "
                + "500 0 0 0 700 300 0 0 20 0 12 0 100000 999999 5000 "
                + "18446744073709551615 1 1 0 0 0 0 0 0 0 0 0 0 17 3 0 0 0 0 0";
        // ')' 이후 필드: S 1 1234 1234 0 -1 4194560 500 0 0 0 [700=utime] [300=stime] ...
        assertThat(ProcReader.parseCpuTicks(stat)).isEqualTo(1000L);
    }

    @Test
    void 필드가_모자라면_마이너스1() {
        assertThat(ProcReader.parseCpuTicks("1 (x) S 1")).isEqualTo(-1);
        assertThat(ProcReader.parseVmRssKb("Name: x")).isEqualTo(-1);
    }
}
