package io.deltazium.backend.assist;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 파일명 : LogSearchServiceTest.java
 * 작성일자 : 26. 08. 10.
 * 작성자 : 최남희
 * 설명 : 임시 디렉터리에 가짜 로그 트리를 만들어 검색 규약을 검증한다 —
 * 롤오버 index 정수 정렬, limit 서버 캡, contextAfter, 경로 이탈(심링크) 차단, 없는 날짜 건너뛰기,
 * 최신 우선 선택 + 시간 오름차순 반환.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 10.       | 최남희  | 최초 생성
 * 26. 08. 10.       | 최남희  | 최신 우선 선택·오름차순 반환·from>to 거부 검증 추가
 * 26. 08. 10.       | 최남희  | 최신성 판단을 mtime 기준으로 재작성 (index 정렬 전제 폐기 — 2026-08-04 사례)
 * --------------------------------------------------
 */
class LogSearchServiceTest {

    @TempDir
    Path tmp;

    private Path root;
    private Path outside;
    private LogSearchService service;

    private static final LocalDate TODAY = LocalDate.now();
    private static final LocalDate YESTERDAY = TODAY.minusDays(1);

    @BeforeEach
    void setUp() throws IOException {
        root = Files.createDirectories(tmp.resolve("logs"));
        outside = Files.createDirectories(tmp.resolve("outside"));
        service = new LogSearchService(new AssistProperties(root.toString()));
    }

    private Path dayDir(LocalDate date) throws IOException {
        return Files.createDirectories(root.resolve(date.format(DateTimeFormatter.ISO_LOCAL_DATE)));
    }

    private void write(Path file, String... lines) throws IOException {
        Files.write(file, List.of(lines), StandardCharsets.UTF_8);
    }

    /** 최신성 판단은 mtime이므로 픽스처는 생성 순서에 기대지 않고 시각을 명시한다. */
    private void writeAt(Path file, String isoInstant, String... lines) throws IOException {
        write(file, lines);
        Files.setLastModifiedTime(file, FileTime.from(Instant.parse(isoInstant)));
    }

    @Test
    void 롤오버_파일은_index_숫자로_판별하고_동명이인은_제외한다() throws IOException {
        Path dir = dayDir(YESTERDAY);
        // 자리수가 섞여 있어도(옛 설정 잔재) index가 순수 숫자면 전부 대상이다
        writeAt(dir.resolve("connect-00.log"), "2026-08-04T06:59:00Z", "ERROR zero");
        writeAt(dir.resolve("connect-2.log"), "2026-08-04T07:59:00Z", "ERROR two");
        writeAt(dir.resolve("connect-10.log"), "2026-08-04T08:59:00Z", "ERROR ten");
        writeAt(dir.resolve("connect-11.log"), "2026-08-04T09:59:00Z", "ERROR eleven");
        // index 자리가 숫자가 아닌 동명이인 로그는 같은 glob에 걸려도 대상이 아니다
        writeAt(dir.resolve("connect-console.log"), "2026-08-04T10:59:00Z", "ERROR console");

        LogSearchResult result = service.search(LogSource.CONNECT, "ERROR",
                YESTERDAY, YESTERDAY, 100, 0);

        String prefix = YESTERDAY.format(DateTimeFormatter.ISO_LOCAL_DATE) + "/";
        assertThat(result.searchedFiles()).containsExactly(
                prefix + "connect-00.log",
                prefix + "connect-2.log",
                prefix + "connect-10.log",
                prefix + "connect-11.log");
        assertThat(result.lines()).extracting(LogSearchResult.LogLine::text)
                .containsExactly("ERROR zero", "ERROR two", "ERROR ten", "ERROR eleven");
        assertThat(result.truncated()).isFalse();
        assertThat(result.returnedLines()).isEqualTo(4);
    }

    @Test
    void limit은_서버가_500으로_캡하고_최신_500줄을_오름차순으로_준다() throws IOException {
        List<String> lines = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            lines.add("ERROR line-" + i);
        }
        Files.write(root.resolve("connect.log"), lines, StandardCharsets.UTF_8);

        LogSearchResult result = service.search(LogSource.CONNECT, "ERROR", null, null, 10_000, 0);

        assertThat(result.returnedLines()).isEqualTo(500);
        assertThat(result.truncated()).isTrue();
        // 앞의 100줄(line-0..99)이 링버퍼에서 밀려나고 최신 500줄만 남는다
        assertThat(result.lines().get(0).text()).isEqualTo("ERROR line-100");
        assertThat(result.lines().get(499).text()).isEqualTo("ERROR line-599");
        // 반환은 시간 오름차순 — 줄번호가 단조 증가
        assertThat(result.lines()).extracting(LogSearchResult.LogLine::lineNo)
                .isSorted();
    }

    @Test
    void cap보다_매칭이_많으면_최신_것이_남고_오래된_것이_밀려난다() throws IOException {
        write(root.resolve("connect.log"),
                "ERROR 첫번째", "ERROR 두번째", "ERROR 세번째", "ERROR 네번째");

        LogSearchResult result = service.search(LogSource.CONNECT, "ERROR", null, null, 2, 0);

        assertThat(result.lines()).extracting(LogSearchResult.LogLine::text)
                .containsExactly("ERROR 세번째", "ERROR 네번째");
        assertThat(result.lines()).extracting(LogSearchResult.LogLine::lineNo)
                .containsExactly(3L, 4L);
        assertThat(result.truncated()).isTrue();
    }

    /**
     * 실제 2026-08-04 로그 재현 — index는 log4j2가 회전 재사용하는 슬롯 번호라
     * connect-13(14:59)보다 connect-10(23:59)이 최신이었다. index로 정렬하면 최신을 놓친다.
     */
    @Test
    void 같은_날_최신_판단은_index가_아니라_mtime이다() throws IOException {
        Path dir = dayDir(YESTERDAY);
        writeAt(dir.resolve("connect-11.log"), "2026-08-04T12:59:00Z", "ERROR 12시");
        writeAt(dir.resolve("connect-12.log"), "2026-08-04T13:59:00Z", "ERROR 13시");
        writeAt(dir.resolve("connect-13.log"), "2026-08-04T14:59:00Z", "ERROR 14시");
        writeAt(dir.resolve("connect-10.log"), "2026-08-04T23:59:00Z", "ERROR 23시");

        LogSearchResult result = service.search(LogSource.CONNECT, "ERROR",
                YESTERDAY, YESTERDAY, 1, 0);

        assertThat(result.lines()).extracting(LogSearchResult.LogLine::text)
                .containsExactly("ERROR 23시");
        assertThat(result.searchedFiles()).containsExactly(
                YESTERDAY.format(DateTimeFormatter.ISO_LOCAL_DATE) + "/connect-10.log");
        assertThat(result.truncated()).isTrue();
    }

    @Test
    void 반환_순서도_index가_아니라_mtime_오름차순이다() throws IOException {
        Path dir = dayDir(YESTERDAY);
        writeAt(dir.resolve("connect-13.log"), "2026-08-04T14:59:00Z", "ERROR 14시");
        writeAt(dir.resolve("connect-10.log"), "2026-08-04T23:59:00Z", "ERROR 23시");
        writeAt(dir.resolve("connect-5.log"), "2026-08-04T06:59:00Z", "ERROR 06시");

        LogSearchResult result = service.search(LogSource.CONNECT, "ERROR",
                YESTERDAY, YESTERDAY, 100, 0);

        assertThat(result.lines()).extracting(LogSearchResult.LogLine::text)
                .containsExactly("ERROR 06시", "ERROR 14시", "ERROR 23시");
        String prefix = YESTERDAY.format(DateTimeFormatter.ISO_LOCAL_DATE) + "/";
        assertThat(result.searchedFiles()).containsExactly(
                prefix + "connect-5.log", prefix + "connect-13.log", prefix + "connect-10.log");
    }

    @Test
    void mtime이_같으면_index가_큰_쪽을_최신으로_본다() throws IOException {
        Path dir = dayDir(YESTERDAY);
        writeAt(dir.resolve("connect-2.log"), "2026-08-04T23:59:00Z", "ERROR index 2");
        writeAt(dir.resolve("connect-10.log"), "2026-08-04T23:59:00Z", "ERROR index 10");

        LogSearchResult result = service.search(LogSource.CONNECT, "ERROR",
                YESTERDAY, YESTERDAY, 1, 0);

        assertThat(result.lines()).extracting(LogSearchResult.LogLine::text)
                .containsExactly("ERROR index 10");
    }

    @Test
    void 최신_파일로_cap이_모자라면_이전_파일에서_최신_쪽부터_채운다() throws IOException {
        Path dir = dayDir(YESTERDAY);
        writeAt(dir.resolve("connect-0.log"), "2026-08-04T10:00:00Z",
                "ERROR a1", "ERROR a2", "ERROR a3");
        writeAt(dir.resolve("connect-1.log"), "2026-08-04T20:00:00Z", "ERROR b1", "ERROR b2");

        LogSearchResult result = service.search(LogSource.CONNECT, "ERROR",
                YESTERDAY, YESTERDAY, 3, 0);

        // 최신 파일(connect-1) 2줄 + 이전 파일(connect-0)의 마지막 1줄, 반환은 시간 오름차순
        assertThat(result.lines()).extracting(LogSearchResult.LogLine::text)
                .containsExactly("ERROR a3", "ERROR b1", "ERROR b2");
        String prefix = YESTERDAY.format(DateTimeFormatter.ISO_LOCAL_DATE) + "/";
        assertThat(result.searchedFiles()).containsExactly(
                prefix + "connect-0.log", prefix + "connect-1.log");
        assertThat(result.truncated()).isTrue();
    }

    @Test
    void cap이_찼으면_더_오래된_파일은_열지_않는다() throws IOException {
        Path dir = dayDir(YESTERDAY);
        write(dir.resolve("connect-0.log"), "ERROR 어제 것");
        write(root.resolve("connect.log"), "ERROR 오늘1", "ERROR 오늘2");

        LogSearchResult result = service.search(LogSource.CONNECT, "ERROR",
                YESTERDAY, TODAY, 2, 0);

        assertThat(result.searchedFiles()).containsExactly("connect.log");
        assertThat(result.lines()).extracting(LogSearchResult.LogLine::text)
                .containsExactly("ERROR 오늘1", "ERROR 오늘2");
        assertThat(result.truncated()).isTrue();
    }

    @Test
    void from이_to보다_뒤면_거부한다() {
        assertThatThrownBy(() -> service.search(LogSource.CONNECT, "ERROR",
                TODAY, YESTERDAY, 100, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("보다 뒤다");
    }

    @Test
    void source는_대소문자를_무시하고_없는_값은_허용_목록을_알려준다() {
        assertThat(LogSource.from("connect")).isEqualTo(LogSource.CONNECT);
        assertThat(LogSource.from(" Backend ")).isEqualTo(LogSource.BACKEND);
        assertThatThrownBy(() -> LogSource.from("oracle"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("BACKEND, CONNECT, KAFKA, CONTROLLER");
    }

    @Test
    void 결과가_캡_이하면_truncated는_false다() throws IOException {
        write(root.resolve("connect.log"), "ERROR one", "info", "ERROR two");

        LogSearchResult result = service.search(LogSource.CONNECT, "error", null, null, 100, 0);

        assertThat(result.returnedLines()).isEqualTo(2);
        assertThat(result.truncated()).isFalse();
    }

    @Test
    void contextAfter는_매칭_줄_뒤_N줄을_matched_false로_함께_준다() throws IOException {
        write(root.resolve("backend.log"),
                "정상 시작",
                "java.lang.IllegalStateException: boom",
                "\tat io.deltazium.A.run(A.java:10)",
                "\tat io.deltazium.B.run(B.java:20)",
                "\tat io.deltazium.C.run(C.java:30)",
                "그 뒤 무관한 줄");

        LogSearchResult result = service.search(LogSource.BACKEND, "IllegalStateException",
                null, null, 100, 2);

        assertThat(result.lines()).hasSize(3);
        assertThat(result.lines().get(0).matched()).isTrue();
        assertThat(result.lines().get(0).lineNo()).isEqualTo(2);
        assertThat(result.lines().get(1).matched()).isFalse();
        assertThat(result.lines().get(1).text()).isEqualTo("\tat io.deltazium.A.run(A.java:10)");
        assertThat(result.lines().get(2).matched()).isFalse();
        assertThat(result.lines().get(2).lineNo()).isEqualTo(4);
        assertThat(result.lines()).allSatisfy(l -> assertThat(l.file()).isEqualTo("backend.log"));
    }

    @Test
    void contextAfter는_20으로_캡된다() throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("ERROR boom");
        for (int i = 0; i < 50; i++) {
            lines.add("\tat frame-" + i);
        }
        Files.write(root.resolve("backend.log"), lines, StandardCharsets.UTF_8);

        LogSearchResult result = service.search(LogSource.BACKEND, "boom", null, null, 500, 999);

        assertThat(result.returnedLines()).isEqualTo(21);   // 매칭 1줄 + context 20줄
    }

    @Test
    void 루트_밖을_가리키는_심링크는_결과에서_제외된다() throws IOException {
        Path secret = outside.resolve("secret.log");
        write(secret, "ERROR 비밀 내용");

        Path dir = dayDir(YESTERDAY);
        write(dir.resolve("connect-1.log"), "ERROR 정상 내용");
        Files.createSymbolicLink(dir.resolve("connect-2.log"), secret);
        // 오늘자 평면 파일도 심링크로 바꿔치기 시도
        Files.createSymbolicLink(root.resolve("connect.log"), secret);

        LogSearchResult past = service.search(LogSource.CONNECT, "ERROR",
                YESTERDAY, YESTERDAY, 100, 0);
        assertThat(past.searchedFiles()).containsExactly(
                YESTERDAY.format(DateTimeFormatter.ISO_LOCAL_DATE) + "/connect-1.log");
        assertThat(past.lines()).extracting(LogSearchResult.LogLine::text)
                .containsExactly("ERROR 정상 내용");

        LogSearchResult todayResult = service.search(LogSource.CONNECT, "ERROR", null, null, 100, 0);
        assertThat(todayResult.searchedFiles()).isEmpty();
        assertThat(todayResult.lines()).isEmpty();
    }

    @Test
    void 없는_날짜_디렉터리는_조용히_건너뛴다() throws IOException {
        Path dir = dayDir(TODAY.minusDays(3));
        write(dir.resolve("kafka-0.log"), "ERROR 3일 전");
        // 그 사이 날짜 디렉터리와 오늘 평면 파일은 아예 없다

        LogSearchResult result = service.search(LogSource.KAFKA, "ERROR",
                TODAY.minusDays(10), TODAY, 100, 0);

        assertThat(result.searchedFiles()).containsExactly(
                TODAY.minusDays(3).format(DateTimeFormatter.ISO_LOCAL_DATE) + "/kafka-0.log");
        assertThat(result.returnedLines()).isEqualTo(1);
        assertThat(result.truncated()).isFalse();
    }

    @Test
    void 로그_루트_자체가_없어도_예외없이_빈_결과를_준다() {
        LogSearchService missing = new LogSearchService(
                new AssistProperties(tmp.resolve("nope").toString()));

        LogSearchResult result = missing.search(LogSource.CONNECT, "ERROR", null, null, 100, 0);

        assertThat(result.searchedFiles()).isEmpty();
        assertThat(result.lines()).isEmpty();
        assertThat(result.truncated()).isFalse();
    }

    @Test
    void 키워드는_대소문자를_무시한_literal_매칭이다() throws IOException {
        write(root.resolve("controller.log"), "abc 정상", "a.c 리터럴", "ABC 대문자");

        // 정규식이라면 "a.c"가 "abc"에도 걸린다 — literal이므로 걸리지 않아야 한다
        LogSearchResult literal = service.search(LogSource.CONTROLLER, "a.c", null, null, 100, 0);
        assertThat(literal.lines()).extracting(LogSearchResult.LogLine::text)
                .containsExactly("a.c 리터럴");

        LogSearchResult ignoreCase = service.search(LogSource.CONTROLLER, "abc", null, null, 100, 0);
        assertThat(ignoreCase.lines()).extracting(LogSearchResult.LogLine::text)
                .containsExactly("abc 정상", "ABC 대문자");
    }

    @Test
    void 키워드가_없으면_전체_줄이_대상이다() throws IOException {
        write(root.resolve("connect.log"), "one", "two", "three");

        LogSearchResult result = service.search(LogSource.CONNECT, null, null, null, 100, 0);

        assertThat(result.returnedLines()).isEqualTo(3);
        assertThat(result.lines()).allSatisfy(l -> assertThat(l.matched()).isTrue());
    }

    @Test
    void from만_주면_from부터_오늘까지_본다() throws IOException {
        Path dir = dayDir(YESTERDAY);
        write(dir.resolve("connect-0.log"), "ERROR 어제");
        write(root.resolve("connect.log"), "ERROR 오늘");

        LogSearchResult result = service.search(LogSource.CONNECT, "ERROR", YESTERDAY, null, 100, 0);

        assertThat(result.lines()).extracting(LogSearchResult.LogLine::text)
                .containsExactly("ERROR 어제", "ERROR 오늘");
    }
}
