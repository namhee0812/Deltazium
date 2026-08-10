package io.deltazium.backend.assist;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

import org.springframework.stereotype.Service;

/**
 * 파일명 : LogSearchService.java
 * 작성일자 : 26. 08. 10.
 * 작성자 : 최남희
 * 설명 : AI 진단 어시스턴트용 로그 검색(읽기 전용).
 * 로그 레이아웃 — 오늘은 루트 바로 아래 평면 파일 {base}.log,
 * 지난 날짜는 {yyyy-MM-dd}/{base}-{index}.log (index는 롤오버 파일 판별용이고, 최신성 판단은 mtime).
 *
 * <p>
 * 설계 제약 세 가지:
 * ① 경로는 LogSource enum의 고정 매핑으로만 만들고, 최종 경로를 realpath로 정규화해
 * 로그 루트 하위인지 검증한다(심링크 이탈 차단).
 * ② 검색은 대소문자 무시 literal 부분문자열 — 정규식을 쓰지 않는다(ReDoS 방지).
 * ③ 파일을 통째로 올리지 않고 줄 단위 스트리밍으로 읽으며, limit에 닿으면 더 오래된 파일을 열지 않는다.
 *
 * <p>
 * 정렬 정책: 진단은 거의 항상 "최근 에러"를 보므로 <b>최신부터 골라</b>(날짜 역순 · 같은 날은 mtime 역순,
 * 파일 안에서는 링버퍼로 최근 cap개만 유지) <b>시간 오름차순으로 반환</b>한다.
 * 오름차순으로 훑다 cap에서 끊으면 가장 오래된 매칭만 남아 진단 용도로는 반대 결과가 된다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 10.       | 최남희  | 최초 생성
 * 26. 08. 10.       | 최남희  | 최신 우선 선택(링버퍼)·시간 오름차순 반환으로 변경, from>to 거부
 * 26. 08. 10.       | 최남희  | 정렬 키를 index → mtime으로 수정 (index는 회전 재사용 슬롯이라 시각과 무관)
 * --------------------------------------------------
 */
@Service
public class LogSearchService {

    /** 서버 강제 캡 — 요청이 이보다 커도 이 값으로 자른다. */
    public static final int MAX_LIMIT = 500;
    public static final int DEFAULT_LIMIT = 100;
    /** stack trace를 보기 위한 뒷줄 수 상한. */
    public static final int MAX_CONTEXT_AFTER = 20;

    private static final DateTimeFormatter DIR_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final String LOG_SUFFIX = ".log";

    private final Path root;

    public LogSearchService(AssistProperties properties) {
        this.root = Path.of(properties.logDir()).toAbsolutePath().normalize();
    }

    /**
     * 지정 기간의 로그에서 키워드를 찾는다. 최신 매칭부터 cap개를 고르고, 결과는 시간 오름차순이다.
     *
     * @param q            null·빈 문자열이면 전체 줄이 대상
     * @param from         null이면 to와 같은 날 (to도 null이면 오늘 하루)
     * @param to           null이면 오늘
     * @param limit        반환 줄 수 상한 (1..{@value #MAX_LIMIT}로 클램프)
     * @param contextAfter 매칭 줄 뒤에 함께 반환할 줄 수 (0..{@value #MAX_CONTEXT_AFTER}로 클램프)
     * @throws IllegalArgumentException from이 to보다 뒤일 때 — 조용한 빈 결과는 파라미터 실수를
     *                                  "로그 없음"으로 위장시켜 오진을 부른다
     */
    public LogSearchResult search(LogSource source, String q, LocalDate from, LocalDate to,
                                  int limit, int contextAfter) {
        int cap = Math.min(Math.max(limit, 1), MAX_LIMIT);
        int context = Math.min(Math.max(contextAfter, 0), MAX_CONTEXT_AFTER);
        String needle = (q == null || q.isEmpty()) ? null : q.toLowerCase(Locale.ROOT);

        LocalDate today = LocalDate.now();
        LocalDate end = (to != null) ? to : today;
        LocalDate start = (from != null) ? from : end;
        if (start.isAfter(end)) {
            throw new IllegalArgumentException(
                    "from(" + start + ")이 to(" + end + ")보다 뒤다 — 기간이 비어 있다");
        }

        // 후보 파일을 최신 → 오래된 순으로 세운다 (열지는 않는다 — 디렉터리 목록만)
        List<Path> candidates = new ArrayList<>();
        for (LocalDate date = end; !date.isBefore(start); date = date.minusDays(1)) {
            List<Path> ofDay = filesFor(source, date, today);   // 오름차순
            for (int i = ofDay.size() - 1; i >= 0; i--) {
                candidates.add(ofDay.get(i));
            }
        }

        List<String> opened = new ArrayList<>();                 // 최신 → 오래된
        List<List<LogSearchResult.LogLine>> chunks = new ArrayList<>();
        int remaining = cap;
        boolean truncated = false;

        for (Path file : candidates) {
            if (remaining == 0) {
                // cap이 이미 찼는데 후보가 남았다 — 더 오래된 파일은 열지 않고 잘렸다고 알린다
                truncated = true;
                break;
            }
            String relative = root.relativize(file).toString();
            opened.add(relative);
            List<LogSearchResult.LogLine> chunk = new ArrayList<>();
            if (scanNewest(file, relative, needle, context, remaining, chunk)) {
                truncated = true;   // 링버퍼에서 더 오래된 매칭이 밀려났다
            }
            chunks.add(chunk);
            remaining -= chunk.size();
        }

        // 고를 때는 최신 우선, 보여줄 때는 시간 오름차순
        Collections.reverse(opened);
        Collections.reverse(chunks);
        List<LogSearchResult.LogLine> lines = chunks.stream().flatMap(List::stream).toList();

        return new LogSearchResult(source, List.copyOf(opened), lines.size(), truncated, lines);
    }

    /**
     * 파일 한 개를 앞에서부터 스트리밍으로 훑되, 고정 크기 링버퍼로 <b>가장 최근 keep개</b>만 남긴다.
     * 파일을 통째로 메모리에 올리지 않으며 상주 메모리는 keep(≤ cap)줄로 묶인다.
     *
     * @param out keep개 이하, 파일 내 오름차순으로 채워진다
     * @return 링버퍼에서 밀려난 줄이 있으면 true
     */
    private boolean scanNewest(Path file, String relative, String needle, int contextAfter,
                               int keep, List<LogSearchResult.LogLine> out) {
        Deque<LogSearchResult.LogLine> ring = new ArrayDeque<>(Math.min(keep, 1024));
        boolean evicted = false;
        int remainingContext = 0;   // 파일 경계를 넘어 context를 잇지 않는다
        long lineNo = 0;
        // 로그에 깨진 바이트가 섞여도 예외로 끊기지 않도록 InputStreamReader의 REPLACE 기본 동작을 쓴다
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lineNo++;
                boolean matched = needle == null
                        || line.toLowerCase(Locale.ROOT).contains(needle);
                LogSearchResult.LogLine emitted;
                if (matched) {
                    emitted = new LogSearchResult.LogLine(relative, lineNo, line, true);
                    remainingContext = contextAfter;
                } else if (remainingContext > 0) {
                    emitted = new LogSearchResult.LogLine(relative, lineNo, line, false);
                    remainingContext--;
                } else {
                    continue;
                }
                if (ring.size() == keep) {
                    ring.removeFirst();
                    evicted = true;
                }
                ring.addLast(emitted);
            }
        } catch (IOException e) {
            throw new IllegalStateException("로그 파일 읽기 실패(" + relative + "): " + e.getMessage(), e);
        }
        out.addAll(ring);
        return evicted;
    }

    /** 정렬용 후보 — 최신성 판단은 mtime, index는 tie-break에만 쓴다. */
    private record Candidate(Path path, FileTime mtime, int index) {
    }

    /**
     * 해당 날짜에 읽을 파일 목록을 <b>시간 오름차순</b>(오래된 → 최신)으로 돌려준다.
     * 디렉터리·파일이 없으면 조용히 빈 목록.
     *
     * <p>
     * 정렬 키는 mtime이다. index로 정렬하면 안 된다 — log4j2 DefaultRolloverStrategy의 index는
     * 1..max를 <b>회전 재사용</b>하는 슬롯 번호라 시각과 단조 대응하지 않는다.
     * 실제로 2026-08-04에는 connect-13.log(14:59)보다 connect-10.log(23:59)가 더 최신이었다.
     */
    private List<Path> filesFor(LogSource source, LocalDate date, LocalDate today) {
        if (date.equals(today)) {
            Path flat = root.resolve(source.base() + LOG_SUFFIX);
            return withinRoot(flat) ? List.of(flat) : List.of();
        }
        Path dir = root.resolve(date.format(DIR_FORMAT));
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        String base = source.base();
        try (Stream<Path> entries = Files.list(dir)) {
            return entries
                    .filter(p -> rolledIndex(p, base) >= 0)   // 롤오버 파일 판별 (정렬용이 아니다)
                    .filter(this::withinRoot)
                    .map(p -> new Candidate(p, lastModified(p), rolledIndex(p, base)))
                    .sorted(Comparator.comparing(Candidate::mtime)
                            .thenComparingInt(Candidate::index))
                    .map(Candidate::path)
                    .toList();
        } catch (IOException e) {
            throw new IllegalStateException("로그 디렉터리 조회 실패(" + dir + "): " + e.getMessage(), e);
        }
    }

    /**
     * mtime을 읽지 못하면(경합으로 삭제되는 등) 가장 오래된 것으로 취급해 뒤로 민다.
     * 검색 전체를 실패시키지 않는다.
     */
    private static FileTime lastModified(Path path) {
        try {
            return Files.getLastModifiedTime(path);
        } catch (IOException e) {
            return FileTime.fromMillis(Long.MIN_VALUE);
        }
    }

    /**
     * {base}-{index}.log 형태인지 <b>판별</b>하고 index를 돌려준다. 순수 숫자가 아니면 -1.
     * 같은 glob에 걸리는 동명이인(connect-console.log, kafka-authorizer-1.log)을 걸러내는 용도이며,
     * 정렬 키가 아니다 (mtime이 같을 때 tie-break로만 쓴다).
     */
    static int rolledIndex(Path path, String base) {
        String name = path.getFileName().toString();
        String prefix = base + "-";
        if (!name.startsWith(prefix) || !name.endsWith(LOG_SUFFIX)) {
            return -1;
        }
        String index = name.substring(prefix.length(), name.length() - LOG_SUFFIX.length());
        if (index.isEmpty() || !index.chars().allMatch(Character::isDigit)) {
            return -1;
        }
        try {
            return Integer.parseInt(index);
        } catch (NumberFormatException e) {
            return -1;   // 자리수가 int를 넘는 비정상 파일명
        }
    }

    /**
     * 실제 파일이며 realpath 기준 로그 루트 하위인지 검증한다.
     * 심링크로 루트를 벗어나는 파일은 여기서 걸러 조용히 건너뛴다.
     */
    private boolean withinRoot(Path path) {
        try {
            if (!Files.isRegularFile(path)) {
                return false;
            }
            return path.toRealPath().startsWith(root.toRealPath());
        } catch (IOException e) {
            return false;
        }
    }
}
