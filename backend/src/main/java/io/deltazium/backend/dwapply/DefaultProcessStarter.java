package io.deltazium.backend.dwapply;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

/**
 * 파일명 : DefaultProcessStarter.java
 * 작성일자 : 26. 10. 19.
 * 작성자 : 최남희
 * 설명 : ProcessBuilder 기반 ProcessStarter — 로그는 append, 환경변수는 상속 + 추가분.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@Component
public class DefaultProcessStarter implements ProcessStarter {

    @Override
    public Process start(List<String> command, Map<String, String> env, Path logFile) throws IOException {
        Files.createDirectories(logFile.toAbsolutePath().getParent());
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.environment().putAll(env);
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile.toFile()));
        return pb.start();
    }
}
