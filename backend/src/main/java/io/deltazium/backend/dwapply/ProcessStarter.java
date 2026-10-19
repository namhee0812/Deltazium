package io.deltazium.backend.dwapply;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * 파일명 : ProcessStarter.java
 * 작성일자 : 26. 10. 19.
 * 작성자 : 최남희
 * 설명 : DW apply 워커 프로세스 기동 경계 — 실구현은 ProcessBuilder, 테스트는 가짜 스크립트/객체로 대체한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public interface ProcessStarter {

    /**
     * @param command 실행 명령(런처 + 인자)
     * @param env     프로세스 환경에 추가할 변수(토큰 등 — argv로 넘기지 않는다)
     * @param logFile stdout/stderr를 append할 파일
     */
    Process start(List<String> command, Map<String, String> env, Path logFile) throws IOException;
}
