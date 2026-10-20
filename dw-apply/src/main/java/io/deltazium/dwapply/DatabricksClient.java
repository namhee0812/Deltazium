package io.deltazium.dwapply;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * 파일명 : DatabricksClient.java
 * 작성일자 : 26. 10. 19.
 * 작성자 : 최남희
 * 설명 : 워커가 Databricks에 하는 모든 호출의 경계. 실구현은 Files API(HTTP)+JDBC이고,
 * 테스트는 가짜 구현으로 루프를 검증한다. 구현은 스레드 안전해야 한다(테이블별 병렬 호출).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public interface DatabricksClient extends AutoCloseable {

    /** Volume 파일 업로드(overwrite). */
    void upload(String path, byte[] content) throws IOException;

    /** Volume 파일 삭제. 없는 파일은 성공으로 취급한다. */
    void delete(String path) throws IOException;

    /** 디렉터리 직속 파일 경로 목록(하위 디렉터리 제외). 디렉터리가 없으면 빈 목록. */
    List<String> listDir(String path) throws IOException;

    /** MERGE 등 DML 1문 실행. */
    void merge(String sql) throws SQLException;

    /** 타깃 컬럼명(소문자) → full_data_type. information_schema.columns 조회. */
    Map<String, String> describeColumns(String schema, String table) throws SQLException;

    @Override
    void close();
}
