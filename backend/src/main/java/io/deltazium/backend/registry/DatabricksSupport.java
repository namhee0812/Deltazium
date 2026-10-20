package io.deltazium.backend.registry;

import java.util.Properties;
import java.util.function.Function;

/**
 * 파일명 : DatabricksSupport.java
 * 작성일자 : 26. 10. 19.
 * 작성자 : 최남희
 * 설명 : Databricks 타깃 접속 공용 헬퍼 — 토큰 해석(연결의 password, 비어 있으면 backend 환경변수
 * DZ_DATABRICKS_TOKEN 폴백)과 JDBC Properties 조립을 한 곳에서 한다. 연결 테스트·워커 기동·
 * 딕셔너리 조회·DDL 실행이 모두 이 메서드를 거쳐야 토큰 해석 규칙이 갈라지지 않는다.
 * EnableArrow=0은 3.3.x에서 효과 없음(드라이버가 "deprecated and ignored, Arrow always enabled"
 * 로그) — 무해해서 둔다. JSON inline 결과를 원하면 EnableQueryResultDownload=0.
 * --add-opens는 3.3.1+가 JDK16+ Arrow를 지원해 불필요(라이브 실측).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 최초 생성
 * --------------------------------------------------
 * 26. 10. 20.       | 최남희  | EnableArrow 속성 제거·주석 정정(라이브 실측: 3.3.x에서 효과 없음)
 * --------------------------------------------------
 */
public final class DatabricksSupport {

    public static final String TOKEN_ENV = "DZ_DATABRICKS_TOKEN";

    private static volatile Function<String, String> env = System::getenv;

    private DatabricksSupport() {
    }

    /** 테스트 전용 — 환경변수 조회 대체. null이면 System.getenv로 복원. */
    public static void setEnvForTest(Function<String, String> replacement) {
        env = replacement == null ? System::getenv : replacement;
    }

    /** 연결에 저장된 토큰, 없으면 환경변수. 둘 다 없으면 null. */
    public static String token(DbConnection c) {
        if (c.password() != null && !c.password().isBlank()) {
            return c.password();
        }
        String fromEnv = env.apply(TOKEN_ENV);
        return fromEnv == null || fromEnv.isBlank() ? null : fromEnv;
    }

    /** JDBC 접속 Properties — 토큰은 PWD로만 넘긴다. 토큰이 없으면 IllegalStateException. */
    public static Properties props(DbConnection c) {
        String token = token(c);
        if (token == null) {
            throw new IllegalStateException("Databricks 토큰 없음 — 연결의 Access token 또는 환경변수 "
                    + TOKEN_ENV + " 필요");
        }
        Properties p = new Properties();
        p.setProperty("UID", "token");
        p.setProperty("PWD", token);
        return p;
    }

    /** stagingVolume("schema.volume") → Files API 경로 "/Volumes/catalog/schema/volume". */
    public static String volumePath(DbConnection c) {
        String[] sv = c.stagingVolume().split("\\.", 2);
        return "/Volumes/" + c.databaseName() + "/" + sv[0] + "/" + sv[1];
    }
}
