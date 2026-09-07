package io.deltazium.backend.dictionary;

import java.util.List;
import java.util.Map;

import io.deltazium.backend.registry.DbConnection;
import io.deltazium.backend.registry.DbType;

/**
 * 파일명 : SourceDictionary.java
 * 작성일자 : 26. 09. 07.
 * 작성자 : 최남희
 * 설명 : 소스 딕셔너리 조회·사전 점검·캡처 설정 적용의 DB 종류별 계약 (architecture.md 8절
 * "소스별 분기가 공식적으로 존재하는 유일한 자리"). Oracle 구현은 OracleDictionaryService,
 * PostgreSQL 구현은 PostgresDictionaryService — 선택은 DictionaryRouter가 dbType으로 한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 최초 생성 — 다중 소스·다중 타깃 ②. 기존 OracleDictionaryService의
 * |                          | Oracle 전용 메서드 시그니처(Map<String,String> 등)를 PrecheckItem
 * |                          | 목록 기반 범용 계약으로 승격, 캡처 설정(supp.log/REPLICA IDENTITY)을
 * |                          | 동일 승인 UX(미리보기→승인→적용)로 통일
 * --------------------------------------------------
 */
public interface SourceDictionary {

    DbType dbType();

    /** 패턴에 걸리는 테이블 목록 + 테이블별 PK/캡처 준비 상태. */
    List<SourceTableInfo> listTables(DbConnection conn, String pattern);

    /** 컬럼 목록 + PK 여부. 소스(매핑 원본)와 타깃(매핑 대상) 모두 이걸로 조회한다. */
    List<TableColumn> listColumns(DbConnection conn, String schema, String table);

    /** DB 레벨 점검 (예: Oracle ARCHIVELOG, PostgreSQL wal_level). */
    List<PrecheckItem> databaseChecks(DbConnection conn);

    /** 캡처 계정 권한 점검 — 계정 스스로 부여 불가하므로 적용 API는 없다(UI가 GRANT 안내). */
    List<PrecheckItem> privilegeChecks(DbConnection conn);

    /** 캡처 사전조건 라벨 — 에러 메시지·UI 문구용 (예: "supplemental logging (ALL) COLUMNS"). */
    String captureSetupLabel();

    /** 미충족 테이블에 실행될 DDL 미리보기 (승인 전 화면 표시용, qualified → DDL 문). */
    Map<String, String> captureSetupPreview(DbConnection conn, List<String> qualifiedTables);

    /** 사용자가 승인한 뒤 실제 적용. qualified → "OK" 또는 에러 메시지. */
    Map<String, String> applyCaptureSetup(DbConnection conn, List<String> qualifiedTables);

    class DictionaryException extends RuntimeException {
        public DictionaryException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
