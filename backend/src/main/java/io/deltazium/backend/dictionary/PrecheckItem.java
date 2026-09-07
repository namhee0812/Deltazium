package io.deltazium.backend.dictionary;

/**
 * 파일명 : PrecheckItem.java
 * 작성일자 : 26. 09. 07.
 * 작성자 : 최남희
 * 설명 : 등록 사전 점검 결과 한 줄 (architecture.md 8절) — 소스 DB 종류별로 점검 항목·문구가
 * 달라도 UI는 이 구조 하나로 범용 렌더링한다(위저드가 Oracle 전용 필드명을 더 이상 모른다).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 최초 생성 — 다중 소스·다중 타깃 ② SourceDictionary 인터페이스 도입
 * --------------------------------------------------
 *
 * @param key      항목 식별자 (예: archivelog, wal_level, LOGMINING)
 * @param label    화면 표시용 라벨
 * @param ok       통과 여부
 * @param detail   실측값·에러 메시지 등 상세
 * @param blocking true면 미통과 시 등록을 막는다(필수). false면 경고만(참고 정보)
 */
public record PrecheckItem(String key, String label, boolean ok, String detail, boolean blocking) {
}
