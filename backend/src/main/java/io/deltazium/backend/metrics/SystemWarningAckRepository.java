package io.deltazium.backend.metrics;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/**
 * 파일명 : SystemWarningAckRepository.java
 * 작성일자 : 26. 09. 23.
 * 작성자 : 최남희
 * 설명 : SQL은 resources/mappers/system-warning-ack.xml. system_warning_acks 테이블 접근.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 23.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@Mapper
public interface SystemWarningAckRepository {

    List<WarningObservation> findAll();

    /** 최초 관측 기록 — 이미 있으면 호출하지 않는 것이 호출부 책임(SystemWarningService). */
    void insertObserved(@Param("id") String id, @Param("sinceMs") long sinceMs);

    /** 확인(ack) — acked_at을 현재 시각으로 채운다. 대상 행은 insertObserved로 이미 존재해야 한다. */
    void ack(@Param("id") String id);

    /** 해소된(더 이상 활성이 아닌) INFO 경고 관측 행 정리 — 재발 시 새 sinceMs로 다시 뜨게 한다. */
    void deleteIds(@Param("ids") List<String> ids);
}
