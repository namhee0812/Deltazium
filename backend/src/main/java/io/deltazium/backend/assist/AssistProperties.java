package io.deltazium.backend.assist;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 파일명 : AssistProperties.java
 * 작성일자 : 26. 08. 10.
 * 작성자 : 최남희
 * 설명 : AI 진단 어시스턴트 설정 — 로그 검색 루트 디렉터리, overview lag 임계값.
 * 인프라 전체 로그(backend/connect/kafka/controller)를 읽는 용도이므로
 * 복구 잡 로그 출력 경로인 deltazium.recovery.log-dir과는 별개 프로퍼티다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 10.       | 최남희  | 최초 생성
 * 26. 08. 11.       | 최남희  | lag-threshold 추가 (overview lagging 판정, offset 건수, 기본 1000)
 * --------------------------------------------------
 *
 * @param logDir       로그 검색 루트
 * @param lagThreshold overview에서 lagging으로 분류하는 임계값 — offset 건수(시간 아님).
 *                     미설정 시 1000
 */
@ConfigurationProperties(prefix = "deltazium.assist")
public record AssistProperties(String logDir, Long lagThreshold) {

    public AssistProperties {
        if (lagThreshold == null) {
            lagThreshold = 1000L;
        }
    }
}
