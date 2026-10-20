package io.deltazium.dwapply;

/**
 * 파일명 : BatchPolicy.java
 * 작성일자 : 26. 10. 19.
 * 작성자 : 최남희
 * 설명 : 배치 닫힘 조건 — "batchMs 경과 또는 누적 바이트 >= batchMaxBytes" 중 먼저 도달하는 쪽
 * (architecture.md 6.5). 배치 처리는 루프 안에서 동기 실행이므로 이전 배치가 끝나기 전에는
 * 다음 배치가 시작되지 않는다 — 부하가 크면 처리 중 쌓인 만큼 다음 배치가 자연히 커진다
 * (자기 조절, 별도 백프레셔 코드 없음).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 10. 19.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
public final class BatchPolicy {

    private BatchPolicy() {
    }

    public static boolean shouldClose(long windowStartMs, long nowMs, long bufferedBytes,
                                      long batchMs, long batchMaxBytes) {
        return nowMs - windowStartMs >= batchMs || bufferedBytes >= batchMaxBytes;
    }
}
