package io.deltazium.backend.iceberg;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 파일명 : ChangelogStorageController.java
 * 작성일자 : 26. 09. 07.
 * 작성자 : 최남희
 * 설명 : changelog 저장소(MinIO/R2) 요약·연결 테스트 REST API — 연결 화면의 읽기 전용
 * "changelog 저장소" 카드가 쓴다 (architecture.md 2.2·3절, TODO ③). 편집 API는 없다 — 프로파일
 * 전환은 설치 작업(deploy/env.local.sh)이라 UI에서 바꾸지 않는다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@RestController
@RequestMapping("/api/system/changelog-storage")
public class ChangelogStorageController {

    private final ChangelogStorageService storage;

    public ChangelogStorageController(ChangelogStorageService storage) {
        this.storage = storage;
    }

    @GetMapping
    public ChangelogStorageService.ChangelogStorageInfo info() {
        return storage.info();
    }

    @PostMapping("/test")
    public ChangelogStorageService.TestResult test() {
        return storage.test();
    }
}
