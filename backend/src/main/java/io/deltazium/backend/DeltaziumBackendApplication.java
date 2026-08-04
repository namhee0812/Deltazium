package io.deltazium.backend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 파일명 : DeltaziumBackendApplication.java
 * 작성일자 : 26. 07. 24.
 * 작성자 : 최남희
 * 설명 : Deltazium backend(제어면) Spring Boot 진입점.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 07. 24.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class DeltaziumBackendApplication {

    public static void main(String[] args) {
        SpringApplication.run(DeltaziumBackendApplication.class, args);
    }
}
