package io.deltazium.backend.config;

import java.util.Properties;

import org.apache.ibatis.mapping.DatabaseIdProvider;
import org.apache.ibatis.mapping.VendorDatabaseIdProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 파일명 : MyBatisConfig.java
 * 작성일자 : 26. 08. 06.
 * 작성자 : 최남희
 * 설명 : MyBatis databaseIdProvider — 매퍼 XML에서 PG/H2 방언 분기용
 * (DATE_TRUNC 인자 문법이 달라 metrics-sample.xml 롤업 SQL이 databaseId로 갈린다).
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 06.       | 최남희  | 최초 생성
 * --------------------------------------------------
 */
@Configuration
public class MyBatisConfig {

    @Bean
    public DatabaseIdProvider databaseIdProvider() {
        VendorDatabaseIdProvider provider = new VendorDatabaseIdProvider();
        Properties props = new Properties();
        props.setProperty("PostgreSQL", "postgresql");
        props.setProperty("H2", "h2");
        provider.setProperties(props);
        return provider;
    }
}
