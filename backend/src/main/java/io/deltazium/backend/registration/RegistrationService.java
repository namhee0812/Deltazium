package io.deltazium.backend.registration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.deltazium.backend.connect.ConnectorDeployService;
import io.deltazium.backend.dictionary.OracleDictionaryService;
import io.deltazium.backend.dictionary.SourceTableInfo;
import io.deltazium.backend.registry.DbConnection;
import io.deltazium.backend.registry.DbConnectionService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * CDC 테이블 등록 (architecture.md 8절).
 * 흐름: 딕셔너리 조회 → 사전 점검(PK 필수, supp.log ALL) → 메타데이터 저장 →
 * source·jdbc-sink 커넥터 배포(등록 테이블 전체 목록으로 갱신).
 *
 * iceberg-sink 배선은 changelog 스키마(5.1절) 개정 확정 후 추가한다 — connectors/README.md 미결 1.
 */
@Service
public class RegistrationService {

    private final RegisteredTableRepository repository;
    private final DbConnectionService connections;
    private final OracleDictionaryService dictionary;
    private final ConnectorDeployService deploy;
    private final String kafkaBootstrap;
    private final String topicPrefix;

    public RegistrationService(RegisteredTableRepository repository,
                               DbConnectionService connections,
                               OracleDictionaryService dictionary,
                               ConnectorDeployService deploy,
                               @Value("${deltazium.kafka.bootstrap}") String kafkaBootstrap,
                               @Value("${deltazium.topic-prefix}") String topicPrefix) {
        this.repository = repository;
        this.connections = connections;
        this.dictionary = dictionary;
        this.deploy = deploy;
        this.kafkaBootstrap = kafkaBootstrap;
        this.topicPrefix = topicPrefix;
    }

    public List<RegisteredTable> list() {
        return repository.findAll();
    }

    /** 소스 딕셔너리에서 패턴에 걸리는 테이블 + 점검 상태 조회 (등록 후보). */
    public List<SourceTableInfo> discover(long sourceConnectionId, String pattern) {
        DbConnection source = requireRole(sourceConnectionId, "SOURCE");
        return dictionary.listTables(source, pattern);
    }

    /** DB 레벨 사전 점검 (ARCHIVELOG, DB supplemental logging). */
    public Map<String, String> databaseChecks(long sourceConnectionId) {
        return dictionary.databaseChecks(requireRole(sourceConnectionId, "SOURCE"));
    }

    /** 사용자가 UI에서 승인한 경우에만 호출 — 테이블별 supp.log(ALL) 적용 시도. */
    public Map<String, String> applySupplementalLogging(long sourceConnectionId, List<String> tables) {
        return dictionary.applySupplementalLogging(requireRole(sourceConnectionId, "SOURCE"), tables);
    }

    /**
     * 등록 확정 + 커넥터 배포. tables는 "SCHEMA.TABLE" 목록 (와일드카드 불허 —
     * 전개는 discover 단계에서 끝났어야 한다).
     */
    @Transactional
    public List<RegisteredTable> register(long sourceConnectionId, long targetConnectionId,
                                          List<String> tables) {
        DbConnection source = requireRole(sourceConnectionId, "SOURCE");
        DbConnection target = requireRole(targetConnectionId, "TARGET");
        if (tables == null || tables.isEmpty()) {
            throw new IllegalArgumentException("등록할 테이블이 없다");
        }

        // 등록 직전 재검증 — discover 이후 상태가 바뀌었을 수 있다
        for (String qualified : tables) {
            if (qualified.contains("*") || qualified.contains("%")) {
                throw new IllegalArgumentException("와일드카드는 등록 시점에 허용되지 않는다: " + qualified);
            }
            List<SourceTableInfo> found = dictionary.listTables(source, qualified);
            if (found.isEmpty()) {
                throw new IllegalArgumentException("소스에 존재하지 않는 테이블: " + qualified);
            }
            SourceTableInfo info = found.get(0);
            if (!info.hasPk()) {
                throw new IllegalArgumentException(
                        "PK 없는 테이블은 등록 불가 (멱등 upsert 전제): " + qualified);
            }
            if (!info.suppLogAll()) {
                throw new IllegalArgumentException(
                        "supplemental logging (ALL) COLUMNS 미설정: " + qualified
                        + " — 사전 점검 단계에서 적용 후 다시 시도");
            }
            if (repository.exists(info.schema(), info.table())) {
                throw new IllegalArgumentException("이미 등록된 테이블: " + qualified);
            }
        }

        for (String qualified : tables) {
            int dot = qualified.indexOf('.');
            repository.insert(qualified.substring(0, dot).toUpperCase(),
                    qualified.substring(dot + 1).toUpperCase(),
                    sourceConnectionId, targetConnectionId);
        }

        deployConnectors(source, target);
        return repository.findAll();
    }

    /** 등록 테이블 전체 목록 기준으로 source·jdbc-sink 설정을 갱신 배포 (멱등 PUT). */
    private void deployConnectors(DbConnection source, DbConnection target) {
        List<RegisteredTable> all = repository.findAll();
        String includeList = all.stream().map(RegisteredTable::qualified)
                .collect(Collectors.joining(","));
        String topics = all.stream()
                .map(t -> topicPrefix + "." + t.qualified())
                .collect(Collectors.joining(","));

        Map<String, String> sourceVars = new HashMap<>();
        sourceVars.put("connector_name", "dz-source");
        sourceVars.put("oracle_host", source.host());
        sourceVars.put("oracle_port", String.valueOf(source.port()));
        sourceVars.put("oracle_user", source.username());
        sourceVars.put("oracle_password", source.password());
        sourceVars.put("oracle_dbname", source.databaseName());
        sourceVars.put("topic_prefix", topicPrefix);
        sourceVars.put("table_include_list", includeList);
        sourceVars.put("kafka_bootstrap", kafkaBootstrap);
        deploy.deploy("source", sourceVars);

        Map<String, String> sinkVars = new HashMap<>();
        sinkVars.put("connector_name", "dz-jdbc-sink");
        sinkVars.put("topics", topics);
        sinkVars.put("target_jdbc_url", target.jdbcUrl());
        sinkVars.put("target_user", target.username());
        sinkVars.put("target_password", target.password());
        deploy.deploy("jdbc-sink", sinkVars);
    }

    private DbConnection requireRole(long connectionId, String role) {
        DbConnection c = connections.get(connectionId);
        if (!role.equals(c.role())) {
            throw new IllegalArgumentException(
                    "%s 역할 연결이 필요하다: %s(role=%s)".formatted(role, c.name(), c.role()));
        }
        return c;
    }
}
